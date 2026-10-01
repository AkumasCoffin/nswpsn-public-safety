// Package chanmgr is the automatic channel manager: a channel whose decode
// health sits below 40% for 10 continuous minutes is stopped (it is burning a
// tuner for nothing), then retested every 20 minutes — started, given time to
// lock and for the quality metric to mean something, and restored only once it
// decodes properly again.
//
// Design rules, learned the hard way (two earlier agent-autonomy attempts were
// reverted for churning whole nodes):
//
//   - The manager is STATELESS across restarts. Nothing is persisted; every
//     tick reconciles against the live /channels list, and the one fact that
//     must survive an agent restart — "which channels did I stop on purpose" —
//     is recovered from vce's own suppressed flag. A world that changed under
//     us (config push, operator click, sdrtrunk restart) always wins.
//   - All state keys on the TRIMMED CHANNEL NAME. vce channel ids are
//     process-local counters reassigned on every reload; the id is re-resolved
//     from a fresh fetch immediately before every start/stop call.
//   - It never touches tuner assignment, sample rates, or config
//     import/reload. Only per-channel start/stop/suppress/unsuppress.
//   - Suppress strictly BEFORE stop: vce's 30s self-heal sweep restarts any
//     non-processing auto-start channel, and would otherwise win the race.
//   - A measured decode sample is SyncPercent non-nil and > 0. Null and 0 both
//     mean "unmeasured" (no monitor attached, or no fresh snapshot) — an
//     unmeasured channel is never judged, so analog (NBFM/AM) channels and
//     channels on an old sdrtrunk runtime are never touched.
//   - Every decision is logged loudly, and the full manager state rides the
//     status frame so staff see exactly what it is doing and why.
package chanmgr

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
)

const (
	// pollInterval: one detection pass per tick. Matches the status heartbeat
	// cadence; the 10-minute dwell makes anything faster pointless.
	pollInterval   = 15 * time.Second
	intervalJitter = 2 * time.Second

	// lowThresholdPct: decode health below this is "not working".
	lowThresholdPct = 40.0
	// lowDwell: how long a channel must stay below threshold before it is
	// stopped. Every measured sample at/above threshold resets the clock.
	lowDwell = 10 * time.Minute
	// probeInterval: how long a stopped channel rests between retests.
	probeInterval = 20 * time.Minute

	// lockWait: a restarted control channel reaches CONTROL within a few
	// seconds when the signal is usable; no lock by now = probe failed.
	lockWait = 15 * time.Second
	// probeTrustAge/probeWindow: vce's syncPercent is a 30s rolling window
	// that is NOT cleared on start, so early samples are dominated by
	// acquisition losses. The verdict is the max measured sample seen between
	// trust-age and window-end after the probe start.
	probeTrustAge = 35 * time.Second
	probeWindow   = 60 * time.Second

	// startupQuiesce/applyQuiesce: no judgements while the world is still
	// settling — after the agent starts (boot config re-import restarts
	// channels) and after any config apply (a full playlist rebuild).
	startupQuiesce = 2 * time.Minute
	applyQuiesce   = 2 * time.Minute
	// actionQuiesce: starting/stopping a channel re-centres a shared dongle
	// and knocks sibling channels to IDLE for ~5s, dragging their decode
	// average down. No judgements for this long after any action of our own.
	actionQuiesce = 60 * time.Second

	// maxStopsPerHour: circuit breaker. More auto-stops than this in an hour
	// means something is systemically wrong (an antenna fault reads exactly
	// like six bad channels) — stop deciding and say so, rather than fight.
	maxStopsPerHour = 6

	// logCap: how many decisions the manager remembers. The ring rides every
	// status frame (the backend persists it), so staff always see the recent
	// history even if nobody was watching when it happened.
	logCap = 50
)

// logEntry is one manager decision for the audit ring: an auto-stop, a probe
// with its verdict, a restore, or a release back to the operator.
type logEntry struct {
	AtMs    int64  `json:"atMs"`
	Channel string `json:"channel"`
	// Kind: "stopped" | "probePass" | "probeFail" | "released" | "recovered"
	Kind string `json:"kind"`
	Text string `json:"text"`
}

// lockedStates mirrors vce's isLockedState: any of these means the channel
// has acquired its control channel (or is actively working).
var lockedStates = map[string]bool{
	"CONTROL": true, "CALL": true, "ENCRYPTED": true, "DATA": true, "ACTIVE": true,
}

// Policy is the operator's say: the per-node kill switch and per-channel
// opt-outs, both from the applied config. The zero value means "enabled,
// nothing opted out" — absence of configuration is ON by design.
type Policy struct {
	Disabled bool
	OptedOut map[string]bool // trimmed channel name -> opted out
}

// Options wires the manager to the control server and the agent's config
// state. Everything is an injected func so tests run without a JVM.
type Options struct {
	// Fetch returns the live configured-channel list (sdrctl.Client.Channels,
	// activeCalls discarded).
	Fetch func() ([]sdrctl.Channel, error)
	// Start/Stop/Suppress/Unsuppress act on a channel by its CURRENT id.
	Start, Stop, Suppress, Unsuppress func(id int) error
	// LastApplyAt reports when the last config apply finished. May block while
	// an apply is in flight — which conveniently pauses the manager for
	// exactly that long.
	LastApplyAt func() time.Time
	// Policy returns the operator's current policy (from the applied config).
	Policy func() Policy
	// Now is the clock; nil = time.Now. Injected for tests.
	Now func() time.Time
	// Sleep waits for d or ctx; nil = real sleep. Injected for tests.
	Sleep func(ctx context.Context, d time.Duration)
}

// chanState is one managed channel's position in the lifecycle.
type chanState string

const (
	stLowWatch    chanState = "lowWatch"    // measured below threshold, dwell running
	stAutoStopped chanState = "autoStopped" // stopped by us, probing every 20m
)

type entry struct {
	state        chanState
	lowSince     time.Time // lowWatch: first measured-low sample of the streak
	stoppedAt    time.Time // autoStopped: when we stopped it
	lastProbeAt  time.Time // autoStopped: last probe (or the stop itself)
	lastProbePct float64   // autoStopped: last probe's verdict; -1 = none / no lock
	reason       string
}

// Manager runs the loop. Construct with New, start with Run.
type Manager struct {
	opts  Options
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration)

	mu      sync.Mutex
	entries map[string]*entry
	probing string // name of the channel currently mid-probe ("" = none)
	logs    []logEntry

	startedAt    time.Time
	lastActionAt time.Time
	stopTimes    []time.Time // auto-stop timestamps inside the breaker window

	seeded        bool
	capWarned     bool // one-time "runtime too old" notice
	breakerWarned bool

	// lastPolicy/lastSupported: cached for Snapshot (status frame).
	lastPolicy    Policy
	lastSupported bool
}

// New builds a Manager.
func New(opts Options) *Manager {
	m := &Manager{opts: opts, now: opts.Now, sleep: opts.Sleep, entries: map[string]*entry{}}
	if m.now == nil {
		m.now = time.Now
	}
	if m.sleep == nil {
		m.sleep = func(ctx context.Context, d time.Duration) {
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
		}
	}
	return m
}

// Run ticks until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	m.startedAt = m.now()
	log.Printf("chanmgr: automatic channel management running (stop below %.0f%% decode for %s, retest every %s)",
		lowThresholdPct, lowDwell, probeInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval + jitter(intervalJitter)):
		}
		m.Tick(ctx)
	}
}

// Tick is one full pass: reconcile, detect, and possibly run one probe.
// Exported for tests; Run is the only production caller.
func (m *Manager) Tick(ctx context.Context) {
	pol := m.opts.Policy()

	chans, err := m.opts.Fetch()
	if err != nil {
		return // control server down or starting — a normal, silent condition
	}
	if len(chans) == 0 {
		return
	}

	// Capability: suppressed/autoStart are nil on a runtime that predates the
	// suppression API. Without it, any stop we issue is undone by the 30s
	// self-heal sweep — so do nothing at all until the runtime updates.
	supported := false
	for i := range chans {
		if chans[i].Suppressed != nil {
			supported = true
			break
		}
	}

	m.mu.Lock()
	m.lastPolicy = pol
	m.lastSupported = supported
	m.mu.Unlock()

	if !supported {
		if !m.capWarned {
			m.capWarned = true
			log.Printf("chanmgr: sdrtrunk runtime predates the suppression API — automatic channel management idle until it updates")
		}
		return
	}

	live := map[string]sdrctl.Channel{}
	for _, ch := range chans {
		name := strings.TrimSpace(ch.Name)
		if name != "" {
			live[name] = ch
		}
	}

	// Kill switch: hand every suppressed channel back to vce (self-heal
	// restarts it within 30s) and forget everything.
	if pol.Disabled {
		for name, ch := range live {
			if ch.Suppressed != nil && *ch.Suppressed {
				if err := m.opts.Unsuppress(ch.ID); err != nil {
					log.Printf("chanmgr: disable: unsuppress [%s] failed: %v", name, err)
				} else {
					log.Printf("chanmgr: disabled — released [%s]; self-heal will restart it", name)
					m.logEvent(name, "released", "auto-management disabled — channel handed back")
				}
			}
		}
		m.mu.Lock()
		if len(m.entries) > 0 {
			m.entries = map[string]*entry{}
		}
		m.mu.Unlock()
		return
	}

	now := m.now()

	// Seed once from live reality: after an agent restart, vce's suppressed
	// flag is the only record of which channels we stopped on purpose.
	if !m.seeded {
		m.seeded = true
		m.mu.Lock()
		for name, ch := range live {
			if ch.Suppressed != nil && *ch.Suppressed && !ch.Processing {
				m.entries[name] = &entry{
					state: stAutoStopped, stoppedAt: now, lastProbeAt: now,
					lastProbePct: -1, reason: "recovered after agent restart (was auto-stopped)",
				}
				log.Printf("chanmgr: [%s] was auto-stopped before the agent restarted — resuming its probe schedule", name)
				m.logEventLocked(name, "recovered", "agent restarted; channel was auto-stopped — probe schedule resumes")
			}
		}
		m.mu.Unlock()
	}

	m.reconcile(live, pol)

	// Quiesce: no judgements, no probes, while the world is settling. A
	// config apply rebuilt every channel, so low-streaks from before it are
	// about a world that no longer exists — drop them.
	lastApply := m.opts.LastApplyAt()
	if !lastApply.IsZero() && now.Sub(lastApply) < applyQuiesce {
		m.mu.Lock()
		for name, e := range m.entries {
			if e.state == stLowWatch {
				delete(m.entries, name)
			}
		}
		m.mu.Unlock()
		return
	}
	if now.Sub(m.startedAt) < startupQuiesce {
		return
	}
	if !m.lastActionAt.IsZero() && now.Sub(m.lastActionAt) < actionQuiesce {
		return
	}

	m.detect(live, pol, now)
	m.maybeProbe(ctx, live, now)
}

// logEvent appends one decision to the audit ring (newest last, capped).
// Caller must NOT hold m.mu.
func (m *Manager) logEvent(channel, kind, text string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logEventLocked(channel, kind, text)
}

// logEventLocked is logEvent for callers already holding m.mu.
func (m *Manager) logEventLocked(channel, kind, text string) {
	m.logs = append(m.logs, logEntry{AtMs: m.now().UnixMilli(), Channel: channel, Kind: kind, Text: text})
	if len(m.logs) > logCap {
		m.logs = m.logs[len(m.logs)-logCap:]
	}
}

// reconcile folds external reality into our state: channels that vanished,
// were restarted by a config push, or were started by an operator.
func (m *Manager) reconcile(live map[string]sdrctl.Channel, pol Policy) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, e := range m.entries {
		ch, ok := live[name]
		if !ok {
			// Channel removed from the config — nothing left to manage.
			delete(m.entries, name)
			continue
		}
		if e.state != stAutoStopped || name == m.probing {
			continue
		}
		suppressed := ch.Suppressed != nil && *ch.Suppressed
		switch {
		case ch.Processing && !suppressed:
			// A config import/reload cleared suppression and restarted it.
			// The world reset — watch it fresh.
			log.Printf("chanmgr: [%s] restarted externally (config reload) — watching it fresh", name)
			m.logEventLocked(name, "released", "restarted by a config reload — watching it fresh")
			delete(m.entries, name)
		case ch.Processing && suppressed:
			// An operator pressed Start while we had it suppressed. The
			// operator wins: release it. If it is still bad, the detector
			// re-stops it after a fresh 10-minute dwell.
			if err := m.opts.Unsuppress(ch.ID); err != nil {
				log.Printf("chanmgr: operator-start release: unsuppress [%s] failed: %v", name, err)
				continue
			}
			log.Printf("chanmgr: [%s] started by operator — released from auto-management until it misbehaves again", name)
			m.logEventLocked(name, "released", "started by operator — released until it misbehaves again")
			delete(m.entries, name)
		case !suppressed:
			// Stopped AND unsuppressed (e.g. our stop landed but the suppress
			// was lost to a reload). Self-heal will restart it; drop state and
			// let the next pass see whatever happens.
			delete(m.entries, name)
		}
	}
}

// detect runs the low-decode dwell over every eligible channel.
func (m *Manager) detect(live map[string]sdrctl.Channel, pol Policy, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, ch := range live {
		e := m.entries[name]
		if e != nil && e.state == stAutoStopped {
			continue
		}

		eligible := ch.Type == "STANDARD" &&
			ch.AutoStart != nil && *ch.AutoStart &&
			ch.Processing &&
			!pol.OptedOut[name]
		if !eligible {
			if e != nil {
				delete(m.entries, name)
			}
			continue
		}

		// Unmeasured (nil or 0): no judgement either way. The streak clock
		// neither resets nor fires — only a measured sample moves anything.
		if ch.SyncPercent == nil || *ch.SyncPercent <= 0 {
			continue
		}
		pct := *ch.SyncPercent

		if pct >= lowThresholdPct {
			if e != nil {
				delete(m.entries, name)
			}
			continue
		}

		if e == nil {
			m.entries[name] = &entry{state: stLowWatch, lowSince: now}
			continue
		}
		if now.Sub(e.lowSince) < lowDwell {
			continue
		}

		// Dwell complete on a measured-low sample: stop the channel.
		if !m.breakerAllows(now) {
			if !m.breakerWarned {
				m.breakerWarned = true
				log.Printf("chanmgr: %d auto-stops inside an hour — circuit breaker open, no further stops (is the antenna/SDR itself failing?)", maxStopsPerHour)
			}
			continue
		}
		// Suppress BEFORE stop, so the self-heal sweep can't restart it in
		// the gap between the two calls.
		if err := m.opts.Suppress(ch.ID); err != nil {
			log.Printf("chanmgr: suppress [%s] failed: %v — not stopping", name, err)
			continue
		}
		if err := m.opts.Stop(ch.ID); err != nil {
			log.Printf("chanmgr: stop [%s] failed: %v — releasing suppression", name, err)
			if uerr := m.opts.Unsuppress(ch.ID); uerr != nil {
				log.Printf("chanmgr: unsuppress [%s] after failed stop also failed: %v", name, uerr)
			}
			continue
		}
		low := now.Sub(e.lowSince).Round(time.Second)
		log.Printf("chanmgr: AUTO-STOPPED [%s] — decode %.0f%% below %.0f%% for %s; retesting every %s", name, pct, lowThresholdPct, low, probeInterval)
		m.logEventLocked(name, "stopped", fmt.Sprintf("auto-stopped — decode %.0f%% below %.0f%% for %s", pct, lowThresholdPct, low))
		*e = entry{
			state: stAutoStopped, stoppedAt: now, lastProbeAt: now, lastProbePct: -1,
			reason: "decode below 40% for 10 minutes",
		}
		m.lastActionAt = now
		m.stopTimes = append(m.stopTimes, now)
		m.breakerWarned = false
	}
}

// maybeProbe runs AT MOST ONE probe per tick, inline (blocking this tick is
// what serialises probes node-wide): the auto-stopped channel that has waited
// longest, once its 20 minutes are up.
func (m *Manager) maybeProbe(ctx context.Context, live map[string]sdrctl.Channel, now time.Time) {
	m.mu.Lock()
	var name string
	var oldest time.Time
	for n, e := range m.entries {
		if e.state != stAutoStopped || now.Sub(e.lastProbeAt) < probeInterval {
			continue
		}
		if name == "" || e.lastProbeAt.Before(oldest) {
			name, oldest = n, e.lastProbeAt
		}
	}
	if name == "" {
		m.mu.Unlock()
		return
	}
	ch, ok := live[name]
	m.probing = name
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.probing = ""
		m.mu.Unlock()
	}()

	if !ok || ch.Processing {
		return // reconcile will sort it out next tick
	}
	m.probe(ctx, name, ch.ID)
}

// probe starts a stopped channel, waits for lock, waits for the decode metric
// to become trustworthy, and either restores the channel or re-stops it.
// The channel stays suppressed throughout, so the self-heal sweep cannot
// interfere mid-probe.
func (m *Manager) probe(ctx context.Context, name string, id int) {
	finish := func(verdictPct float64, keep bool, detail string) {
		now := m.now()
		m.lastActionAt = now
		m.mu.Lock()
		defer m.mu.Unlock()
		e := m.entries[name]
		if e == nil {
			return
		}
		if keep {
			delete(m.entries, name)
			return
		}
		e.lastProbeAt = now
		e.lastProbePct = verdictPct
		e.reason = detail
	}

	if err := m.opts.Start(id); err != nil {
		// Typically "No Tuner Available" — another channel took the freed
		// bandwidth. Not a verdict about RF; try again next interval.
		log.Printf("chanmgr: probe [%s]: start failed: %v — retrying in %s", name, probeInterval, err)
		m.logEvent(name, "probeFail", "test could not start ("+err.Error()+") — retry in 20 min")
		finish(-1, false, "probe could not start: "+err.Error())
		return
	}
	m.lastActionAt = m.now()
	started := m.now()

	// Phase 1: lock. CONTROL arrives within a few seconds on a usable signal.
	locked := false
	for m.now().Sub(started) < lockWait && ctx.Err() == nil {
		m.sleep(ctx, time.Second)
		ch, ok := m.find(name)
		if !ok {
			break
		}
		if ch.Control || lockedStates[ch.State] {
			locked = true
			break
		}
	}
	if ctx.Err() != nil {
		return
	}
	if !locked {
		m.stopAfterProbe(name)
		log.Printf("chanmgr: probe [%s] FAILED — no lock within %s; next retry in %s", name, lockWait, probeInterval)
		m.logEvent(name, "probeFail", fmt.Sprintf("test failed — no lock within %s; retry in 20 min", lockWait))
		finish(-1, false, "probe failed: no lock")
		return
	}

	// Phase 2: verdict. syncPercent is a 30s rolling window not cleared on
	// start, so only samples old enough to have outgrown the acquisition
	// period count; take the best of them.
	best := -1.0
	for m.now().Sub(started) < probeWindow && ctx.Err() == nil {
		m.sleep(ctx, 2*time.Second)
		age := m.now().Sub(started)
		if age < probeTrustAge {
			continue
		}
		ch, ok := m.find(name)
		if !ok {
			break
		}
		if ch.SyncPercent != nil && *ch.SyncPercent > 0 && *ch.SyncPercent > best {
			best = *ch.SyncPercent
		}
	}
	if ctx.Err() != nil {
		return
	}

	if best >= lowThresholdPct {
		if err := m.opts.Unsuppress(id); err != nil {
			log.Printf("chanmgr: probe [%s] passed (%.0f%%) but unsuppress failed: %v — leaving running, retry clears it", name, best, err)
			m.logEvent(name, "probeFail", fmt.Sprintf("test passed (%.0f%%) but release failed — retrying", best))
			finish(best, false, "restored but unsuppress failed")
			return
		}
		log.Printf("chanmgr: probe [%s] PASSED — decode %.0f%%; channel restored", name, best)
		m.logEvent(name, "probePass", fmt.Sprintf("test passed — decode %.0f%%; channel restored", best))
		finish(best, true, "")
		return
	}
	m.stopAfterProbe(name)
	if best < 0 {
		log.Printf("chanmgr: probe [%s] FAILED — locked but decode never measured; next retry in %s", name, probeInterval)
		m.logEvent(name, "probeFail", "test failed — locked but decode never measured; retry in 20 min")
		finish(-1, false, "probe failed: decode unmeasured")
		return
	}
	log.Printf("chanmgr: probe [%s] FAILED — decode %.0f%% still below %.0f%%; next retry in %s", name, best, lowThresholdPct, probeInterval)
	m.logEvent(name, "probeFail", fmt.Sprintf("test failed — decode %.0f%% still below %.0f%%; retry in 20 min", best, lowThresholdPct))
	finish(best, false, "probe failed: still below 40%")
}

// stopAfterProbe re-stops the probe channel by its CURRENT id (the start may
// have been preceded by reloads; never trust a stale id).
func (m *Manager) stopAfterProbe(name string) {
	ch, ok := m.find(name)
	if !ok {
		return
	}
	if err := m.opts.Stop(ch.ID); err != nil {
		log.Printf("chanmgr: probe [%s]: re-stop failed: %v (channel left running, still suppressed; next pass retries)", name, err)
	}
}

// find fetches the live list and returns the named channel.
func (m *Manager) find(name string) (sdrctl.Channel, bool) {
	chans, err := m.opts.Fetch()
	if err != nil {
		return sdrctl.Channel{}, false
	}
	for _, ch := range chans {
		if strings.TrimSpace(ch.Name) == name {
			return ch, true
		}
	}
	return sdrctl.Channel{}, false
}

// breakerAllows reports whether another auto-stop is allowed right now, and
// prunes the window. Caller holds m.mu.
func (m *Manager) breakerAllows(now time.Time) bool {
	keep := m.stopTimes[:0]
	for _, t := range m.stopTimes {
		if now.Sub(t) < time.Hour {
			keep = append(keep, t)
		}
	}
	m.stopTimes = keep
	return len(m.stopTimes) < maxStopsPerHour
}

// Snapshot renders the manager's state for the status frame. Times are epoch
// milliseconds; the UI joins channels by name, same as everywhere else.
func (m *Manager) Snapshot() any {
	m.mu.Lock()
	defer m.mu.Unlock()

	channels := map[string]any{}
	for name, e := range m.entries {
		state := string(e.state)
		if name == m.probing {
			state = "probing"
		}
		row := map[string]any{"state": state, "reason": e.reason}
		switch e.state {
		case stLowWatch:
			row["sinceMs"] = e.lowSince.UnixMilli()
		case stAutoStopped:
			row["sinceMs"] = e.stoppedAt.UnixMilli()
			row["lastProbeAtMs"] = e.lastProbeAt.UnixMilli()
			row["nextProbeAtMs"] = e.lastProbeAt.Add(probeInterval).UnixMilli()
			if e.lastProbePct >= 0 {
				row["lastProbePct"] = e.lastProbePct
			}
		}
		channels[name] = row
	}
	logs := make([]logEntry, len(m.logs))
	copy(logs, m.logs)
	return map[string]any{
		"enabled":   !m.lastPolicy.Disabled,
		"supported": m.lastSupported,
		"channels":  channels,
		"log":       logs,
	}
}

func jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(time.Now().UnixNano() % int64(max))
}
