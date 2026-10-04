// Package chanmgr is the automatic channel manager: a channel whose decode
// health sits below 50% for 10 continuous minutes is stopped (it is burning a
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

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/decodeprobe"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
)

const (
	// pollInterval: one detection pass per tick. Matches the status heartbeat
	// cadence; the 10-minute dwell makes anything faster pointless.
	pollInterval   = 15 * time.Second
	intervalJitter = 2 * time.Second

	// lowThresholdPct: decode health below this is "not working".
	lowThresholdPct = 60.0
	// lowDwell: how long a channel must stay below threshold before it is
	// stopped. Every measured sample at/above threshold resets the clock.
	lowDwell = 10 * time.Minute
	// severeThresholdPct/severeDwell: the fast tier. Below 40% the channel is
	// not marginal, it is dead air — no need to wait out the full 10 minutes.
	// Its clock runs alongside the 60% one: a sample in [40,60) resets only
	// the severe clock, a sample at/above 60 resets both.
	severeThresholdPct = 40.0
	severeDwell        = 5 * time.Minute
	// firstProbeDelay/probeInterval: the FIRST retest comes quickly — a channel
	// stopped by a passing problem (a brief interference spike, a neighbouring
	// start perturbing the dongle) should not sit dead for half an hour on that
	// evidence. Once one retest has already failed, the problem looks durable
	// and the slower cadence applies.
	firstProbeDelay = 10 * time.Minute
	probeInterval   = 30 * time.Minute

	// recoverSamples: how many CONSECUTIVE measured samples must sit at or
	// above a threshold before the channel is credited with recovering past
	// it. A bad channel routinely throws a lone good reading — observed on a
	// site that decoded badly for eight hours and spiked often enough that a
	// single-sample reset meant it was never stopped at all. At the 15s poll
	// this is a minute of sustained evidence.
	recoverSamples = 4

	// The retest measurement itself lives in decodeprobe, shared with the RF
	// site survey so both judge a frequency the same way. These aliases keep
	// the manager's own prose (and its tests) reading in local terms.
	lockWait        = decodeprobe.LockWait
	probeTrustAge   = decodeprobe.TrustAge
	probeWindow     = decodeprobe.Window
	probeMinSamples = decodeprobe.MinSamples

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
	// Paused, when it returns true, suspends the manager entirely for this
	// tick: no reconcile, no detection, no probes. The RF site survey holds it
	// while it has the node's channel set replaced with test channels — a
	// world the manager must not reason about, let alone act on. nil = never
	// paused.
	Paused func() bool
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
	lowSince     time.Time // lowWatch: first measured sample below the low threshold, this streak
	severeSince  time.Time // lowWatch: first measured sample below the severe threshold, this streak (zero = none)
	goodLow      int       // consecutive measured samples at/above the low threshold
	goodSevere   int       // consecutive measured samples at/above the severe threshold
	stoppedAt    time.Time // autoStopped: when we stopped it
	lastProbeAt  time.Time // autoStopped: last probe (or the stop itself)
	probes       int       // autoStopped: retests run so far (0 = none yet → firstProbeDelay)
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
	log.Printf("chanmgr: automatic channel management running (stop below %.0f%% for %s or below %.0f%% for %s, first retest after %s then every %s)",
		lowThresholdPct, lowDwell, severeThresholdPct, severeDwell, firstProbeDelay, probeInterval)
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
	// Paused first, before anything is even read: during a site survey the
	// live channel list is the survey's, not the operator's.
	if m.opts.Paused != nil && m.opts.Paused() {
		return
	}
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
					// probes=1: this channel was already stopped before the
					// restart, so it has earned the slower cadence, not a
					// fresh fast first probe.
					state: stAutoStopped, stoppedAt: now, lastProbeAt: now, probes: 1,
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

// goodLowReset clears the low-threshold recovery run when a sample falls back
// below it. Safe on a nil entry (the first bad sample of a new streak).
func (e *entry) goodLowReset(pct float64) {
	if e == nil {
		return
	}
	if pct < lowThresholdPct {
		e.goodLow = 0
	}
}

// probeDue is how long this channel waits before its next retest: the short
// delay until the first one has run, the normal interval after that.
func (e *entry) probeDue() time.Duration {
	if e.probes == 0 {
		return firstProbeDelay
	}
	return probeInterval
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
			if e == nil {
				continue // healthy and untracked: nothing to do
			}
			// Healthy — but only a SUSTAINED run of healthy samples clears the
			// watch. One spike inside a long bad patch proves nothing.
			e.goodLow++
			e.goodSevere++
			if e.goodLow >= recoverSamples {
				delete(m.entries, name)
			}
			continue
		}
		e.goodLowReset(pct)

		if e == nil {
			e = &entry{state: stLowWatch, lowSince: now}
			if pct < severeThresholdPct {
				e.severeSince = now
			}
			m.entries[name] = e
			continue
		}
		// The severe clock runs while samples stay below the severe threshold.
		// A reading in [severe, low) ends that streak — again only once it is
		// sustained — while the low streak keeps running underneath.
		if pct >= severeThresholdPct {
			e.goodSevere++
			if e.goodSevere >= recoverSamples {
				e.severeSince = time.Time{}
			}
		} else {
			e.goodSevere = 0
			if e.severeSince.IsZero() {
				e.severeSince = now
			}
		}

		var rule string // which rule completed, for the log/reason
		switch {
		case !e.severeSince.IsZero() && now.Sub(e.severeSince) >= severeDwell:
			rule = fmt.Sprintf("below %.0f%% for %s", severeThresholdPct, now.Sub(e.severeSince).Round(time.Second))
		case now.Sub(e.lowSince) >= lowDwell:
			rule = fmt.Sprintf("below %.0f%% for %s", lowThresholdPct, now.Sub(e.lowSince).Round(time.Second))
		default:
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
		log.Printf("chanmgr: AUTO-STOPPED [%s] — decode %.0f%%, %s; retesting every %s", name, pct, rule, probeInterval)
		m.logEventLocked(name, "stopped", fmt.Sprintf("auto-stopped — decode %.0f%%, %s", pct, rule))
		*e = entry{
			state: stAutoStopped, stoppedAt: now, lastProbeAt: now, lastProbePct: -1,
			reason: "decode " + rule,
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
		if e.state != stAutoStopped || now.Sub(e.lastProbeAt) < e.probeDue() {
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
		e.probes++
		e.lastProbePct = verdictPct
		e.reason = detail
	}

	if err := m.opts.Start(id); err != nil {
		// Typically "No Tuner Available" — another channel took the freed
		// bandwidth. Not a verdict about RF; try again next interval.
		log.Printf("chanmgr: probe [%s]: start failed: %v — retrying in %s", name, probeInterval, err)
		m.logEvent(name, "probeFail", fmt.Sprintf("test could not start (%s) — retry in %s", err.Error(), probeInterval))
		finish(-1, false, "probe could not start: "+err.Error())
		return
	}
	m.lastActionAt = m.now()

	// The measurement — wait for lock, discard the acquisition period, take
	// the median of what is left — is decodeprobe's job, shared with the site
	// survey. A batch of one: the manager probes strictly one channel at a
	// time, node-wide.
	res := decodeprobe.Measure(ctx, []string{name}, m.probeDeps())[name]
	if ctx.Err() != nil {
		return
	}
	if res.Outcome == decodeprobe.NoLock {
		m.stopAfterProbe(name)
		log.Printf("chanmgr: probe [%s] FAILED — no lock within %s; next retry in %s", name, lockWait, probeInterval)
		m.logEvent(name, "probeFail", fmt.Sprintf("test failed — no lock within %s; retry in %s", lockWait, probeInterval))
		finish(-1, false, "probe failed: no lock")
		return
	}
	best := res.MedianPct

	if best >= lowThresholdPct {
		if err := m.opts.Unsuppress(id); err != nil {
			log.Printf("chanmgr: probe [%s] passed (%.0f%%) but unsuppress failed: %v — leaving running, retry clears it", name, best, err)
			m.logEvent(name, "probeFail", fmt.Sprintf("test passed (%.0f%%) but release failed — retrying", best))
			finish(best, false, "restored but unsuppress failed")
			return
		}
		log.Printf("chanmgr: probe [%s] PASSED — median decode %.0f%%; channel restored", name, best)
		m.logEvent(name, "probePass", fmt.Sprintf("test passed — median decode %.0f%%; channel restored", best))
		finish(best, true, "")
		return
	}
	m.stopAfterProbe(name)
	if best < 0 {
		log.Printf("chanmgr: probe [%s] FAILED — locked but decode never measured; next retry in %s", name, probeInterval)
		m.logEvent(name, "probeFail", fmt.Sprintf("test failed — locked but decode never measured; retry in %s", probeInterval))
		finish(-1, false, "probe failed: decode unmeasured")
		return
	}
	log.Printf("chanmgr: probe [%s] FAILED — median decode %.0f%% still below %.0f%%; next retry in %s", name, best, lowThresholdPct, probeInterval)
	m.logEvent(name, "probeFail", fmt.Sprintf("test failed — median decode %.0f%% still below %.0f%%; retry in %s", best, lowThresholdPct, probeInterval))
	finish(best, false, "probe failed: still below 40%")
}

// medianPct is the middle of the measured probe samples, or -1 when there were
// too few to rule on.
func medianPct(in []float64) float64 { return decodeprobe.Median(in) }

// probeDeps wires decodeprobe to the manager's injected clock and fetch.
func (m *Manager) probeDeps() decodeprobe.Deps {
	return decodeprobe.Deps{
		Now:   m.now,
		Sleep: m.sleep,
		Fetch: func() (map[string]decodeprobe.Snapshot, error) {
			chans, err := m.opts.Fetch()
			if err != nil {
				return nil, err
			}
			out := make(map[string]decodeprobe.Snapshot, len(chans))
			for _, ch := range chans {
				out[strings.TrimSpace(ch.Name)] = decodeprobe.Snapshot{
					Control: ch.Control, State: ch.State,
					SyncPercent: ch.SyncPercent, SignalDbfs: ch.SignalDbfs,
				}
			}
			return out, nil
		},
	}
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
			row["nextProbeAtMs"] = e.lastProbeAt.Add(e.probeDue()).UnixMilli()
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
