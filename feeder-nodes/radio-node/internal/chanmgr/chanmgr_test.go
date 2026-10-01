package chanmgr

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
)

// fake is the whole world: the clock, the live channel list, and a recorder of
// every control-server call the manager makes. Sleep advances the clock
// instantly, so probes run in microseconds of real time.
type fake struct {
	now   time.Time
	chans map[string]*sdrctl.Channel
	order []string // insertion order, for deterministic Fetch
	calls []string // "suppress:Name", "stop:Name", ...
	pol   Policy
	apply time.Time

	startErr error

	// onTick lets a test mutate the world as the probe's inner fetches happen
	// (e.g. lock after N seconds).
	onFetch func(f *fake)
}

func newFake() *fake {
	return &fake{now: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), chans: map[string]*sdrctl.Channel{}}
}

func bptr(b bool) *bool       { return &b }
func fptr(v float64) *float64 { return &v }

// add registers a managed-capable STANDARD autoStart channel.
func (f *fake) add(id int, name string, syncPct *float64) *sdrctl.Channel {
	c := &sdrctl.Channel{
		ID: id, Name: name, Type: "STANDARD", Processing: true, State: "CONTROL", Control: true,
		SyncPercent: syncPct, AutoStart: bptr(true), Suppressed: bptr(false),
	}
	f.chans[name] = c
	f.order = append(f.order, name)
	return c
}

func (f *fake) byID(id int) *sdrctl.Channel {
	for _, c := range f.chans {
		if c.ID == id {
			return c
		}
	}
	return nil
}

func (f *fake) record(verb string, id int) {
	name := fmt.Sprintf("#%d", id)
	if c := f.byID(id); c != nil {
		name = c.Name
	}
	f.calls = append(f.calls, verb+":"+name)
}

func (f *fake) manager() *Manager {
	return New(Options{
		Fetch: func() ([]sdrctl.Channel, error) {
			if f.onFetch != nil {
				f.onFetch(f)
			}
			out := make([]sdrctl.Channel, 0, len(f.order))
			for _, n := range f.order {
				out = append(out, *f.chans[n])
			}
			return out, nil
		},
		Start: func(id int) error {
			f.record("start", id)
			if f.startErr != nil {
				return f.startErr
			}
			if c := f.byID(id); c != nil {
				c.Processing = true
			}
			return nil
		},
		Stop: func(id int) error {
			f.record("stop", id)
			if c := f.byID(id); c != nil {
				c.Processing = false
				c.State = "STOPPED"
				c.Control = false
			}
			return nil
		},
		Suppress: func(id int) error {
			f.record("suppress", id)
			if c := f.byID(id); c != nil {
				c.Suppressed = bptr(true)
			}
			return nil
		},
		Unsuppress: func(id int) error {
			f.record("unsuppress", id)
			if c := f.byID(id); c != nil {
				c.Suppressed = bptr(false)
			}
			return nil
		},
		LastApplyAt: func() time.Time { return f.apply },
		Policy:      func() Policy { return f.pol },
		Now:         func() time.Time { return f.now },
		Sleep:       func(_ context.Context, d time.Duration) { f.now = f.now.Add(d) },
	})
}

// seedPast marks startup/apply quiesce as long over.
func (f *fake) seedPast(m *Manager) {
	m.startedAt = f.now.Add(-time.Hour)
	m.seeded = true
}

// runDwell ticks the manager with the channel low until just before the dwell
// would fire.
func runDwell(m *Manager, f *fake, d time.Duration) {
	ctx := context.Background()
	end := f.now.Add(d)
	for f.now.Before(end) {
		m.Tick(ctx)
		f.now = f.now.Add(pollInterval)
	}
}

func TestDwellFiresAtTenMinutesSuppressBeforeStop(t *testing.T) {
	f := newFake()
	f.add(1, "RFS Illawarra", fptr(20))
	m := f.manager()
	f.seedPast(m)

	runDwell(m, f, lowDwell-time.Minute)
	if len(f.calls) != 0 {
		t.Fatalf("acted before the dwell completed: %v", f.calls)
	}

	runDwell(m, f, 2*time.Minute)
	if len(f.calls) < 2 || f.calls[0] != "suppress:RFS Illawarra" || f.calls[1] != "stop:RFS Illawarra" {
		t.Fatalf("want suppress then stop, got %v", f.calls)
	}
	snap := m.Snapshot().(map[string]any)
	row := snap["channels"].(map[string]any)["RFS Illawarra"].(map[string]any)
	if row["state"] != "autoStopped" {
		t.Fatalf("want autoStopped, got %v", row["state"])
	}
	logs := snap["log"].([]logEntry)
	if len(logs) != 1 || logs[0].Kind != "stopped" || logs[0].Channel != "RFS Illawarra" {
		t.Fatalf("want one 'stopped' log entry, got %+v", logs)
	}
}

func TestLogRingCapsAtFifty(t *testing.T) {
	f := newFake()
	m := f.manager()
	for i := 0; i < logCap+20; i++ {
		m.logEvent(fmt.Sprintf("C%d", i), "stopped", "x")
	}
	logs := m.Snapshot().(map[string]any)["log"].([]logEntry)
	if len(logs) != logCap {
		t.Fatalf("want %d entries, got %d", logCap, len(logs))
	}
	// Newest survive: the first 20 must have been shed.
	if logs[0].Channel != "C20" || logs[logCap-1].Channel != fmt.Sprintf("C%d", logCap+19) {
		t.Fatalf("ring kept the wrong end: first=%s last=%s", logs[0].Channel, logs[logCap-1].Channel)
	}
}

func TestProbeOutcomesAreLogged(t *testing.T) {
	f := newFake()
	c := f.add(1, "A", fptr(5))
	m := f.manager()
	f.seedPast(m)
	runDwell(m, f, lowDwell+time.Minute)

	f.onFetch = func(f *fake) {
		if c.Processing {
			c.State = "CONTROL"
			c.Control = true
			c.SyncPercent = fptr(88)
		}
	}
	f.now = f.now.Add(probeInterval + actionQuiesce)
	m.Tick(context.Background())

	logs := m.Snapshot().(map[string]any)["log"].([]logEntry)
	kinds := make([]string, len(logs))
	for i, e := range logs {
		kinds[i] = e.Kind
	}
	if len(logs) != 2 || kinds[0] != "stopped" || kinds[1] != "probePass" {
		t.Fatalf("want [stopped probePass], got %v", kinds)
	}
}

func TestHealthySampleResetsDwell(t *testing.T) {
	f := newFake()
	c := f.add(1, "A", fptr(20))
	m := f.manager()
	f.seedPast(m)

	runDwell(m, f, lowDwell-time.Minute)
	c.SyncPercent = fptr(80) // one good sample
	m.Tick(context.Background())
	c.SyncPercent = fptr(20) // low again — the clock must restart
	runDwell(m, f, lowDwell-time.Minute)
	if len(f.calls) != 0 {
		t.Fatalf("dwell did not reset on a healthy sample: %v", f.calls)
	}
}

func TestSevereDwellFiresAtFiveMinutes(t *testing.T) {
	f := newFake()
	f.add(1, "A", fptr(10)) // dead air: severe tier
	m := f.manager()
	f.seedPast(m)

	runDwell(m, f, severeDwell-time.Minute)
	if len(f.calls) != 0 {
		t.Fatalf("severe rule fired before its 5-minute dwell: %v", f.calls)
	}
	runDwell(m, f, 2*time.Minute)
	if len(f.calls) < 2 || f.calls[0] != "suppress:A" || f.calls[1] != "stop:A" {
		t.Fatalf("want suppress then stop at ~5m, got %v", f.calls)
	}
	e := m.entries["A"]
	if e == nil || e.state != stAutoStopped || !strings.Contains(e.reason, "below 20%") {
		t.Fatalf("want a below-20%% reason, got %+v", e)
	}
}

func TestMidBandSampleResetsOnlySevereClock(t *testing.T) {
	// 4 minutes of dead air, one 25% sample, dead air again: the severe clock
	// restarts (no stop at the 5-minute mark of the ORIGINAL streak) but the
	// 40% clock keeps running from the very first low sample.
	f := newFake()
	c := f.add(1, "A", fptr(10))
	m := f.manager()
	f.seedPast(m)

	runDwell(m, f, 4*time.Minute)
	c.SyncPercent = fptr(25) // low, but not severe
	m.Tick(context.Background())
	c.SyncPercent = fptr(10)
	runDwell(m, f, severeDwell-time.Minute)
	if len(f.calls) != 0 {
		t.Fatalf("severe clock did not reset on a 25%% sample: %v", f.calls)
	}
	runDwell(m, f, 2*time.Minute) // severe restart completes its 5 minutes
	if len(f.calls) < 2 || f.calls[1] != "stop:A" {
		t.Fatalf("want the restarted severe streak to fire, got %v", f.calls)
	}
}

func TestSlowTierStillFiresWithoutSevere(t *testing.T) {
	// 25% the whole time: never severe, so the stop lands on the 10-minute
	// rule — and not a tick before.
	f := newFake()
	f.add(1, "A", fptr(25))
	m := f.manager()
	f.seedPast(m)

	runDwell(m, f, lowDwell-time.Minute)
	if len(f.calls) != 0 {
		t.Fatalf("25%% fired early: %v", f.calls)
	}
	runDwell(m, f, 2*time.Minute)
	if len(f.calls) < 2 || f.calls[1] != "stop:A" {
		t.Fatalf("want the 10-minute rule to fire, got %v", f.calls)
	}
	e := m.entries["A"]
	if e == nil || !strings.Contains(e.reason, "below 40%") {
		t.Fatalf("want a below-40%% reason, got %+v", e)
	}
}

func TestUnmeasuredNeverFires(t *testing.T) {
	f := newFake()
	f.add(1, "NilChan", nil)      // no monitor at all
	f.add(2, "ZeroChan", fptr(0)) // defaulted zero = unmeasured
	m := f.manager()
	f.seedPast(m)

	runDwell(m, f, 2*lowDwell)
	if len(f.calls) != 0 {
		t.Fatalf("acted on unmeasured channels: %v", f.calls)
	}
}

func TestUnmeasuredTailCannotFire(t *testing.T) {
	// Low for 9 minutes, then the metric goes stale (nil): the streak may not
	// fire off the stale tail.
	f := newFake()
	c := f.add(1, "A", fptr(20))
	m := f.manager()
	f.seedPast(m)

	runDwell(m, f, lowDwell-time.Minute)
	c.SyncPercent = nil
	runDwell(m, f, lowDwell)
	if len(f.calls) != 0 {
		t.Fatalf("fired on an unmeasured sample: %v", f.calls)
	}
}

func TestIneligibleChannelsIgnored(t *testing.T) {
	f := newFake()
	conv := f.add(1, "Airband", fptr(10))
	conv.Type = "STANDARD"
	conv.SyncPercent = fptr(10)
	noAuto := f.add(2, "Manual", fptr(10))
	noAuto.AutoStart = bptr(false)
	f.add(3, "OptedOut", fptr(10))
	traffic := f.add(4, "Traffic", fptr(10))
	traffic.Type = "TRAFFIC"
	f.pol = Policy{OptedOut: map[string]bool{"OptedOut": true}}
	m := f.manager()
	f.seedPast(m)

	runDwell(m, f, 2*lowDwell)
	for _, call := range f.calls {
		if strings.Contains(call, "Manual") || strings.Contains(call, "OptedOut") || strings.Contains(call, "Traffic") {
			t.Fatalf("acted on an ineligible channel: %v", f.calls)
		}
	}
	// "Airband" IS eligible here (STANDARD + autoStart + measured low) — it
	// stands in for a real trunked channel and proves eligibility still works.
	if len(f.calls) == 0 {
		t.Fatalf("the eligible channel was never stopped")
	}
}

func TestOldRuntimeIsANoop(t *testing.T) {
	f := newFake()
	c := f.add(1, "A", fptr(5))
	c.AutoStart = nil
	c.Suppressed = nil // runtime predates the suppression API
	m := f.manager()
	f.seedPast(m)

	runDwell(m, f, 2*lowDwell)
	if len(f.calls) != 0 {
		t.Fatalf("acted against an old runtime: %v", f.calls)
	}
}

func TestQuiesceBlocksDetection(t *testing.T) {
	ctx := context.Background()

	// Startup quiesce.
	f := newFake()
	f.add(1, "A", fptr(5))
	m := f.manager()
	m.startedAt = f.now
	m.seeded = true
	runDwell(m, f, startupQuiesce/2)
	if len(m.entries) != 0 {
		t.Fatalf("watched during startup quiesce")
	}

	// Apply quiesce: an apply mid-dwell clears the streak. 25% keeps this on
	// the slow tier only (the 20%/5m severe rule must not engage here).
	f2 := newFake()
	f2.add(1, "A", fptr(25))
	m2 := f2.manager()
	f2.seedPast(m2)
	runDwell(m2, f2, lowDwell-time.Minute)
	f2.apply = f2.now // config apply happens now
	m2.Tick(ctx)
	if len(m2.entries) != 0 {
		t.Fatalf("low streak survived a config apply")
	}
	runDwell(m2, f2, lowDwell-time.Minute)
	if len(f2.calls) != 0 {
		t.Fatalf("fired without a full fresh dwell after an apply: %v", f2.calls)
	}

	// Own-action quiesce.
	f3 := newFake()
	f3.add(1, "A", fptr(5))
	f3.add(2, "B", fptr(5))
	m3 := f3.manager()
	f3.seedPast(m3)
	runDwell(m3, f3, lowDwell+time.Minute) // A (or B) gets stopped
	stops := len(f3.calls)
	m3.lastActionAt = f3.now
	m3.Tick(ctx) // inside action quiesce: the sibling must not be judged
	if len(f3.calls) != stops {
		t.Fatalf("acted during own-action quiesce: %v", f3.calls)
	}
}

func TestProbeNoLockFailsFast(t *testing.T) {
	f := newFake()
	c := f.add(1, "A", fptr(5))
	m := f.manager()
	f.seedPast(m)
	runDwell(m, f, lowDwell+time.Minute)
	if !*c.Suppressed {
		t.Fatalf("channel should be auto-stopped before the probe; calls %v", f.calls)
	}
	f.calls = nil

	// Probe time. The channel starts but never locks.
	f.onFetch = func(f *fake) {
		if c.Processing {
			c.State = "IDLE"
			c.Control = false
		}
	}
	f.now = f.now.Add(probeInterval + actionQuiesce)
	m.Tick(context.Background())

	want := []string{"start:A", "stop:A"}
	if len(f.calls) != 2 || f.calls[0] != want[0] || f.calls[1] != want[1] {
		t.Fatalf("want %v, got %v", want, f.calls)
	}
	if *c.Suppressed != true {
		t.Fatalf("channel must stay suppressed after a failed probe")
	}
}

func TestProbePassRestores(t *testing.T) {
	f := newFake()
	c := f.add(1, "A", fptr(5))
	m := f.manager()
	f.seedPast(m)
	runDwell(m, f, lowDwell+time.Minute)
	f.calls = nil

	started := time.Time{}
	f.onFetch = func(f *fake) {
		if !c.Processing {
			return
		}
		if started.IsZero() {
			started = f.now
		}
		c.State = "CONTROL"
		c.Control = true
		// Early samples read low (acquisition in the 30s window); honest
		// numbers only after the trust age.
		if f.now.Sub(started) < probeTrustAge {
			c.SyncPercent = fptr(15)
		} else {
			c.SyncPercent = fptr(88)
		}
	}
	f.now = f.now.Add(probeInterval + actionQuiesce)
	m.Tick(context.Background())

	if len(f.calls) != 2 || f.calls[0] != "start:A" || f.calls[1] != "unsuppress:A" {
		t.Fatalf("want start then unsuppress, got %v", f.calls)
	}
	if len(m.entries) != 0 {
		t.Fatalf("restored channel still tracked: %+v", m.entries)
	}
}

func TestProbeFailKeepsStopped(t *testing.T) {
	f := newFake()
	c := f.add(1, "A", fptr(5))
	m := f.manager()
	f.seedPast(m)
	runDwell(m, f, lowDwell+time.Minute)
	f.calls = nil

	f.onFetch = func(f *fake) {
		if c.Processing {
			c.State = "CONTROL"
			c.Control = true
			c.SyncPercent = fptr(22) // locked, but still rubbish
		}
	}
	f.now = f.now.Add(probeInterval + actionQuiesce)
	m.Tick(context.Background())

	if len(f.calls) != 2 || f.calls[0] != "start:A" || f.calls[1] != "stop:A" {
		t.Fatalf("want start then stop, got %v", f.calls)
	}
	e := m.entries["A"]
	if e == nil || e.state != stAutoStopped || e.lastProbePct != 22 {
		t.Fatalf("bad post-probe state: %+v", e)
	}
}

func TestOneProbePerTick(t *testing.T) {
	f := newFake()
	a := f.add(1, "A", fptr(5))
	b := f.add(2, "B", fptr(5))
	m := f.manager()
	f.seedPast(m)
	runDwell(m, f, lowDwell+2*time.Minute)
	if !(*a.Suppressed && *b.Suppressed) {
		t.Fatalf("both channels should be auto-stopped; calls %v", f.calls)
	}
	f.calls = nil

	f.onFetch = func(f *fake) {
		for _, c := range []*sdrctl.Channel{a, b} {
			if c.Processing {
				c.State = "IDLE"
				c.Control = false
			}
		}
	}
	f.now = f.now.Add(probeInterval + time.Hour)
	m.Tick(context.Background())

	starts := 0
	for _, call := range f.calls {
		if strings.HasPrefix(call, "start:") {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("want exactly one probe per tick, got %d (%v)", starts, f.calls)
	}
}

func TestKillSwitchReleasesEverything(t *testing.T) {
	f := newFake()
	f.add(1, "A", fptr(5))
	m := f.manager()
	f.seedPast(m)
	runDwell(m, f, lowDwell+time.Minute)
	f.calls = nil

	f.pol = Policy{Disabled: true}
	m.Tick(context.Background())

	if len(f.calls) != 1 || f.calls[0] != "unsuppress:A" {
		t.Fatalf("want unsuppress on disable, got %v", f.calls)
	}
	if len(m.entries) != 0 {
		t.Fatalf("state survived the kill switch")
	}
}

func TestOperatorStartWins(t *testing.T) {
	f := newFake()
	c := f.add(1, "A", fptr(5))
	m := f.manager()
	f.seedPast(m)
	runDwell(m, f, lowDwell+time.Minute)
	f.calls = nil

	// Operator presses Start: channel runs again, suppression still set.
	c.Processing = true
	c.State = "CONTROL"
	m.Tick(context.Background())

	if len(f.calls) != 1 || f.calls[0] != "unsuppress:A" {
		t.Fatalf("want unsuppress on operator start, got %v", f.calls)
	}
	// Released — but it is still measurably low, so the same tick rightly
	// begins a FRESH dwell. What matters is it is no longer autoStopped.
	if e := m.entries["A"]; e == nil || e.state != stLowWatch {
		t.Fatalf("want a fresh lowWatch after operator start, got %+v", m.entries["A"])
	}
}

func TestExternalRestartResetsWatch(t *testing.T) {
	f := newFake()
	c := f.add(1, "A", fptr(5))
	m := f.manager()
	f.seedPast(m)
	runDwell(m, f, lowDwell+time.Minute)
	f.calls = nil

	// A config import restarted it and cleared suppression in vce.
	c.Processing = true
	c.State = "CONTROL"
	c.Suppressed = bptr(false)
	m.Tick(context.Background())

	if len(f.calls) != 0 {
		t.Fatalf("no calls expected on an external restart, got %v", f.calls)
	}
	// Dropped to a fresh watch (it is still low), not autoStopped.
	if e := m.entries["A"]; e == nil || e.state != stLowWatch {
		t.Fatalf("want a fresh lowWatch after external restart, got %+v", m.entries["A"])
	}
}

func TestSeedRecoversSuppressedStopped(t *testing.T) {
	f := newFake()
	c := f.add(1, "A", fptr(5))
	c.Processing = false
	c.State = "STOPPED"
	c.Suppressed = bptr(true)
	m := f.manager()
	m.startedAt = f.now.Add(-time.Hour) // past startup quiesce, but NOT seeded

	m.Tick(context.Background())
	e := m.entries["A"]
	if e == nil || e.state != stAutoStopped {
		t.Fatalf("seed did not recover the auto-stopped channel: %+v", m.entries)
	}
	if len(f.calls) != 0 {
		t.Fatalf("seeding must not act, got %v", f.calls)
	}
}

func TestCircuitBreaker(t *testing.T) {
	f := newFake()
	for i := 0; i < maxStopsPerHour+3; i++ {
		f.add(i+1, fmt.Sprintf("C%d", i), fptr(5))
	}
	m := f.manager()
	f.seedPast(m)
	runDwell(m, f, lowDwell+2*time.Minute)

	stops := 0
	for _, call := range f.calls {
		if strings.HasPrefix(call, "stop:") {
			stops++
		}
	}
	if stops != maxStopsPerHour {
		t.Fatalf("breaker allowed %d stops, want %d (%v)", stops, maxStopsPerHour, f.calls)
	}
}
