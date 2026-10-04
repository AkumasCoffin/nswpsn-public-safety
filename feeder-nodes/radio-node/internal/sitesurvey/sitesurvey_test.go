package sitesurvey

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/configapply"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
)

// world is the whole node: a clock that jumps instantly on Sleep, the live
// channel list the survey drives, and a recorder of everything it did.
type world struct {
	mu    sync.Mutex
	now   time.Time
	chans map[string]*sdrctl.Channel
	order []string
	calls []string
	// imports records each Import call's extra-channel names ("restore" for
	// the real-config import).
	imports  [][]string
	silenced []bool
	paused   []bool
	reports  []Report
	tuners   int
	importErr error
	// decode decides what a started channel reports, per fetch.
	decode func(w *world, name string, ch *sdrctl.Channel)
	// onFetch runs before every live read (used to make channels vanish).
	onFetch func(w *world)
	reportErr error
}

func newWorld() *world {
	return &world{
		now:    time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC),
		chans:  map[string]*sdrctl.Channel{},
		tuners: 1,
	}
}

func fptr(v float64) *float64 { return &v }
func bptr(b bool) *bool       { return &b }

func (w *world) record(f string, a ...any) {
	w.calls = append(w.calls, fmt.Sprintf(f, a...))
}

func (w *world) byID(id int) *sdrctl.Channel {
	for _, c := range w.chans {
		if c.ID == id {
			return c
		}
	}
	return nil
}

func (w *world) runner() *Runner {
	return New(Options{
		Fetch: func() ([]sdrctl.Channel, error) {
			if w.onFetch != nil {
				w.onFetch(w)
			}
			out := make([]sdrctl.Channel, 0, len(w.order))
			for _, n := range w.order {
				c := w.chans[n]
				if c == nil {
					continue
				}
				if c.Processing && w.decode != nil {
					w.decode(w, n, c)
				}
				out = append(out, *c)
			}
			return out, nil
		},
		Start: func(id int) error {
			c := w.byID(id)
			if c == nil {
				return fmt.Errorf("no channel %d", id)
			}
			w.record("start:%s", c.Name)
			c.Processing = true
			return nil
		},
		Stop: func(id int) error {
			c := w.byID(id)
			if c == nil {
				return fmt.Errorf("no channel %d", id)
			}
			w.record("stop:%s", c.Name)
			c.Processing = false
			c.State = "IDLE"
			c.Control = false
			c.SyncPercent = nil
			return nil
		},
		Suppress: func(id int) error {
			c := w.byID(id)
			if c == nil {
				return fmt.Errorf("no channel %d", id)
			}
			w.record("suppress:%s", c.Name)
			c.Suppressed = bptr(true)
			return nil
		},
		Import: func(extra []configapply.ChannelPlan, silenceOwn bool) error {
			if w.importErr != nil {
				return w.importErr
			}
			w.silenced = append(w.silenced, silenceOwn)
			// An import sets auto-start, it does not stop what is already
			// running — the node needs its own backstop for that, which is
			// what makes this worth modelling.
			for _, c := range w.chans {
				if !strings.HasPrefix(c.Name, channelPrefix) {
					c.AutoStart = bptr(!silenceOwn)
				}
			}
			names := make([]string, 0, len(extra))
			// An import replaces the channel set: drop the old survey channels
			// and install the new ones, exactly like vce's full overwrite.
			for n := range w.chans {
				if strings.HasPrefix(n, channelPrefix) {
					delete(w.chans, n)
				}
			}
			w.order = w.order[:0]
			for n := range w.chans {
				w.order = append(w.order, n)
			}
			for i, ch := range extra {
				names = append(names, ch.Name)
				w.chans[ch.Name] = &sdrctl.Channel{
					ID: 500 + i, Name: ch.Name, Type: "STANDARD", State: "IDLE",
					AutoStart: bptr(false), Suppressed: bptr(false),
				}
				w.order = append(w.order, ch.Name)
			}
			if len(names) == 0 {
				names = []string{"restore"}
			}
			w.imports = append(w.imports, names)
			return nil
		},
		Tuners: func() int { return w.tuners },
		Pause:  func(on bool) { w.paused = append(w.paused, on) },
		Report: func(_ context.Context, rep Report) error {
			if w.reportErr != nil {
				return w.reportErr
			}
			w.reports = append(w.reports, rep)
			return nil
		},
		Now: func() time.Time { return w.now },
		Sleep: func(ctx context.Context, d time.Duration) {
			w.now = w.now.Add(d)
		},
	})
}

// decodesAt makes every started survey channel lock and report pct, except the
// names in dead (which never lock).
func decodesAt(pct map[string]float64, def float64) func(*world, string, *sdrctl.Channel) {
	return func(_ *world, name string, c *sdrctl.Channel) {
		v, ok := pct[name]
		if !ok {
			v = def
		}
		if v < 0 {
			return // never locks: stays IDLE with no sync
		}
		c.State = "CONTROL"
		c.Control = true
		c.SyncPercent = fptr(v)
		c.SignalDbfs = fptr(-50)
	}
}

func cand(name string, mhz float64) Candidate { return Candidate{Name: name, Mhz: mhz} }

func lastReport(t *testing.T, w *world) Report {
	t.Helper()
	if len(w.reports) != 1 {
		t.Fatalf("want exactly one report, got %d", len(w.reports))
	}
	return w.reports[0]
}

func byName(rep Report) map[string]Result {
	out := map[string]Result{}
	for _, r := range rep.Results {
		key := r.SiteName
		if r.IsAlt {
			key += " (alt)"
		}
		out[key] = r
	}
	return out
}

func TestSurveyMeasuresAndRestores(t *testing.T) {
	w := newWorld()
	w.decode = decodesAt(map[string]float64{
		channelPrefix + "Strong": 88,
		channelPrefix + "Weak":   20,
		channelPrefix + "Dead":   -1,
	}, 50)
	r := w.runner()
	r.Run(context.Background(), Request{SurveyID: 7, Candidates: []Candidate{
		cand("Strong", 422.375), cand("Weak", 422.5), cand("Dead", 423.0),
	}})

	rep := lastReport(t, w)
	if rep.SurveyID != 7 || rep.Aborted {
		t.Fatalf("bad report envelope: %+v", rep)
	}
	got := byName(rep)
	if g := got["Strong"]; g.Outcome != "measured" || g.MedianPct == nil || *g.MedianPct != 88 {
		t.Fatalf("Strong: %+v", g)
	}
	if g := got["Weak"]; g.Outcome != "measured" || *g.MedianPct != 20 {
		t.Fatalf("Weak: %+v", g)
	}
	if g := got["Dead"]; g.Outcome != "noLock" || g.MedianPct != nil {
		t.Fatalf("Dead: %+v", g)
	}
	if g := got["Strong"]; g.SignalDbfs == nil || *g.SignalDbfs != -50 {
		t.Fatalf("signal not reported: %+v", g)
	}

	// The real configuration always goes back, and the manager is paused for
	// exactly the span of the survey.
	if len(w.imports) != 2 || len(w.imports[0]) != 3 || w.imports[1][0] != "restore" {
		t.Fatalf("imports: %v", w.imports)
	}
	if len(w.paused) != 2 || !w.paused[0] || w.paused[1] {
		t.Fatalf("pause/resume: %v", w.paused)
	}
	// Every started channel is stopped again.
	starts, stops := 0, 0
	for _, c := range w.calls {
		switch {
		case strings.HasPrefix(c, "start:"):
			starts++
		case strings.HasPrefix(c, "stop:"):
			stops++
		}
	}
	if starts == 0 || starts != stops {
		t.Fatalf("started %d, stopped %d (%v)", starts, stops, w.calls)
	}
}

func TestSurveySuppressesBeforeStarting(t *testing.T) {
	w := newWorld()
	w.decode = decodesAt(nil, 80)
	r := w.runner()
	r.Run(context.Background(), Request{SurveyID: 1, Candidates: []Candidate{cand("A", 420)}})

	si, st := -1, -1
	for i, c := range w.calls {
		if c == "suppress:"+channelPrefix+"A" && si < 0 {
			si = i
		}
		if c == "start:"+channelPrefix+"A" && st < 0 {
			st = i
		}
	}
	if si < 0 || st < 0 || si > st {
		t.Fatalf("suppress must precede start: %v", w.calls)
	}
}

func TestAltControlChannelIsItsOwnMeasurement(t *testing.T) {
	w := newWorld()
	alt := 421.0
	w.decode = decodesAt(map[string]float64{
		channelPrefix + "Site":       70,
		channelPrefix + "Site (alt)": 30,
	}, 0)
	r := w.runner()
	r.Run(context.Background(), Request{SurveyID: 2, Candidates: []Candidate{
		{Name: "Site", Mhz: 420.0, AltMhz: &alt},
	}})

	got := byName(lastReport(t, w))
	primary, ok := got["Site"]
	if !ok || primary.IsAlt || primary.FreqHz != 420_000_000 {
		t.Fatalf("primary row: %+v", primary)
	}
	if primary.AltFreqHz == nil || *primary.AltFreqHz != 421_000_000 {
		t.Fatalf("primary must carry the alt frequency: %+v", primary)
	}
	a, ok := got["Site (alt)"]
	if !ok || !a.IsAlt || a.FreqHz != 421_000_000 || *a.MedianPct != 30 {
		t.Fatalf("alt row: %+v", a)
	}
}

func TestDesensedCandidateIsRetestedAlone(t *testing.T) {
	// Three close frequencies share one dongle. "Shy" reads badly in company
	// and well alone — the solo reading is the honest one.
	w := newWorld()
	solo := false
	w.decode = func(_ *world, name string, c *sdrctl.Channel) {
		c.State = "CONTROL"
		c.Control = true
		pct := 90.0
		if name == channelPrefix+"Shy" {
			if solo {
				pct = 85
			} else {
				pct = 30
			}
		}
		c.SyncPercent = fptr(pct)
	}
	w.onFetch = func(w *world) {
		// Solo once only the Shy channel is processing.
		running := 0
		shy := false
		for _, c := range w.chans {
			if c.Processing {
				running++
				if c.Name == channelPrefix+"Shy" {
					shy = true
				}
			}
		}
		solo = running == 1 && shy
	}
	r := w.runner()
	r.Run(context.Background(), Request{SurveyID: 3, Candidates: []Candidate{
		cand("A", 420.0), cand("Shy", 420.5), cand("B", 421.0),
	}})

	got := byName(lastReport(t, w))
	if g := got["Shy"]; g.Outcome != "measured" || *g.MedianPct != 85 {
		t.Fatalf("the solo retest should win: %+v", g)
	}
	// Exactly one retest: Shy started twice, the healthy pair once each.
	starts := map[string]int{}
	for _, c := range w.calls {
		if n, ok := strings.CutPrefix(c, "start:"); ok {
			starts[n]++
		}
	}
	if starts[channelPrefix+"Shy"] != 2 {
		t.Fatalf("want one retest of Shy, got %d starts (%v)", starts[channelPrefix+"Shy"], w.calls)
	}
	if starts[channelPrefix+"A"] != 1 || starts[channelPrefix+"B"] != 1 {
		t.Fatalf("healthy channels must not be retested: %v", starts)
	}
}

func TestSoloWaveIsNeverRetested(t *testing.T) {
	// A one-tuner node tests one cluster at a time, but a cluster of ONE has
	// no company to blame — a bad reading there is just a bad site.
	w := newWorld()
	w.decode = decodesAt(nil, 10)
	r := w.runner()
	r.Run(context.Background(), Request{SurveyID: 4, Candidates: []Candidate{cand("Lonely", 400)}})

	starts := 0
	for _, c := range w.calls {
		if strings.HasPrefix(c, "start:") {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("want a single test, got %d (%v)", starts, w.calls)
	}
}

func TestConfigPushMidSurveyAborts(t *testing.T) {
	// A config push replaces the channel set: the survey's channels vanish.
	// Measuring channels that no longer exist is not evidence of anything, so
	// the run aborts and says so.
	w := newWorld()
	w.decode = decodesAt(nil, 90)
	fetches := 0
	w.onFetch = func(w *world) {
		fetches++
		if fetches == 6 {
			for n := range w.chans {
				if strings.HasPrefix(n, channelPrefix) {
					delete(w.chans, n)
				}
			}
			w.order = w.order[:0]
		}
	}
	r := w.runner()
	r.Run(context.Background(), Request{SurveyID: 5, Candidates: []Candidate{
		cand("A", 420), cand("B", 430), cand("C", 440),
	}})

	rep := lastReport(t, w)
	if !rep.Aborted {
		t.Fatalf("a vanished channel set must abort the survey: %+v", rep)
	}
	if rep.Note == nil || !strings.Contains(*rep.Note, "configuration changed") {
		t.Fatalf("abort note should say why: %+v", rep.Note)
	}
	// Partial measurements are still reported — they are the audit trail.
	if len(rep.Results) != 3 {
		t.Fatalf("every candidate gets a row: %+v", rep.Results)
	}
	if last := w.imports[len(w.imports)-1]; last[0] != "restore" {
		t.Fatalf("the real config must be restored even on abort: %v", w.imports)
	}
}

func TestCancelRestoresAndReportsPartial(t *testing.T) {
	w := newWorld()
	w.decode = decodesAt(nil, 90)
	ctx, cancel := context.WithCancel(context.Background())
	fetches := 0
	w.onFetch = func(_ *world) {
		fetches++
		if fetches == 4 {
			cancel()
		}
	}
	r := w.runner()
	r.Run(ctx, Request{SurveyID: 6, Candidates: []Candidate{
		cand("A", 420), cand("B", 430), cand("C", 440), cand("D", 450),
	}})
	cancel()

	rep := lastReport(t, w)
	if !rep.Aborted || rep.Note == nil || *rep.Note != "cancelled" {
		t.Fatalf("want a cancelled report, got %+v", rep)
	}
	if last := w.imports[len(w.imports)-1]; last[0] != "restore" {
		t.Fatalf("cancel must still restore: %v", w.imports)
	}
	if len(w.paused) != 2 || w.paused[1] {
		t.Fatalf("the channel manager must be resumed: %v", w.paused)
	}
}

func TestFailedInstallImportReportsAndRestores(t *testing.T) {
	w := newWorld()
	w.importErr = fmt.Errorf("control server unreachable")
	r := w.runner()
	r.Run(context.Background(), Request{SurveyID: 8, Candidates: []Candidate{cand("A", 420)}})

	rep := lastReport(t, w)
	if !rep.Aborted || rep.Note == nil || !strings.Contains(*rep.Note, "could not install") {
		t.Fatalf("want an install-failure report: %+v", rep)
	}
	if len(w.paused) != 2 || w.paused[1] {
		t.Fatalf("the manager must be resumed even here: %v", w.paused)
	}
}

func TestWaveWidthFollowsTunerCount(t *testing.T) {
	// Six frequencies, each 3 MHz apart, so every one is its own cluster.
	plan := buildMeasurements([]Candidate{
		cand("A", 400), cand("B", 403), cand("C", 406),
		cand("D", 409), cand("E", 412), cand("F", 415),
	})
	if got := len(cluster(plan, 1)); got != 6 {
		t.Fatalf("one tuner: want 6 waves, got %d", got)
	}
	if got := len(cluster(plan, 3)); got != 2 {
		t.Fatalf("three tuners: want 2 waves, got %d", got)
	}
}

func TestClusterPacksCloseFrequencies(t *testing.T) {
	// Frequencies close enough to share a dongle do, up to the per-tuner
	// capacity; one beyond the span never joins however free the tuner is.
	// Written against the constants, because what fits in a wave is exactly
	// what decides how long a survey takes and it is expected to be tuned.
	var close []Candidate
	for i := 0; i <= maxPerCluster; i++ { // one more than fits
		close = append(close, cand(fmt.Sprintf("C%d", i), 400.0+float64(i)*0.1))
	}
	plan := buildMeasurements(append(close, cand("Far", 450.0)))
	waves := cluster(plan, 1)

	if len(waves) != 3 {
		t.Fatalf("want full cluster + spill + distant, got %d clusters", len(waves))
	}
	if len(waves[0]) != maxPerCluster {
		t.Fatalf("first cluster should be full (%d), got %d", maxPerCluster, len(waves[0]))
	}
	if len(waves[1]) != 1 {
		t.Fatalf("the overflow belongs in its own cluster: %d", len(waves[1]))
	}
	if len(waves[2]) != 1 || waves[2][0].cand.Name != "Far" {
		t.Fatalf("a distant frequency must not share a tuner: %+v", waves[2])
	}
	// The span rule, independent of capacity: two sites a whole band apart.
	spread := cluster(buildMeasurements([]Candidate{cand("Low", 400), cand("High", 403)}), 1)
	if len(spread) != 2 {
		t.Fatalf("frequencies %d Hz apart cannot share a tuner", clusterSpanHz)
	}
}

func TestSurveyTakesTheRadioForItself(t *testing.T) {
	// A tuner already sourcing the node's own channels cannot retune to a
	// candidate, so the survey silences them — and because an import does not
	// stop what is already running, it stops them itself.
	w := newWorld()
	own := &sdrctl.Channel{
		ID: 1, Name: "Mount Boyce", Type: "STANDARD", Processing: true,
		State: "CONTROL", Control: true, AutoStart: bptr(true), Suppressed: bptr(false),
	}
	w.chans[own.Name] = own
	w.order = append(w.order, own.Name)
	w.decode = decodesAt(nil, 80)

	r := w.runner()
	r.Run(context.Background(), Request{SurveyID: 20, Candidates: []Candidate{cand("A", 420)}})

	// Silenced going in, restored coming out.
	if len(w.silenced) != 2 || !w.silenced[0] || w.silenced[1] {
		t.Fatalf("want silence-then-restore, got %v", w.silenced)
	}
	if !containsCall(w, "stop:Mount Boyce") {
		t.Fatalf("the node's own channel was left running: %v", w.calls)
	}
	if own.Processing {
		t.Fatal("the node's own channel should be stopped for the survey")
	}
}

func TestSurveyLeavesItsOwnTestChannelsAlone(t *testing.T) {
	// The backstop stops everything that is not the survey's. It must not
	// mistake a test channel for one of the node's.
	w := newWorld()
	w.decode = decodesAt(nil, 90)
	r := w.runner()
	r.Run(context.Background(), Request{SurveyID: 21, Candidates: []Candidate{cand("A", 420)}})

	// One start and one stop for the test channel: the measurement. If the
	// backstop had stopped it too there would be more.
	stops := 0
	for _, c := range w.calls {
		if c == "stop:"+channelPrefix+"A" {
			stops++
		}
	}
	if stops != 1 {
		t.Fatalf("the test channel should be stopped once, by its own measurement: %v", w.calls)
	}
	got := byName(lastReport(t, w))
	if g := got["A"]; g.Outcome != "measured" {
		t.Fatalf("the measurement should still have happened: %+v", g)
	}
}

func TestTestChannelIsDeafAndStopped(t *testing.T) {
	ch := testChannel(channelPrefix+"X", 422_375_000, 0)
	switch {
	case ch.AutoStart:
		t.Fatal("a test channel must never auto-start")
	case ch.SDR != "":
		t.Fatal("the agent never pins a tuner")
	case ch.DecoderConfig == nil:
		t.Fatal("missing decoder config")
	case ch.DecoderConfig.TrafficPoolSize == nil || *ch.DecoderConfig.TrafficPoolSize != 0:
		t.Fatal("a survey must not follow voice calls")
	case ch.DecoderConfig.LearnControlChannels == nil || *ch.DecoderConfig.LearnControlChannels:
		t.Fatal("a survey tests the frequency it was given, not learned alternates")
	case ch.AutoManage == nil || *ch.AutoManage:
		t.Fatal("test channels must be invisible to the channel manager")
	}
}

func TestMeasurementPlanSkipsJunk(t *testing.T) {
	alt := 420.0
	plan := buildMeasurements([]Candidate{
		{Name: "  ", Mhz: 420},             // no name
		{Name: "Zero", Mhz: 0},             // no frequency
		{Name: "Same", Mhz: 420, AltMhz: &alt}, // alt == primary: one row only
		{Name: "Dupe", Mhz: 420},           // same frequency as Same
	})
	if len(plan) != 1 || plan[0].cand.Name != "Same" || plan[0].altHz != nil {
		t.Fatalf("unexpected plan: %+v", plan)
	}
}

// containsCall reports whether the survey made this exact control-server call.
func containsCall(w *world, want string) bool {
	for _, c := range w.calls {
		if c == want {
			return true
		}
	}
	return false
}
