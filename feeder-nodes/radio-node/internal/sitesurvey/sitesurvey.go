// Package sitesurvey measures how well this node can hear a list of candidate
// GRN sites, so the backend can decide which ones belong in its channel list.
//
// The backend picks the candidates (sites in the node's LGA neighbourhood,
// with their control frequencies) and owns the pass threshold; the agent only
// answers "what decode rate does this frequency actually get HERE", which is
// a question only the radio can answer.
//
// How it works, and why:
//
//   - vce has no tune-and-measure primitive. The only way to hear a frequency
//     is to have a channel configured for it, so a survey is bracketed by TWO
//     config imports: the first adds the candidates as stopped test channels
//     alongside the node's real ones, the last puts the real configuration
//     back. Nothing else in the agent uses /config/import for start/stop —
//     this is the deliberate exception, and the restore runs on every exit
//     path including panics and cancellation.
//   - Test channels are named "SURVEY: <site>", carry autoStart=false (so
//     neither vce's self-heal nor the channel manager touches them), and are
//     deliberately deaf: no traffic pool, no control-channel learning, data
//     calls ignored. A survey must not record audio or chase voice calls; it
//     measures the control channel and nothing else.
//   - The node's OWN channels are stopped for the duration. A tuner already
//     sourcing a channel cannot retune to a candidate's frequency, so leaving
//     them up meant the test channels competed for the tuner they needed and
//     every reading was taken on a desensed dongle. They stay configured and
//     come back with the restoring import. Note what this does and does not
//     touch: the feed is left exactly as it was — rdio keeps running, its
//     downstream stays enabled, the relay stays up — and the node simply has
//     nothing to upload while it is decoding nothing. Uploads resume the
//     moment the channels come back.
//   - Candidates are tested in WAVES. Frequencies within one tuner's usable
//     span can be decoded by a single dongle at once, so the candidates are
//     greedy-clustered into groups inside clusterSpanHz (at most
//     maxPerCluster each) and as many clusters run concurrently as the node
//     has tuners. A one-dongle node still works — it just takes longer.
//   - Anything that fails inside a multi-channel wave is retested ALONE once.
//     Starting several channels on one dongle desenses it, and a site that
//     reads 40% in company but 80% by itself is a good site, not a bad one.
//     The better of the two readings wins.
//   - The channel manager is paused for the whole survey (it would see the
//     node's real channels stopped and the test channels flapping, and act on
//     a world that is not real), and the survey aborts the moment its test
//     channels disappear — that means a config push landed, and the measuring
//     it is doing is about channels that no longer exist.
package sitesurvey

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/configapply"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/decodeprobe"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/version"
)

const (
	// channelPrefix marks a survey's test channels. It is how the survey finds
	// its own channels in the live list, and how anyone reading a node's
	// channel list during a survey can tell what they are looking at.
	channelPrefix = "SURVEY: "

	// clusterSpanHz: how far apart two candidates may sit and still share a
	// tuner. A 2 MHz span fits comfortably inside the sample rate the node's
	// dongles run at, with room at the edges where the channelizer rolls off.
	clusterSpanHz = 2_000_000
	// maxPerCluster: concurrent channels per tuner. The whole cost of a survey
	// is one measurement window per wave, so what fits in a wave decides how
	// long the thing takes. With the node's own channels stopped the dongle is
	// doing nothing else, and six P25 control channels inside one 2 MHz span
	// is work it does comfortably — the node runs that many in normal service.
	maxPerCluster = 6
	// maxTuners bounds the concurrent-wave width regardless of how many
	// dongles are plugged in — a survey is not a stress test.
	maxTuners = 4

	// retestBelowPct: a batched reading under this gets one solo retest. Set
	// well above the backend's pass bar so a near miss caused by desense is
	// given its fair chance, not just an outright failure.
	retestBelowPct = 75

	// settleAfterStart: let a freshly started channel be assigned a tuner
	// before the measurement window begins counting.
	settleAfterStart = 2 * time.Second
	// postRetries/postBackoff: the result POST is the whole point of the
	// survey, so it is retried properly rather than fire-and-forget.
	postRetries = 4
	postBackoff = 5 * time.Second
	postTimeout = 30 * time.Second
)

// Candidate is one site the backend wants measured.
type Candidate struct {
	Name   string   `json:"name"`
	GrnKey *string  `json:"grnKey"`
	Mhz    float64  `json:"mhz"`
	AltMhz *float64 `json:"altMhz"`
}

// Request is the surveySites command payload.
type Request struct {
	SurveyID   int64       `json:"surveyId"`
	Candidates []Candidate `json:"candidates"`
}

// Result is one measured frequency, reported raw: the agent applies no
// threshold, so the pass bar can change without an agent release.
type Result struct {
	GrnKey     *string  `json:"grnKey"`
	SiteName   string   `json:"siteName"`
	FreqHz     int64    `json:"freqHz"`
	AltFreqHz  *int64   `json:"altFreqHz"`
	Outcome    string   `json:"outcome"` // measured | noLock | unmeasured
	MedianPct  *float64 `json:"medianPct"`
	Samples    int      `json:"samples"`
	SignalDbfs *float64 `json:"signalDbfs"`
	IsAlt      bool     `json:"isAlt"`
}

// Report is the body POSTed to the backend when a survey ends.
type Report struct {
	SurveyID int64    `json:"surveyId"`
	Aborted  bool     `json:"aborted"`
	Note     *string  `json:"note,omitempty"`
	Results  []Result `json:"results"`
}

// LiveResult is one site's reading as the survey has it so far.
type LiveResult struct {
	Site      string   `json:"site"`
	FreqHz    int64    `json:"freqHz"`
	Outcome   string   `json:"outcome"`
	MedianPct *float64 `json:"medianPct"`
	IsAlt     bool     `json:"isAlt"`
}

// Progress is the survey's live state for the status frame.
//
// It carries the readings taken so far, not just a count. A survey is minutes
// of work and the report it files at the end is the only other time anyone
// would learn what it heard — which makes watching one pointless, and a long
// run impossible to judge until it is over. The readings are small and the
// heartbeat already carries far more than this.
type Progress struct {
	Running  bool   `json:"running"`
	SurveyID int64  `json:"surveyId"`
	Phase    string `json:"phase"` // preparing | testing | retesting | restoring | reporting
	Done     int    `json:"done"`
	Total    int    `json:"total"`
	Current  string `json:"current"`
	// Results so far, in the order the sites were planned. Omitted while
	// there is nothing measured yet.
	Results []LiveResult `json:"results,omitempty"`
}

// Options wires the survey to the node. Everything it touches is injected, so
// the whole state machine runs in tests without a JVM or a backend.
type Options struct {
	// Fetch returns the live configured channels.
	Fetch func() ([]sdrctl.Channel, error)
	// Start/Stop/Suppress act on a channel by its CURRENT id.
	Start, Stop, Suppress func(id int) error
	// Import replaces the node's vce configuration with the real one plus
	// extra channels, optionally silencing the node's own channels so the
	// survey has the tuners to itself. (nil, false) restores the node exactly
	// as it was. The caller holds whatever apply lock the agent uses.
	Import func(extra []configapply.ChannelPlan, silenceOwn bool) error
	// Tuners reports how many SDRs this node has (wave width).
	Tuners func() int
	// Pause suspends/resumes the automatic channel manager.
	Pause func(on bool)
	// Report delivers the finished report to the backend.
	Report func(ctx context.Context, rep Report) error
	// OnProgress publishes survey progress (status sidecar). Optional.
	OnProgress func(p Progress)
	// Now/Sleep are the clock; nil = real time.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration)
}

// Runner executes one survey at a time.
type Runner struct {
	opts  Options
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration)

	mu   sync.Mutex
	prog Progress
}

// New builds a Runner.
func New(opts Options) *Runner {
	r := &Runner{opts: opts, now: opts.Now, sleep: opts.Sleep}
	if r.now == nil {
		r.now = time.Now
	}
	if r.sleep == nil {
		r.sleep = func(ctx context.Context, d time.Duration) {
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
		}
	}
	return r
}

// Snapshot returns the survey's state for the status frame.
func (r *Runner) Snapshot() Progress {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prog
}

func (r *Runner) setProgress(p Progress) {
	r.mu.Lock()
	r.prog = p
	r.mu.Unlock()
	if r.opts.OnProgress != nil {
		r.opts.OnProgress(p)
	}
}

// measurement is one frequency to test: a candidate's primary control channel,
// or its alternate as a separate measurement of the same site.
type measurement struct {
	cand    Candidate
	freqHz  int64
	altHz   *int64 // the site's alt frequency, carried on the primary row
	isAlt   bool
	chanNam string
	res     *decodeprobe.Result
}

// Run executes the survey to completion and reports it. It always restores the
// node's real configuration and always resumes the channel manager, however it
// exits.
func (r *Runner) Run(ctx context.Context, req Request) {
	plan := buildMeasurements(req.Candidates)
	if len(plan) == 0 {
		r.finish(ctx, req.SurveyID, nil, true, "no testable candidates")
		return
	}

	log.Printf("sitesurvey: survey #%d starting — %d frequencies across %d site(s)", req.SurveyID, len(plan), len(req.Candidates))
	r.setProgress(Progress{Running: true, SurveyID: req.SurveyID, Phase: "preparing", Total: len(plan)})

	if r.opts.Pause != nil {
		r.opts.Pause(true)
		defer r.opts.Pause(false)
	}

	// The restore is registered BEFORE the first import: if that import half
	// lands (the control server accepting it and then dying, say), the node
	// must still be put back.
	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		r.setProgress(Progress{
			Running: true, SurveyID: req.SurveyID, Phase: "restoring",
			Total: len(plan), Done: len(plan), Results: liveResults(plan),
		})
		if err := r.opts.Import(nil, false); err != nil {
			// Loud: the node is now running a configuration with survey
			// channels in it until the next config push or restart.
			log.Printf("sitesurvey: RESTORE FAILED after survey #%d: %v — the node still has survey channels configured; push its config to clear them", req.SurveyID, err)
		} else {
			log.Printf("sitesurvey: survey #%d — real configuration restored", req.SurveyID)
		}
	}
	defer restore()

	extra := make([]configapply.ChannelPlan, 0, len(plan))
	for i := range plan {
		extra = append(extra, testChannel(plan[i].chanNam, plan[i].freqHz, i))
	}
	// silenceOwn: the node stops feeding here and starts again at the restore.
	if err := r.opts.Import(extra, true); err != nil {
		log.Printf("sitesurvey: survey #%d could not install its test channels: %v", req.SurveyID, err)
		restore()
		r.finish(ctx, req.SurveyID, nil, true, "could not install test channels: "+err.Error())
		return
	}
	r.silenceOwnChannels(ctx)

	aborted, note := r.measureAll(ctx, req.SurveyID, plan)
	restore()
	r.finish(ctx, req.SurveyID, plan, aborted, note)
}

// silenceOwnChannels stops anything running that is not ours.
//
// The import marks the node's own channels auto-start=false, but an import
// does not reliably stop a channel that is ALREADY running — the same reason
// configapply carries its own enforce-stopped backstop. A channel left running
// holds a tuner the survey needs, so this is not cosmetic: it is the
// difference between measuring a candidate and reporting it unmeasured.
//
// Bounded, and two clean passes end it early: channels stop asynchronously,
// so one look is not enough to know the node is quiet.
func (r *Runner) silenceOwnChannels(ctx context.Context) {
	clean := 0
	for attempt := 0; attempt < 12 && clean < 2 && ctx.Err() == nil; attempt++ {
		chans, err := r.opts.Fetch()
		if err != nil {
			r.sleep(ctx, 500*time.Millisecond)
			continue
		}
		stopped := false
		for _, ch := range chans {
			name := strings.TrimSpace(ch.Name)
			if !ch.Processing || strings.HasPrefix(name, channelPrefix) {
				continue
			}
			stopped = true
			if err := r.opts.Stop(ch.ID); err != nil {
				log.Printf("sitesurvey: could not stop [%s] for the survey: %v", name, err)
			} else {
				log.Printf("sitesurvey: stopped [%s] for the survey (restored when it finishes)", name)
			}
		}
		if stopped {
			clean = 0
		} else {
			clean++
		}
		r.sleep(ctx, 500*time.Millisecond)
	}
}

// liveResults is what has been measured so far, for the status frame.
func liveResults(plan []*measurement) []LiveResult {
	out := make([]LiveResult, 0, len(plan))
	for _, m := range plan {
		if m.res == nil {
			continue
		}
		row := LiveResult{Site: m.cand.Name, FreqHz: m.freqHz, Outcome: string(m.res.Outcome), IsAlt: m.isAlt}
		if m.res.Outcome == decodeprobe.Measured {
			pct := m.res.MedianPct
			row.MedianPct = &pct
		}
		out = append(out, row)
	}
	return out
}

// measureAll runs the waves and fills in each measurement's result. It returns
// aborted=true when the survey could not be completed honestly.
func (r *Runner) measureAll(ctx context.Context, surveyID int64, plan []*measurement) (bool, string) {
	waves := cluster(plan, r.tunerCount())
	done := 0
	for _, wave := range waves {
		if ctx.Err() != nil {
			return true, "cancelled"
		}
		r.setProgress(Progress{
			Running: true, SurveyID: surveyID, Phase: "testing",
			Done: done, Total: len(plan), Current: describe(wave),
			Results: liveResults(plan),
		})
		gone := r.measureBatch(ctx, wave)
		if gone {
			// Every channel in the wave vanished: a config push replaced the
			// configuration mid-survey. Everything measured so far was about a
			// world that still existed; this wave was not.
			return true, "the node's configuration changed during the survey"
		}
		done += len(wave)
		r.setProgress(Progress{
			Running: true, SurveyID: surveyID, Phase: "testing",
			Done: done, Total: len(plan), Results: liveResults(plan),
		})

		// Solo retest for anything that disappointed in company. Only worth it
		// when the wave actually had company to blame.
		if len(wave) > 1 {
			for _, m := range wave {
				if ctx.Err() != nil {
					return true, "cancelled"
				}
				if !needsRetest(m.res) {
					continue
				}
				r.setProgress(Progress{
					Running: true, SurveyID: surveyID, Phase: "retesting",
					Done: done, Total: len(plan), Current: m.cand.Name,
					Results: liveResults(plan),
				})
				before := m.res
				if gone := r.measureBatch(ctx, []*measurement{m}); gone {
					return true, "the node's configuration changed during the survey"
				}
				m.res = better(before, m.res)
			}
		}
	}
	if ctx.Err() != nil {
		return true, "cancelled"
	}
	return false, ""
}

// measureBatch suppresses, starts, measures and stops one wave of test
// channels. It returns true when the channels could not be found at all — the
// survey's configuration is gone.
func (r *Runner) measureBatch(ctx context.Context, batch []*measurement) bool {
	ids := r.resolveIDs(batch)
	if len(ids) == 0 {
		return true
	}
	names := make([]string, 0, len(batch))
	for _, m := range batch {
		id, ok := ids[m.chanNam]
		if !ok {
			continue // this one is gone; the others still measure honestly
		}
		names = append(names, m.chanNam)
		// Suppress before start, matching the channel manager's rule: a
		// channel we are driving by hand must never be subject to the
		// runtime's self-heal sweep in either direction.
		if err := r.opts.Suppress(id); err != nil {
			log.Printf("sitesurvey: suppress %q failed: %v (continuing)", m.chanNam, err)
		}
		if err := r.opts.Start(id); err != nil {
			// Usually "No Tuner Available". Not a verdict about RF — report it
			// as unmeasured rather than as a failed site.
			log.Printf("sitesurvey: start %q failed: %v", m.chanNam, err)
			m.res = &decodeprobe.Result{Outcome: decodeprobe.Unmeasured, MedianPct: -1}
			names = names[:len(names)-1]
		}
	}
	if len(names) == 0 {
		// Nothing started. If the channels exist, that is a tuner problem (the
		// results already say unmeasured); the survey carries on.
		return false
	}
	r.sleep(ctx, settleAfterStart)

	out := decodeprobe.Measure(ctx, names, r.probeDeps())
	for _, m := range batch {
		if res, ok := out[m.chanNam]; ok {
			cp := res
			m.res = &cp
		}
	}

	// Stop them again by their CURRENT ids: an import between start and now
	// would have renumbered everything.
	stopIDs := r.resolveIDs(batch)
	allGone := len(stopIDs) == 0
	for _, m := range batch {
		if id, ok := stopIDs[m.chanNam]; ok {
			if err := r.opts.Stop(id); err != nil {
				log.Printf("sitesurvey: stop %q failed: %v", m.chanNam, err)
			}
		}
	}
	return allGone
}

// resolveIDs maps each batch channel's name to its current vce id.
func (r *Runner) resolveIDs(batch []*measurement) map[string]int {
	out := map[string]int{}
	chans, err := r.opts.Fetch()
	if err != nil {
		return out
	}
	want := make(map[string]bool, len(batch))
	for _, m := range batch {
		want[m.chanNam] = true
	}
	for _, ch := range chans {
		name := strings.TrimSpace(ch.Name)
		if want[name] {
			out[name] = ch.ID
		}
	}
	return out
}

func (r *Runner) probeDeps() decodeprobe.Deps {
	return decodeprobe.Deps{
		Now:   r.now,
		Sleep: r.sleep,
		Fetch: func() (map[string]decodeprobe.Snapshot, error) {
			chans, err := r.opts.Fetch()
			if err != nil {
				return nil, err
			}
			out := make(map[string]decodeprobe.Snapshot, len(chans))
			for _, ch := range chans {
				out[strings.TrimSpace(ch.Name)] = decodeprobe.Snapshot{
					Control: ch.Control, State: ch.State,
					SyncPercent: ch.SyncPercent, SignalDbfs: ch.SignalDbfs,
					DecodingForMs: ch.DecodingForMs, SyncFrames: ch.SyncFrames,
				}
			}
			return out, nil
		},
	}
}

// finish reports the survey. A report is sent even for an aborted run: the
// measurements that DID happen are the audit trail, and the backend decides
// what an aborted survey is allowed to change (nothing).
func (r *Runner) finish(ctx context.Context, surveyID int64, plan []*measurement, aborted bool, note string) {
	r.setProgress(Progress{
		Running: true, SurveyID: surveyID, Phase: "reporting",
		Total: len(plan), Done: len(plan), Results: liveResults(plan),
	})
	rep := Report{SurveyID: surveyID, Aborted: aborted, Results: make([]Result, 0, len(plan))}
	if note != "" {
		n := note
		rep.Note = &n
	}
	measured, locked := 0, 0
	for _, m := range plan {
		row := Result{
			GrnKey: m.cand.GrnKey, SiteName: m.cand.Name, FreqHz: m.freqHz,
			AltFreqHz: m.altHz, IsAlt: m.isAlt, Outcome: string(decodeprobe.Unmeasured),
		}
		if m.res != nil {
			row.Outcome = string(m.res.Outcome)
			row.Samples = m.res.Samples
			row.SignalDbfs = m.res.SignalDbfs
			if m.res.Outcome == decodeprobe.Measured {
				pct := m.res.MedianPct
				row.MedianPct = &pct
				measured++
			}
			if m.res.Outcome != decodeprobe.NoLock {
				locked++
			}
		}
		rep.Results = append(rep.Results, row)
	}
	log.Printf("sitesurvey: survey #%d finished — %d/%d measured, %d locked, aborted=%t", surveyID, measured, len(plan), locked, aborted)

	// Report with retries. Use a context that outlives a cancelled survey: a
	// cancelled run's partial results are exactly what staff asked to see.
	rctx := ctx
	if rctx.Err() != nil {
		var cancel context.CancelFunc
		rctx, cancel = context.WithTimeout(context.Background(), postTimeout*postRetries)
		defer cancel()
	}
	var err error
	for attempt := 0; attempt < postRetries; attempt++ {
		if attempt > 0 {
			r.sleep(rctx, postBackoff)
		}
		if err = r.opts.Report(rctx, rep); err == nil {
			r.setProgress(Progress{})
			return
		}
		log.Printf("sitesurvey: report attempt %d for survey #%d failed: %v", attempt+1, surveyID, err)
	}
	log.Printf("sitesurvey: survey #%d results could not be delivered: %v", surveyID, err)
	r.setProgress(Progress{})
}

func (r *Runner) tunerCount() int {
	n := 1
	if r.opts.Tuners != nil {
		n = r.opts.Tuners()
	}
	if n < 1 {
		n = 1
	}
	if n > maxTuners {
		n = maxTuners
	}
	return n
}

// ---------------------------------------------------------------------------
// planning
// ---------------------------------------------------------------------------

// buildMeasurements expands candidates into the frequencies to test: every
// site's primary control channel, plus its alternate as a separate
// measurement (the backend folds a passing alternate back into its site).
func buildMeasurements(cands []Candidate) []*measurement {
	var out []*measurement
	seen := map[int64]bool{}
	for _, c := range cands {
		name := strings.TrimSpace(c.Name)
		if name == "" {
			continue
		}
		primary := hz(c.Mhz)
		if primary <= 0 || seen[primary] {
			continue
		}
		seen[primary] = true
		var altHz *int64
		if c.AltMhz != nil {
			if a := hz(*c.AltMhz); a > 0 && a != primary {
				altHz = &a
			}
		}
		out = append(out, &measurement{
			cand: c, freqHz: primary, altHz: altHz,
			chanNam: channelPrefix + name,
		})
		if altHz != nil && !seen[*altHz] {
			seen[*altHz] = true
			out = append(out, &measurement{
				cand: c, freqHz: *altHz, isAlt: true,
				chanNam: channelPrefix + name + " (alt)",
			})
		}
	}
	return out
}

// hz converts a MHz figure to whole Hz, rounding rather than truncating: these
// come from a human-maintained dataset in MHz with up to six decimals, and
// float truncation would put a channel 1 Hz off its control frequency.
func hz(mhz float64) int64 {
	if mhz <= 0 || math.IsNaN(mhz) || math.IsInf(mhz, 0) {
		return 0
	}
	return int64(math.Round(mhz * 1e6))
}

// testChannel renders one candidate as a deaf, stopped P25 control-channel
// decoder: no traffic pool (never follow a voice call), no control-channel
// learning (a survey tests the frequency it was asked about, not whatever
// alternates the site advertises), data calls ignored. No preferredTuner —
// tuner allocation is sdrtrunk's job, always.
func testChannel(name string, freqHz int64, order int) configapply.ChannelPlan {
	pool, no := 0, false
	return configapply.ChannelPlan{
		Name:      name,
		Frequency: freqHz,
		Decoder:   "p25p1",
		System:    "SURVEY",
		Site:      name,
		AutoStart: false,
		Order:     1000 + order,
		DecoderConfig: &configapply.DecoderConfig{
			TrafficPoolSize:      &pool,
			LearnControlChannels: &no,
			IgnoreDataCalls:      boolPtr(true),
		},
		AutoManage: boolPtr(false),
	}
}

func boolPtr(b bool) *bool { return &b }

// cluster groups the measurements into waves: each wave holds up to `tuners`
// clusters, and each cluster holds up to maxPerCluster frequencies spanning no
// more than clusterSpanHz (what one dongle can cover at once).
func cluster(plan []*measurement, tuners int) [][]*measurement {
	sorted := append([]*measurement(nil), plan...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].freqHz < sorted[j].freqHz })

	var clusters [][]*measurement
	var cur []*measurement
	for _, m := range sorted {
		if len(cur) > 0 && (len(cur) >= maxPerCluster || m.freqHz-cur[0].freqHz > clusterSpanHz) {
			clusters = append(clusters, cur)
			cur = nil
		}
		cur = append(cur, m)
	}
	if len(cur) > 0 {
		clusters = append(clusters, cur)
	}

	var waves [][]*measurement
	for i := 0; i < len(clusters); i += tuners {
		end := i + tuners
		if end > len(clusters) {
			end = len(clusters)
		}
		var wave []*measurement
		for _, c := range clusters[i:end] {
			wave = append(wave, c...)
		}
		waves = append(waves, wave)
	}
	return waves
}

// needsRetest reports whether a batched result deserves a solo second look.
func needsRetest(res *decodeprobe.Result) bool {
	if res == nil {
		return true
	}
	switch res.Outcome {
	case decodeprobe.Measured:
		return res.MedianPct < retestBelowPct
	case decodeprobe.NoLock, decodeprobe.Unmeasured:
		return true
	}
	return false
}

// better picks the more favourable of a batched and a solo reading. Desense
// only ever depresses a reading, so the higher one is the honest one.
func better(a, b *decodeprobe.Result) *decodeprobe.Result {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	}
	score := func(r *decodeprobe.Result) float64 {
		if r.Outcome == decodeprobe.Measured {
			return r.MedianPct
		}
		if r.Outcome == decodeprobe.Unmeasured {
			return -1 // locked but unmeasurable beats never locking
		}
		return -2
	}
	if score(b) > score(a) {
		return b
	}
	return a
}

func describe(batch []*measurement) string {
	names := make([]string, 0, len(batch))
	for _, m := range batch {
		names = append(names, m.cand.Name)
	}
	return strings.Join(names, ", ")
}

// ---------------------------------------------------------------------------
// reporting transport
// ---------------------------------------------------------------------------

// errAuth marks a backend 401/403 — futile to retry.
var errAuth = errors.New("backend rejected node auth")

// Poster builds a Report function that POSTs to the backend's node-ingest
// survey endpoint with the standard node headers.
func Poster(serverURL, nodeToken, installID string) func(context.Context, Report) error {
	hc := &http.Client{Timeout: postTimeout}
	return func(ctx context.Context, rep Report) error {
		body, err := json.Marshal(rep)
		if err != nil {
			return fmt.Errorf("marshal report: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			serverURL+"/api/node-ingest/site-survey", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Node-Token", nodeToken)
		req.Header.Set("X-Node-Install", installID)
		req.Header.Set("User-Agent", version.UserAgent())
		resp, err := hc.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}()
		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			return nil
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			return fmt.Errorf("%w (status %d)", errAuth, resp.StatusCode)
		default:
			return fmt.Errorf("backend returned %d", resp.StatusCode)
		}
	}
}
