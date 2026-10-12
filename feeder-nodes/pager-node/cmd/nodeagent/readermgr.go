package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/agentcfg"
	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/pagersdr"
	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/reader"
	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/supervise"
	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/wsclient"
)

// maxReaders caps the number of concurrent readers. The locked frequency plan is
// at most two (NSW RFS + Fire & Rescue NSW), so extra dongles beyond two are
// idle spares.
const maxReaders = 2

// defaultPagerFrequencies is the locked frequency plan used until a configPush
// supplies one, in priority order (1 SDR -> first only; 2+ -> first two).
var defaultPagerFrequencies = []wsclient.PagerFrequency{
	{Label: "NSWRFS", MHz: 148.5875}, // NSW Rural Fire Service
	{Label: "FRNSW", MHz: 148.9875},  // Fire & Rescue NSW
}

// defaultPagerProtocols is the locked POCSAG demodulator set.
var defaultPagerProtocols = []string{"POCSAG512", "POCSAG1200", "POCSAG2400"}

// defaultGain is the fixed rtl_fm tuner gain (dB). A high fixed gain (near the
// R820T max) matches the operator's proven Pagermon reader; rtl_fm clamps to the
// nearest supported value per dongle.
const defaultGain = "49.6"

// readerSwapSettle is how long rebuild waits after killing the old readers
// before starting the new ones, so the just-killed rtl_fm releases its USB
// dongle first. Without it a live reconfigure (e.g. switching the single-SDR
// frequency) starts the new rtl_fm while the old one still holds the device,
// which fails with "device busy" and crash-loops — so the switch never takes.
// Matches the settle the Rescan path already uses.
const readerSwapSettle = 1500 * time.Millisecond

// maxPlausiblePPM bounds an accepted rtl_test -p reading: a dongle needing more
// correction than this is faulty, and writing an absurd ppm onto rtl_fm's
// command line would be worse than not correcting at all.
//
// It is only a backstop. The thing that used to make a large reading dangerous
// was that it could be NOISE — a bogus 94 ppm at 148.5875 MHz is ~14 kHz of
// detune, which walks the POCSAG carrier out of rtl_fm's passband — and that is
// now caught where it belongs, by pagersdr.MeasurePPM refusing a reading that
// never settles. A settled 94 means the dongle really is 94 ppm out, and
// applying it is the correction, so the bound stays loose on purpose rather
// than throwing away a large but real one. Same number the aircraft agent's
// sdrppm.MaxPlausible uses, for the same reason.
const maxPlausiblePPM = 100

// readerManager owns the pager reader components. It detects the attached SDRs
// once, then (re)computes the reader set from a frequency plan whenever a config
// is applied: it writes each reader.sh (pinned to a dongle by serial), and runs
// them under a supervisor. captureEnabled=false stops the readers (they are
// registered disabled) while keeping the agent connected.
//
// Because the supervisor's component set is fixed at construction, a config
// change rebuilds it: the current supervisor's sub-context is cancelled (killing
// its readers) and a fresh supervisor is started under a new sub-context derived
// from the agent's root context.
//
// It implements wsclient.ConfigApplier (Apply + Restart) and supplies the
// heartbeat's component states via Status.
type readerManager struct {
	rootCtx    context.Context
	dataDir    string
	readersDir string
	relayURL   string
	audioURL   string

	rescanMu  sync.Mutex // single-flights Rescan (SDR detection can't run twice at once)
	mu        sync.Mutex
	devices   []pagersdr.Device
	sup       *supervise.Supervisor
	supCancel context.CancelFunc
	// rescanning: a rescan currently has the dongles open for rtl_test /
	// rtl_eeprom / ppm. Guarded by mu, and distinct from rescanMu: that one
	// stops two rescans overlapping, this one tells applyLocked to record
	// intent instead of starting rtl_fm against a busy device.
	rescanning bool
	lastCfg    wsclient.PagerConfig // the config the readers are built from
	audioLabel string               // reader label currently tapped for browser audio ("" = none)
}

// newReaderManager detects + de-duplicates the attached SDR serials (best-effort;
// a detection failure leaves the manager with no devices, so it simply runs no
// readers until hardware/serials are sorted out) and prepares the reader script
// directory. relayAddr is the loopback relay host:port the readers POST to.
func newReaderManager(rootCtx context.Context, dataDir, relayAddr string) *readerManager {
	m := &readerManager{
		rootCtx:    rootCtx,
		dataDir:    dataDir,
		readersDir: filepath.Join(dataDir, "readers"),
		relayURL:   "http://" + relayAddr + "/pager",
		audioURL:   "http://" + relayAddr + "/audio",
		// Sensible default until the first configPush / Apply, so a Rescan before
		// any config still (re)starts readers rather than leaving them disabled.
		lastCfg: wsclient.PagerConfig{CaptureEnabled: true, FeedEnabled: true},
	}
	m.devices = detectDevices()
	return m
}

// detectDevices enumerates the SDRs and ensures distinct serials, logging and
// returning an empty list on failure rather than aborting the agent.
func detectDevices() []pagersdr.Device {
	devs, err := pagersdr.Detect()
	if err != nil {
		log.Printf("readers: SDR detection failed (%v); no readers will run until it succeeds", err)
		return nil
	}
	log.Printf("readers: detected %d SDR device(s)", len(devs))

	devs, err = pagersdr.EnsureDistinctSerials(devs)
	if err != nil {
		log.Printf("readers: ensuring distinct SDR serials failed (%v); proceeding with detected serials", err)
	}
	// Measure each dongle's sample-clock error (ppm) BEFORE any reader claims the
	// device, so rtl_fm can correct for it. Done fresh every startup.
	measurePPMs(devs)
	for _, d := range devs {
		log.Printf("readers: SDR index=%d serial=%q ppm=%d", d.Index, d.Serial, d.PPM)
	}
	return devs
}

// validFrequencies drops entries whose MHz is NaN/Inf or outside the RTL-SDR
// tunable range, so a bad/hostile pushed frequency can't produce a broken or
// injectable rtl_fm -f arg.
func validFrequencies(freqs []wsclient.PagerFrequency) []wsclient.PagerFrequency {
	out := make([]wsclient.PagerFrequency, 0, len(freqs))
	for _, f := range freqs {
		if math.IsNaN(f.MHz) || math.IsInf(f.MHz, 0) || f.MHz < 24 || f.MHz > 1766 {
			log.Printf("readers: dropping out-of-range frequency %v MHz (label %q)", f.MHz, f.Label)
			continue
		}
		out = append(out, f)
	}
	return out
}

// measurePPMs fills each device's PPM via rtl_test -p. Done SEQUENTIALLY (not in
// parallel): two rtl_test processes opening dongles at the same instant can race
// on USB and one fails to claim its device. A failure leaves ppm=0.
func measurePPMs(devs []pagersdr.Device) {
	if len(devs) == 0 {
		return
	}
	log.Printf("readers: measuring SDR ppm (rtl_test -p, ~%s per dongle, sequential)…", pagersdr.PPMMeasureDur)
	for i := range devs {
		ppm, err := pagersdr.MeasurePPM(devs[i].Index, pagersdr.PPMMeasureDur)
		if err != nil {
			log.Printf("readers: ppm measure failed for SDR index=%d (%v); using ppm=0", devs[i].Index, err)
			continue
		}
		if ppm < -maxPlausiblePPM || ppm > maxPlausiblePPM {
			log.Printf("readers: SDR index=%d measured ppm=%d is implausible (>%d, unreliable clock); using ppm=0", devs[i].Index, ppm, maxPlausiblePPM)
			continue
		}
		devs[i].PPM = ppm
		log.Printf("readers: SDR index=%d measured ppm=%d", devs[i].Index, ppm)
	}
}

// Apply records cfg as the config the readers should be built from, and builds
// them. feedEnabled requires no agent action (the backend feed-gates the forward
// and still counts reception) — it is only logged. captureEnabled drives whether
// the readers run.
func (m *readerManager) Apply(cfg wsclient.PagerConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastCfg = cfg
	return m.applyLocked()
}

// applyCurrent rebuilds the readers from whatever config is current, for the
// paths that change something OTHER than the config — a rescan's new device
// set, the audio tap going on or off.
//
// It deliberately does not take a config argument. Those paths used to snapshot
// m.lastCfg, drop the lock, do their slow work, and then call Apply with the
// copy — and a config push that landed in that window was applied, persisted
// and acked, then overwritten by the stale replay. The node ran the old
// frequencies, gain and ppm while reporting the new configVersion, so the
// backend saw it in sync and never pushed again.
func (m *readerManager) applyCurrent() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applyLocked()
}

// applyLocked renders the reader set from m.lastCfg (and m.audioLabel) and
// restarts the supervisor on it. Caller holds m.mu.
//
// While a rescan is in flight it records intent and does nothing else: the
// rescan has the dongles open for rtl_test, rtl_eeprom and half a minute of ppm
// measurement, so starting rtl_fm now would fail with "device busy" AND spoil
// the measurement it is in the middle of taking. Nothing is lost — the rescan
// finishes by calling this again, with whatever m.lastCfg says by then.
func (m *readerManager) applyLocked() error {
	if m.rescanning {
		log.Printf("readers: SDR rescan in progress; reader rebuild deferred until it finishes")
		return nil
	}
	cfg := m.lastCfg

	// Drop any out-of-range/garbage frequencies from the push before they reach
	// rtl_fm, falling back to the defaults if nothing valid remains.
	freqs := validFrequencies(cfg.Frequencies)
	if len(freqs) == 0 {
		freqs = defaultPagerFrequencies
	}
	protocols := cfg.Protocols
	if len(protocols) == 0 {
		protocols = defaultPagerProtocols
	}

	// Tuner gain: a config override (a number, or "auto" for AGC) wins over the
	// agent default; reader.Write validates/normalizes it before it reaches rtl_fm.
	gain := defaultGain
	if strings.TrimSpace(cfg.Gain) != "" {
		gain = cfg.Gain
		log.Printf("readers: gain override in effect: %q", gain)
	}
	if cfg.Ppm != nil {
		log.Printf("readers: ppm override in effect: %d (applied to all readers)", *cfg.Ppm)
	}

	// Reader count: one per available dongle, capped by the frequency plan and the
	// two-frequency ceiling. 1 SDR -> first frequency only; 2+ -> first two.
	n := min(len(m.devices), min(len(freqs), maxReaders))

	if !cfg.CaptureEnabled {
		log.Printf("readers: capture disabled; %d reader(s) will be registered but stopped", n)
	}
	if !cfg.FeedEnabled {
		log.Printf("readers: feed disabled by config (backend feed-gates the forward); readers still capture")
	}

	comps := make(map[string]agentcfg.ComponentCfg, n)
	for i := 0; i < n; i++ {
		label := sanitizeLabel(freqs[i].Label, i)
		dev := m.devices[i]
		ppm := dev.PPM
		if cfg.Ppm != nil {
			ppm = *cfg.Ppm
		}
		// Tap this reader's audio to the loopback relay only while staff are
		// monitoring THIS label (browser "Listen"); others get no tee (no overhead).
		audioURL := ""
		if m.audioLabel != "" && m.audioLabel == label {
			audioURL = m.audioURL
		}
		scriptPath, err := reader.Write(m.readersDir, label, freqs[i].MHz, dev.Index, dev.Serial, ppm, gain, protocols, m.relayURL, audioURL)
		if err != nil {
			return fmt.Errorf("write reader script for %s: %w", label, err)
		}
		log.Printf("readers: %s -> %.4f MHz on SDR index=%d serial=%q ppm=%d gain=%q audio=%t (%s)", label, freqs[i].MHz, dev.Index, dev.Serial, ppm, gain, audioURL != "", scriptPath)
		comps["reader:"+label] = agentcfg.ComponentCfg{
			Enabled: cfg.CaptureEnabled,
			Command: "bash",
			Args:    []string{scriptPath},
		}
	}

	m.rebuild(comps)
	return nil
}

// rebuild swaps in a fresh supervisor for comps: it cancels the current
// supervisor's sub-context (killing its readers) and starts a new supervisor
// under a new sub-context. Caller must hold m.mu.
func (m *readerManager) rebuild(comps map[string]agentcfg.ComponentCfg) {
	if m.supCancel != nil {
		m.supCancel()
		// Let the killed reader's rtl_fm release the SDR before the replacement
		// opens it (else "device busy" -> crash-loop -> the switch never takes).
		// Held under m.mu: a status call may stall briefly, fine for a reconfigure.
		time.Sleep(readerSwapSettle)
	}
	subCtx, cancel := context.WithCancel(m.rootCtx)
	sup := supervise.New(m.dataDir, comps)
	sup.Start(subCtx)
	m.sup = sup
	m.supCancel = cancel
}

// Rescan stops the running readers, frees the dongles, re-detects the attached
// SDRs (and re-measures ppm / re-assigns colliding serials), then replays the
// last applied config so readers restart against the new device set. Used by the
// staff "Recheck SDRs" button, e.g. to pick up a dongle that wasn't enumerated at
// startup (USB still settling after a re-exec).
func (m *readerManager) Rescan() error {
	// Single-flight: concurrent rescans (staff double-click, or one overlapping a
	// config push) would run two rtl_test enumerations against the same dongles at
	// once and collide on USB. Skip if one is already running.
	if !m.rescanMu.TryLock() {
		log.Printf("readers: rescan already in progress; ignoring duplicate request")
		return nil
	}
	defer m.rescanMu.Unlock()

	// Stop current readers first so rtl_test can open the dongles for detection,
	// and mark the scan in flight so a config push arriving mid-scan does not
	// start rtl_fm on top of it.
	m.mu.Lock()
	m.rescanning = true
	if m.supCancel != nil {
		m.supCancel()
		m.supCancel = nil
		m.sup = nil
	}
	m.mu.Unlock()

	log.Printf("readers: rescan requested — stopped readers, re-detecting SDRs")
	// Let the killed readers release their USB devices before rtl_test opens them.
	time.Sleep(1500 * time.Millisecond)

	devs := detectDevices()
	log.Printf("readers: rescan detected %d SDR device(s); re-applying readers", len(devs))

	// Clear the flag and rebuild under ONE hold of the lock, so nothing can
	// slip between the two and rebuild twice. applyLocked reads m.lastCfg
	// itself, so a config push that landed during the scan is what gets
	// applied here rather than being reverted by a stale replay.
	m.mu.Lock()
	defer m.mu.Unlock()
	m.devices = devs
	m.rescanning = false
	return m.applyLocked()
}

// AudioStart taps the named reader's rtl_fm audio for the browser monitor by
// rebuilding that reader with a tee to the loopback relay's /audio endpoint. The
// label matches a frequency label (e.g. "NSWRFS"); a "reader:" prefix from the
// staff UI is tolerated. Triggers a brief reader restart (the USB-settle applies).
func (m *readerManager) AudioStart(label string) error {
	label = strings.TrimPrefix(strings.TrimSpace(label), "reader:")
	m.mu.Lock()
	m.audioLabel = label
	m.mu.Unlock()
	log.Printf("readers: audio monitor start for %q", label)
	return m.applyCurrent()
}

// AudioStop clears the audio tap and rebuilds the readers without the tee.
func (m *readerManager) AudioStop() error {
	m.mu.Lock()
	had := m.audioLabel
	m.audioLabel = ""
	m.mu.Unlock()
	if had == "" {
		return nil // nothing was tapping; avoid a needless reader restart
	}
	log.Printf("readers: audio monitor stop (was %q)", had)
	return m.applyCurrent()
}

// Restart restarts a single named reader component (e.g. "reader:NSWRFS").
func (m *readerManager) Restart(component string) error {
	m.mu.Lock()
	sup := m.sup
	m.mu.Unlock()
	if sup == nil {
		return fmt.Errorf("no readers running")
	}
	return sup.Restart(component)
}

// SupportedGains returns the SDR's supported tuner-gain steps (dB) for the staff
// gain dropdown, or nil if not detected. The dongles are the same model, so the
// first device's list represents the node.
func (m *readerManager) SupportedGains() []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.devices) > 0 {
		return m.devices[0].Gains
	}
	return nil
}

// Status returns the reader component states normalized to "running"/"stopped"
// for the heartbeat.
func (m *readerManager) Status() map[string]string {
	m.mu.Lock()
	sup := m.sup
	m.mu.Unlock()
	out := map[string]string{}
	if sup == nil {
		return out
	}
	for name, st := range sup.Status() {
		if st == supervise.StatusRunning {
			out[name] = "running"
		} else {
			out[name] = "stopped"
		}
	}
	return out
}

// sanitizeLabel makes a frequency label safe for a filename/component name,
// falling back to a positional label when empty.
func sanitizeLabel(label string, idx int) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return fmt.Sprintf("PAGER%d", idx+1)
	}
	// Replace path/shell-unfriendly characters with underscores.
	repl := func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}
	return strings.Map(repl, label)
}
