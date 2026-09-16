package main

// aircraftManager owns everything about the receiver: the supervised dump1090
// child, the adaptive gain loop, and the snapshots shipped to the backend.
//
// It is the ADS-B counterpart of the pager agent's readerManager, and plays
// the same three roles — wsclient.ConfigApplier, status provider, and stats
// provider — so the shared WS client needs no knowledge of what kind of node
// it is running on.

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/agentcfg"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/autogain"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/decoder"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/decoderjson"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/supervise"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/wsclient"
)

const (
	// componentName is the supervised child's name. The staff UI keys its
	// restart button on this exact string.
	componentName = "dump1090"

	// decoderSwapSettle is how long to wait after killing the decoder before
	// starting the replacement, so the USB device is released first. Same
	// rationale as the pager agent's reader swap: without it the new process
	// hits "usb_claim_interface error" and crash-loops.
	decoderSwapSettle = 1500 * time.Millisecond
)

type aircraftManager struct {
	rootCtx context.Context
	cfg     *agentcfg.Config
	reader  *decoderjson.Reader

	mu        sync.Mutex
	sup       *supervise.Supervisor
	supCancel context.CancelFunc
	applied   wsclient.AdsbConfig
	hasConfig bool

	// gain is the autogain controller, non-nil only while gain is "auto".
	gain *autogain.Controller

	// statsMu guards the figures the heartbeat reads, which are written by the
	// snapshot loop on a different goroutine.
	statsMu   sync.Mutex
	lastStats wsclient.AdsbStats

	rescanMu sync.Mutex
}

func newAircraftManager(ctx context.Context, cfg *agentcfg.Config) *aircraftManager {
	return &aircraftManager{
		rootCtx: ctx,
		cfg:     cfg,
		reader:  decoderjson.NewReader(cfg.JSONDir),
	}
}

// Apply renders the decoder launch script for cfg and (re)starts it.
func (m *aircraftManager) Apply(cfg wsclient.AdsbConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = cfg
	m.hasConfig = true
	return m.launchLocked()
}

// launchLocked builds the decoder params from the applied config and restarts
// the supervisor. Caller must hold m.mu.
func (m *aircraftManager) launchLocked() error {
	cfg := m.applied

	fixed, auto, ok := decoder.ParseGain(cfg.Gain)
	if !ok {
		// Refuse rather than silently falling back: a typo'd gain that quietly
		// became maximum would be invisible until someone wondered why the
		// receiver was over-driven.
		log.Printf("decoder: ignoring unparseable gain %q; using decoder default", cfg.Gain)
		fixed, auto = nil, false
	}

	var gainDB *float64
	switch {
	case auto:
		// Adaptive: the controller always hands the decoder a concrete value,
		// so from dump1090's point of view gain is fixed and only the agent
		// knows it is being adapted.
		if m.gain == nil {
			m.gain = autogain.New(nil)
		}
		g := m.gain.Gain()
		gainDB = &g
	default:
		// Fixed (or default): tear down any controller so a later switch back
		// to "auto" starts its search fresh rather than from a stale state.
		m.gain = nil
		gainDB = fixed
	}

	scriptPath, err := decoder.Write(m.cfg.DecoderDir(), decoder.Params{
		Bin:     m.cfg.Dump1090Bin,
		JSONDir: m.cfg.JSONDir,
		GainDB:  gainDB,
		PPM:     cfg.Ppm,
		Lat:     cfg.Lat,
		Lon:     cfg.Lon,
	})
	if err != nil {
		return err
	}

	if cfg.Lat == nil || cfg.Lon == nil {
		// Worth saying out loud: without a position the decoder reports no
		// range at all, and the staff Data tab's range cards stay empty. The
		// backend requires a pin at creation, so this means something upstream
		// went wrong rather than an ordinary configuration choice.
		log.Printf("decoder: no antenna position in config — range statistics will be unavailable")
	}

	m.rebuildLocked(map[string]agentcfg.ComponentCfg{
		componentName: {
			Enabled: cfg.CaptureEnabled,
			Command: "bash",
			Args:    []string{scriptPath},
		},
	})
	return nil
}

// rebuildLocked swaps in a fresh supervisor: cancels the current one (killing
// the decoder), waits for the USB device to be released, then starts the new
// one. Caller must hold m.mu.
func (m *aircraftManager) rebuildLocked(comps map[string]agentcfg.ComponentCfg) {
	if m.supCancel != nil {
		m.supCancel()
		time.Sleep(decoderSwapSettle)
	}
	ctx, cancel := context.WithCancel(m.rootCtx)
	sup := supervise.New(m.cfg.DataDir, comps)
	sup.Start(ctx)
	m.sup = sup
	m.supCancel = cancel
}

// Restart restarts the decoder component (staff "Restart" button).
func (m *aircraftManager) Restart(component string) error {
	m.mu.Lock()
	sup := m.sup
	m.mu.Unlock()
	if sup == nil {
		return nil
	}
	return sup.Restart(component)
}

// Rescan stops the decoder, lets the USB device settle, and starts it again —
// the staff "Recheck SDR" action, for a dongle that was replugged. Unlike the
// pager agent there is no ppm measurement pass, so this is quick; it is still
// single-flighted so two clicks can't interleave two teardowns.
func (m *aircraftManager) Rescan() error {
	if !m.rescanMu.TryLock() {
		return nil
	}
	defer m.rescanMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasConfig {
		return nil
	}
	log.Printf("decoder: rescan — restarting decoder")
	return m.launchLocked()
}

// Status reports the decoder component's state for the heartbeat.
//
// Unlike the pager agent, crashlooped and disabled are NOT flattened into
// "stopped": a decoder that keeps dying is a different problem from one that
// was deliberately turned off, and the staff drawer should be able to say so.
func (m *aircraftManager) Status() map[string]string {
	m.mu.Lock()
	sup := m.sup
	m.mu.Unlock()
	if sup == nil {
		return map[string]string{componentName: "stopped"}
	}
	out := map[string]string{}
	for name, st := range sup.Status() {
		out[name] = string(st)
	}
	if _, ok := out[componentName]; !ok {
		out[componentName] = "stopped"
	}
	return out
}

// Stats returns the latest decoder figures for the heartbeat.
func (m *aircraftManager) Stats() wsclient.AdsbStats {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	return m.lastStats
}

func (m *aircraftManager) setStats(s wsclient.AdsbStats) {
	m.statsMu.Lock()
	m.lastStats = s
	m.statsMu.Unlock()
}

// feedEnabled reports whether snapshots should be uploaded.
func (m *aircraftManager) feedEnabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hasConfig && m.applied.FeedEnabled
}

// considerGain feeds one stats window to the autogain controller and restarts
// the decoder if it decided to move. No-op unless gain is "auto".
func (m *aircraftManager) considerGain(w *decoderjson.StatsWindow, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gain == nil {
		return
	}
	_, moved := m.gain.Consider(autogain.Sample{
		Messages:       w.Messages,
		StrongFraction: w.StrongFraction(),
	}, now)
	if !moved {
		return
	}
	// The new gain is read back out of the controller by launchLocked.
	if err := m.launchLocked(); err != nil {
		log.Printf("autogain: restart after gain change failed: %v", err)
	}
}
