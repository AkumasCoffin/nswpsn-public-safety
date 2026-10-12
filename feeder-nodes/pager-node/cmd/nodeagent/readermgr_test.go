package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/pagersdr"
	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/wsclient"
)

// testManager builds a readerManager over a temp data dir with one pretend
// dongle and no hardware behind it. Every config it is given has capture off,
// so the reader scripts are written and registered but never launched.
func testManager(t *testing.T) *readerManager {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	dir := t.TempDir()
	m := &readerManager{
		rootCtx:    ctx,
		dataDir:    dir,
		readersDir: filepath.Join(dir, "readers"),
		relayURL:   "http://127.0.0.1:17390/pager",
		audioURL:   "http://127.0.0.1:17390/audio",
	}
	m.devices = []pagersdr.Device{{Index: 0, Serial: "00000001"}}
	return m
}

// cfgAt is a pushed config tuning the one reader to mhz.
func cfgAt(version string, mhz float64) wsclient.PagerConfig {
	return wsclient.PagerConfig{
		ConfigVersion: version,
		FeedEnabled:   true,
		Frequencies:   []wsclient.PagerFrequency{{Label: "NSWRFS", MHz: mhz}},
	}
}

// runningFreq reads back the frequency the rendered reader script tunes to.
func runningFreq(t *testing.T, m *readerManager) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(m.readersDir, "reader-NSWRFS.sh"))
	if err != nil {
		t.Fatalf("read reader script: %v", err)
	}
	for _, field := range strings.Fields(string(raw)) {
		if strings.HasSuffix(field, "M") && strings.HasPrefix(field, "148.") {
			return field
		}
	}
	t.Fatalf("no frequency in the rendered script:\n%s", raw)
	return ""
}

// A config push that lands DURING a rescan must survive it.
//
// Rescan used to snapshot lastCfg, drop the lock, and spend ~35-65s on
// rtl_test, EEPROM work and ppm measurement before re-applying that copy. A
// push arriving in that window was applied, persisted and acked by the WS
// client — and then overwritten by the stale replay, so the node ran the old
// frequencies while reporting the new configVersion. The backend saw it in sync
// and never pushed again.
func TestRescanAppliesTheConfigThatLandedDuringIt(t *testing.T) {
	m := testManager(t)

	if err := m.Apply(cfgAt("v1", 148.5875)); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	if got := runningFreq(t, m); got != "148.5875M" {
		t.Fatalf("want the v1 frequency, got %s", got)
	}

	// A rescan starts: readers down, dongles held by rtl_test.
	m.mu.Lock()
	m.rescanning = true
	m.mu.Unlock()

	// Staff save a new tuning mid-scan. It must be recorded but NOT started —
	// rtl_fm against a dongle rtl_test has open fails with "device busy" and
	// spoils the measurement in progress.
	if err := m.Apply(cfgAt("v2", 148.9875)); err != nil {
		t.Fatalf("apply during rescan: %v", err)
	}
	if got := runningFreq(t, m); got != "148.5875M" {
		t.Fatalf("a rebuild ran during the rescan (script now %s)", got)
	}

	// The rescan finishes and re-applies. It must land on v2, not replay v1.
	m.mu.Lock()
	m.rescanning = false
	err := m.applyLocked()
	m.mu.Unlock()
	if err != nil {
		t.Fatalf("post-rescan apply: %v", err)
	}
	if got := runningFreq(t, m); got != "148.9875M" {
		t.Fatalf("the rescan replayed the stale config (script tunes %s)", got)
	}
	if m.lastCfg.ConfigVersion != "v2" {
		t.Fatalf("want the node running v2, got %q", m.lastCfg.ConfigVersion)
	}
}

// The audio paths take the same route, for the same reason: they change the tap
// and nothing else, so they must rebuild from the CURRENT config rather than
// from a copy they took before the rebuild.
func TestAudioTapKeepsTheCurrentConfig(t *testing.T) {
	m := testManager(t)
	if err := m.Apply(cfgAt("v1", 148.5875)); err != nil {
		t.Fatalf("initial apply: %v", err)
	}
	if err := m.Apply(cfgAt("v2", 148.9875)); err != nil {
		t.Fatalf("second apply: %v", err)
	}

	if err := m.AudioStart("reader:NSWRFS"); err != nil {
		t.Fatalf("audio start: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(m.readersDir, "reader-NSWRFS.sh"))
	if err != nil {
		t.Fatalf("read reader script: %v", err)
	}
	script := string(raw)
	if !strings.Contains(script, m.audioURL) {
		t.Fatalf("the audio tap was not rendered:\n%s", script)
	}
	if !strings.Contains(script, "148.9875M") {
		t.Fatalf("starting the tap reverted the config:\n%s", script)
	}

	if err := m.AudioStop(); err != nil {
		t.Fatalf("audio stop: %v", err)
	}
	raw, err = os.ReadFile(filepath.Join(m.readersDir, "reader-NSWRFS.sh"))
	if err != nil {
		t.Fatalf("read reader script: %v", err)
	}
	if script = string(raw); strings.Contains(script, m.audioURL) {
		t.Fatalf("the audio tap survived the stop:\n%s", script)
	}
	if !strings.Contains(script, "148.9875M") {
		t.Fatalf("stopping the tap reverted the config:\n%s", script)
	}
}
