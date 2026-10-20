package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/pagersdr"
	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/wsclient"
)

const testExe = "/opt/nswpsn/nodeagent-pager"

// nswPair is the NSW plan as the backend pushes it, RFS first.
func nswPair(version string) wsclient.PagerConfig {
	return wsclient.PagerConfig{
		ConfigVersion: version,
		FeedEnabled:   true,
		Frequencies: []wsclient.PagerFrequency{
			{Label: "NSWRFS", MHz: 148.5875},
			{Label: "FRNSW", MHz: 148.9625},
		},
	}
}

func readScript(t *testing.T, m *readerManager, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(m.readersDir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(raw)
}

func scriptNames(t *testing.T, m *readerManager) []string {
	t.Helper()
	entries, err := os.ReadDir(m.readersDir)
	if err != nil {
		t.Fatalf("read readers dir: %v", err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// One dongle now receives both NSW frequencies at once, instead of only the
// primary.
func TestOneDongleRunsBothFrequencies(t *testing.T) {
	m := testManager(t)
	m.exe = testExe
	if err := m.Apply(nswPair("v1")); err != nil {
		t.Fatalf("apply: %v", err)
	}

	names := scriptNames(t, m)
	if len(names) != 1 || names[0] != "reader-NSWRFS+FRNSW.sh" {
		t.Fatalf("want one shared reader script, got %v", names)
	}
	script := readScript(t, m, "reader-NSWRFS+FRNSW.sh")
	for _, want := range []string{
		"rtl_sdr -d 0 ",
		"'" + testExe + "' channelize 1058400 ",
		"148587500 >(",
		"148962500 >(",
		"post 'NSWRFS' '148.5875'",
		"post 'FRNSW' '148.9625'",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("shared reader missing %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "rtl_fm -d") {
		t.Fatalf("a shared dongle must not run rtl_fm:\n%s", script)
	}
	if strings.Contains(script, m.audioURL) {
		t.Fatalf("no audio tap was asked for:\n%s", script)
	}

	// The staff drawer still sees one reader per frequency.
	m.mu.Lock()
	compOf := m.compOf
	m.mu.Unlock()
	if compOf["NSWRFS"] != "reader:NSWRFS+FRNSW" || compOf["FRNSW"] != "reader:NSWRFS+FRNSW" {
		t.Fatalf("both labels should map to the shared component, got %v", compOf)
	}
	st := m.Status()
	if len(st) != 2 || st["reader:NSWRFS"] == "" || st["reader:FRNSW"] == "" {
		t.Fatalf("want status for exactly NSWRFS and FRNSW, got %v", st)
	}
}

// A node with a dongle per frequency keeps the plain rtl_fm reader on each,
// exactly as before.
func TestTwoDonglesKeepOneReaderEach(t *testing.T) {
	m := testManager(t)
	m.exe = testExe
	m.devices = []pagersdr.Device{{Index: 0, Serial: "00000001"}, {Index: 1, Serial: "00000002"}}
	if err := m.Apply(nswPair("v1")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if rfs := readScript(t, m, "reader-NSWRFS.sh"); !strings.Contains(rfs, "rtl_fm -d 0 ") || !strings.Contains(rfs, "148.5875M") {
		t.Fatalf("RFS should stay on dongle 0 with rtl_fm:\n%s", rfs)
	}
	if fr := readScript(t, m, "reader-FRNSW.sh"); !strings.Contains(fr, "rtl_fm -d 1 ") || !strings.Contains(fr, "148.9625M") {
		t.Fatalf("FRNSW should stay on dongle 1 with rtl_fm:\n%s", fr)
	}
	if names := scriptNames(t, m); len(names) != 2 {
		t.Fatalf("want two reader scripts, got %v", names)
	}
}

// Listen on a shared dongle taps that one frequency, under its own label.
func TestListenOnASharedDongle(t *testing.T) {
	m := testManager(t)
	m.exe = testExe
	if err := m.Apply(nswPair("v1")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := m.AudioStart("reader:FRNSW"); err != nil {
		t.Fatalf("audio start: %v", err)
	}
	script := readScript(t, m, "reader-NSWRFS+FRNSW.sh")
	if strings.Count(script, m.audioURL) != 1 {
		t.Fatalf("want exactly one audio tap:\n%s", script)
	}
	if !strings.Contains(script, "-H 'X-Pager-Source: FRNSW' 2>/dev/null || true) | multimon-ng") {
		t.Fatalf("the tap should be on FRNSW's stream:\n%s", script)
	}
	if err := m.AudioStop(); err != nil {
		t.Fatalf("audio stop: %v", err)
	}
	if script = readScript(t, m, "reader-NSWRFS+FRNSW.sh"); strings.Contains(script, m.audioURL) {
		t.Fatalf("the tap survived the stop:\n%s", script)
	}
}

// Restarting one frequency on a shared dongle restarts that dongle's reader,
// not an unknown component.
func TestRestartFindsTheSharedReader(t *testing.T) {
	m := testManager(t)
	m.exe = testExe
	if err := m.Apply(nswPair("v1")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Capture is off in these configs, so the component exists but is
	// disabled: that error proves the label resolved to the shared component.
	err := m.Restart("reader:FRNSW")
	if err == nil || !strings.Contains(err.Error(), `"reader:NSWRFS+FRNSW" is disabled`) {
		t.Fatalf("want the shared component resolved, got %v", err)
	}
}

// Losing a dongle (a rescan finds one fewer) folds both frequencies onto the
// one left, and the old per-frequency scripts are cleared away.
func TestLosingADongleSharesTheOneLeft(t *testing.T) {
	m := testManager(t)
	m.exe = testExe
	m.devices = []pagersdr.Device{{Index: 0, Serial: "00000001"}, {Index: 1, Serial: "00000002"}}
	if err := m.Apply(nswPair("v1")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	m.mu.Lock()
	m.devices = m.devices[:1]
	err := m.applyLocked()
	m.mu.Unlock()
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if names := scriptNames(t, m); len(names) != 1 || names[0] != "reader-NSWRFS+FRNSW.sh" {
		t.Fatalf("want only the shared script left, got %v", names)
	}
}

// Without its own path the agent cannot run channelize, so it falls back to
// the old behaviour: the priority frequency only.
func TestNoAgentPathFallsBackToOneFrequency(t *testing.T) {
	m := testManager(t)
	m.exe = ""
	if err := m.Apply(nswPair("v1")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if names := scriptNames(t, m); len(names) != 1 || names[0] != "reader-NSWRFS.sh" {
		t.Fatalf("want only the RFS rtl_fm reader, got %v", names)
	}
	if st := m.Status(); len(st) != 1 || st["reader:NSWRFS"] == "" {
		t.Fatalf("want only RFS reported, got %v", st)
	}
}
