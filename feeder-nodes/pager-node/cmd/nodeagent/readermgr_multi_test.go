package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

// One reader file is all a node needs: with a second dongle plugged in, both
// frequencies still go on the first and the spare stays idle.
func TestASecondDongleStaysIdle(t *testing.T) {
	m := testManager(t)
	m.exe = testExe
	m.devices = []pagersdr.Device{{Index: 0, Serial: "00000001"}, {Index: 1, Serial: "00000002"}}
	if err := m.Apply(nswPair("v1")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	names := scriptNames(t, m)
	if len(names) != 1 || names[0] != "reader-NSWRFS+FRNSW.sh" {
		t.Fatalf("want one reader for both frequencies, got %v", names)
	}
	if script := readScript(t, m, names[0]); !strings.Contains(script, "rtl_sdr -d 0 ") || strings.Contains(script, "-d 1 ") {
		t.Fatalf("the reader belongs on dongle 0:\n%s", script)
	}
}

// A frequency the first dongle cannot fit is the only thing that takes a
// second one; lose that dongle and it goes unreceived, its old script gone.
func TestFarFrequencyTakesTheSecondDongle(t *testing.T) {
	m := testManager(t)
	m.exe = testExe
	m.devices = []pagersdr.Device{{Index: 0, Serial: "00000001"}, {Index: 1, Serial: "00000002"}}
	cfg := wsclient.PagerConfig{
		ConfigVersion: "v1",
		FeedEnabled:   true,
		Frequencies: []wsclient.PagerFrequency{
			{Label: "NSWRFS", MHz: 148.5875},
			{Label: "FAR", MHz: 152.0},
			{Label: "FRNSW", MHz: 148.9625},
		},
	}
	if err := m.Apply(cfg); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if names := scriptNames(t, m); len(names) != 2 {
		t.Fatalf("want two readers, got %v", names)
	}
	if shared := readScript(t, m, "reader-NSWRFS+FRNSW.sh"); !strings.Contains(shared, "rtl_sdr -d 0 ") {
		t.Fatalf("RFS and FRNSW belong together on dongle 0:\n%s", shared)
	}
	if far := readScript(t, m, "reader-FAR.sh"); !strings.Contains(far, "rtl_fm -d 1 ") || !strings.Contains(far, "152M") {
		t.Fatalf("152 MHz belongs alone on dongle 1:\n%s", far)
	}

	m.mu.Lock()
	m.devices = m.devices[:1]
	err := m.applyLocked()
	m.mu.Unlock()
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if names := scriptNames(t, m); len(names) != 1 || names[0] != "reader-NSWRFS+FRNSW.sh" {
		t.Fatalf("want only the shared reader left, got %v", names)
	}
	if st := m.Status(); st["reader:FAR"] != "" {
		t.Fatalf("a frequency with no dongle must not be reported as a reader: %v", st)
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

// Without its own path the agent cannot run channelize, so it falls back to
// the old behaviour: one frequency per dongle, priority first.
func TestNoAgentPathFallsBackToOneFrequencyPerDongle(t *testing.T) {
	m := testManager(t)
	m.exe = ""
	if err := m.Apply(nswPair("v1")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if names := scriptNames(t, m); len(names) != 1 || names[0] != "reader-NSWRFS.sh" {
		t.Fatalf("one dongle: want only the RFS rtl_fm reader, got %v", names)
	}
	if st := m.Status(); len(st) != 1 || st["reader:NSWRFS"] == "" {
		t.Fatalf("one dongle: want only RFS reported, got %v", st)
	}

	m.mu.Lock()
	m.devices = append(m.devices, pagersdr.Device{Index: 1, Serial: "00000002"})
	err := m.applyLocked()
	m.mu.Unlock()
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if names := scriptNames(t, m); len(names) != 2 {
		t.Fatalf("two dongles: want an rtl_fm reader each, got %v", names)
	}
}

// stubSDRs swaps the hardware calls for fakes and records what detection did.
type sdrCalls struct {
	ensured  bool
	measured []int
}

func stubSDRs(t *testing.T, devs []pagersdr.Device) *sdrCalls {
	t.Helper()
	calls := &sdrCalls{}
	oldDetect, oldEnsure, oldMeasure := sdrDetect, sdrEnsureDistinct, sdrMeasurePPM
	t.Cleanup(func() { sdrDetect, sdrEnsureDistinct, sdrMeasurePPM = oldDetect, oldEnsure, oldMeasure })
	sdrDetect = func() ([]pagersdr.Device, error) {
		return append([]pagersdr.Device(nil), devs...), nil
	}
	sdrEnsureDistinct = func(d []pagersdr.Device) ([]pagersdr.Device, error) {
		calls.ensured = true
		return d, nil
	}
	sdrMeasurePPM = func(index int, _ time.Duration) (int, error) {
		calls.measured = append(calls.measured, index)
		return 7, nil
	}
	return calls
}

// Two dongles with the factory serial, and a plan that fits on one: the spare
// is neither measured (a minute of deafness per boot) nor has its EEPROM
// rewritten (a permanent change it does not need).
func TestDetectionLeavesTheSpareAlone(t *testing.T) {
	calls := stubSDRs(t, []pagersdr.Device{{Index: 0, Serial: "00000001"}, {Index: 1, Serial: "00000001"}})
	m := testManager(t)
	m.exe = testExe
	m.lastCfg = nswPair("v1")

	devs := detectDevices(m.donglesNeeded)
	if calls.ensured {
		t.Fatal("serials were reprogrammed for a plan that uses one dongle")
	}
	if len(calls.measured) != 1 || calls.measured[0] != 0 {
		t.Fatalf("want only dongle 0 measured, got %v", calls.measured)
	}
	if !devs[0].PPMMeasured || devs[0].PPM != 7 || devs[1].PPMMeasured {
		t.Fatalf("want dongle 0 measured (ppm 7) and dongle 1 untouched, got %+v", devs)
	}
}

// A plan that needs both dongles prepares both, as before.
func TestDetectionPreparesEveryDongleInUse(t *testing.T) {
	calls := stubSDRs(t, []pagersdr.Device{{Index: 0, Serial: "00000001"}, {Index: 1, Serial: "00000001"}})
	m := testManager(t)
	m.exe = testExe
	m.lastCfg = wsclient.PagerConfig{Frequencies: []wsclient.PagerFrequency{
		{Label: "NSWRFS", MHz: 148.5875},
		{Label: "FAR", MHz: 152.0},
	}}

	detectDevices(m.donglesNeeded)
	if !calls.ensured {
		t.Fatal("two dongles in use need distinct serials")
	}
	if len(calls.measured) != 2 {
		t.Fatalf("want both dongles measured, got %v", calls.measured)
	}
}
