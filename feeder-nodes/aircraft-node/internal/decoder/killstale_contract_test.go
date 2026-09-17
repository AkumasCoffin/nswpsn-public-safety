package decoder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The startup reap depends on this file's output, so the dependency is pinned
// here rather than left as a comment in another package.
//
// An agent that re-execs for a self-update keeps its PID, so its children are
// not torn down and an orphaned decoder keeps the dongle open — the
// replacement then crash-loops on "usb_claim_interface error -6" forever.
// supervise.KillStale reaps the orphan by matching its /proc cmdline.
//
// What it can match is the catch. The script ends in `exec` so that the
// supervised process IS the decoder, which means bash replaces itself and the
// script's own path leaves the cmdline the instant it starts. The JSON
// directory is the part that survives, because the decoder is still being told
// to write there.
func TestScriptExecsSoTheDecoderIsTheSupervisedProcess(t *testing.T) {
	dir := t.TempDir()
	path, err := Write(dir, Params{Bin: "dump1090-fa", JSONDir: "/run/nswpsn-adsb"})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	body := string(b)
	if !strings.Contains(body, "exec ") {
		t.Fatal("the launcher no longer execs; signals would stop reaching the decoder")
	}
}

func TestJsonDirSurvivesIntoTheDecoderCommandLine(t *testing.T) {
	// If this ever stops being true, KillStale silently matches nothing and
	// every self-update strands the receiver with a dead decoder.
	const jsonDir = "/run/nswpsn-adsb"
	dir := t.TempDir()
	path, err := Write(dir, Params{Bin: "dump1090-fa", JSONDir: jsonDir})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	b, _ := os.ReadFile(path)
	line := ""
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "exec ") {
			line = l
			break
		}
	}
	if line == "" {
		t.Fatal("no exec line in the generated script")
	}
	if !strings.Contains(line, jsonDir) {
		t.Fatalf("the exec'd command does not carry the JSON dir, so an orphan\n"+
			"cannot be matched at startup:\n  %s", line)
	}
	// And the script path itself is NOT in it — the reason matching on the
	// decoder directory alone was never going to work.
	if strings.Contains(line, filepath.Base(path)) {
		t.Fatal("script path is still in the exec line; the match rationale has changed")
	}
}
