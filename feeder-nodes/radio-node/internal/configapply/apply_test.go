package configapply

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// A disabled channel (AutoStart=false) must render enabled="false" and land on
// disk via the pre-launch WritePlaylistOnly path (no SDR-Trunk / rdio needed).
// This is the authoritative playlist the agent writes BEFORE launching SDR-Trunk.
func TestWritePlaylistOnlyDisabledChannel(t *testing.T) {
	dir := t.TempDir()
	deps := Deps{DataDir: dir, SDRTrunkAppRoot: dir}
	payload := ConfigPayload{
		Channels: []ChannelPlan{
			{Name: "test", Frequency: 142658000, Decoder: "p25p2", System: "NSW PSN", Site: "test", Order: 2, AutoStart: false},
		},
	}
	if err := WritePlaylistOnly(payload, deps); err != nil {
		t.Fatalf("WritePlaylistOnly: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "playlist", "default.xml"))
	if err != nil {
		t.Fatalf("read playlist: %v", err)
	}
	out := string(b)
	// The <channel> tag for "test" must be enabled="false".
	chTag := regexp.MustCompile(`<channel\b[^>]*>`).FindString(out)
	if chTag == "" {
		t.Fatalf("no <channel> rendered:\n%s", out)
	}
	if !strings.Contains(chTag, `name="test"`) {
		t.Errorf("channel name not rendered: %s", chTag)
	}
	if !strings.Contains(chTag, `enabled="false"`) {
		t.Errorf("disabled channel not rendered enabled=\"false\": %s", chTag)
	}
}

// Node OFF (captureEnabled=false) must render EVERY channel enabled="false",
// even ones whose config says auto-start on — so SDR-Trunk decodes nothing while
// the agent stays connected.
func TestWritePlaylistCaptureOff(t *testing.T) {
	dir := t.TempDir()
	deps := Deps{DataDir: dir, SDRTrunkAppRoot: dir}
	off := false
	payload := ConfigPayload{
		CaptureEnabled: &off,
		Channels: []ChannelPlan{
			{Name: "on-ch", Frequency: 142658000, Decoder: "p25p2", System: "NSW PSN", Site: "s", Order: 1, AutoStart: true},
		},
	}
	if err := WritePlaylistOnly(payload, deps); err != nil {
		t.Fatalf("WritePlaylistOnly: %v", err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "playlist", "default.xml"))
	chTag := regexp.MustCompile(`<channel\b[^>]*>`).FindString(string(b))
	if !strings.Contains(chTag, `enabled="false"`) {
		t.Errorf("capture off should force enabled=\"false\": %s", chTag)
	}
}

// Feed off → the rdio downstream is written disabled:true; on → disabled:false.
func TestApplyRdioKeysDownstreamFeed(t *testing.T) {
	mk := func() map[string]any {
		return map[string]any{
			"apiKeys": []any{
				map[string]any{"_id": 1, "systems": []any{map[string]any{"id": 1}}},
			},
		}
	}
	keys := map[int]string{1: "k1"}
	for _, feed := range []bool{true, false} {
		cfg := mk()
		if err := applyRdioKeys(cfg, keys, feed); err != nil {
			t.Fatalf("applyRdioKeys feed=%v: %v", feed, err)
		}
		ds := cfg["downstreams"].([]any)[0].(map[string]any)
		if ds["disabled"] != !feed {
			t.Errorf("feed=%v → downstream disabled=%v, want %v", feed, ds["disabled"], !feed)
		}
	}
}

// HasStage must see through an errors.Join so the caller can tell "playlist
// succeeded, rdio failed" (persist the payload) from a playlist failure.
func TestHasStage(t *testing.T) {
	joined := errors.Join(nil, stageErr("rdio", "put config", errors.New("timeout")))
	if !HasStage(joined, "rdio") {
		t.Error("expected the rdio stage to be detected in the joined error")
	}
	if HasStage(joined, "playlist") {
		t.Error("did not expect a playlist stage (only rdio failed)")
	}
	if HasStage(nil, "rdio") {
		t.Error("nil error has no stage")
	}
}
