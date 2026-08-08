package configapply

import (
	"encoding/json"
	"errors"
	"testing"
)

// TestAliasStreamTalkgroupUnmarshal — streamTalkgroupAlias round-trips from
// either a JSON number or a JSON string into the same string form.
func TestAliasStreamTalkgroupUnmarshal(t *testing.T) {
	var n Alias
	if err := json.Unmarshal([]byte(`{"name":"x","streamTalkgroupAlias":1400}`), &n); err != nil {
		t.Fatalf("unmarshal number: %v", err)
	}
	if n.StreamTalkgroupAlias != "1400" {
		t.Errorf("number form: want 1400, got %q", n.StreamTalkgroupAlias)
	}
	var s Alias
	if err := json.Unmarshal([]byte(`{"name":"x","streamTalkgroupAlias":"1401"}`), &s); err != nil {
		t.Fatalf("unmarshal string: %v", err)
	}
	if s.StreamTalkgroupAlias != "1401" {
		t.Errorf("string form: want 1401, got %q", s.StreamTalkgroupAlias)
	}
	var z Alias
	if err := json.Unmarshal([]byte(`{"name":"x"}`), &z); err != nil {
		t.Fatalf("unmarshal absent: %v", err)
	}
	if z.StreamTalkgroupAlias != "" {
		t.Errorf("absent form: want empty, got %q", z.StreamTalkgroupAlias)
	}
}

// A disabled channel (AutoStart=false) must carry autoStart=false into the vce
// import body while keeping its full config (name/frequency/decoder), so it
// decodes correctly the moment it is (re)started.
func TestBuildVceConfigDisabledChannel(t *testing.T) {
	payload := ConfigPayload{
		Channels: []ChannelPlan{
			{Name: "test", Frequency: 142658000, Decoder: "p25p2", System: "NSW PSN", Site: "test", Order: 2, AutoStart: false},
		},
	}
	state := buildVceConfig(payload, nil, "catch all PSN", nil)
	if len(state.Channels) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(state.Channels))
	}
	ch := state.Channels[0]
	if ch.AutoStart {
		t.Errorf("disabled channel must have autoStart=false")
	}
	if ch.Name != "test" || ch.SourceConfiguration.Frequency != 142658000 ||
		ch.DecodeConfiguration.Type != "decodeConfigP25Phase2" {
		t.Errorf("disabled channel lost its config: %+v", ch)
	}
}

// Node OFF (captureEnabled=false) must force EVERY channel autoStart=false in
// the import body, even ones whose config says auto-start on — so sdrtrunk-vce
// decodes nothing while the agent stays connected.
func TestBuildVceConfigCaptureOff(t *testing.T) {
	off := false
	payload := ConfigPayload{
		CaptureEnabled: &off,
		Channels: []ChannelPlan{
			{Name: "on-ch", Frequency: 142658000, Decoder: "p25p2", System: "NSW PSN", Site: "s", Order: 1, AutoStart: true},
		},
	}
	state := buildVceConfig(payload, nil, "catch all PSN", nil)
	for _, ch := range state.Channels {
		if ch.AutoStart {
			t.Errorf("capture off should force autoStart=false on %q", ch.Name)
		}
	}
}

// The preset playlist's alias-list name is extracted from the embedded preset
// (channels + P25 aliases reference it in the import body).
func TestPresetAliasListName(t *testing.T) {
	if got := (Deps{}).presetAliasListName(); got != "catch all PSN" {
		t.Errorf("presetAliasListName = %q, want %q", got, "catch all PSN")
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

func TestEnrichRdioRowIDs(t *testing.T) {
	cur := map[string]any{
		"systems": []any{
			map[string]any{"_id": float64(5), "id": float64(2)},
			map[string]any{"_id": float64(9), "id": float64(7)},
		},
		"apiKeys": []any{
			map[string]any{"_id": float64(3), "key": "abc"},
		},
	}
	cfg := map[string]any{
		"systems": []any{
			map[string]any{"id": float64(2)},            // matches -> _id 5
			map[string]any{"id": float64(99)},           // new -> no _id
		},
		"apiKeys": []any{
			map[string]any{"key": "abc"},                // matches -> _id 3
			map[string]any{"key": "xyz"},                // new -> no _id
		},
	}
	enrichRdioRowIDs(cfg, cur)
	sys := cfg["systems"].([]any)
	if sys[0].(map[string]any)["_id"] != float64(5) {
		t.Errorf("system id=2 should get _id 5, got %v", sys[0].(map[string]any)["_id"])
	}
	if _, has := sys[1].(map[string]any)["_id"]; has {
		t.Errorf("new system id=99 should have no _id")
	}
	if cfg["apiKeys"].([]any)[0].(map[string]any)["_id"] != float64(3) {
		t.Errorf("apiKey abc should get _id 3")
	}
}
