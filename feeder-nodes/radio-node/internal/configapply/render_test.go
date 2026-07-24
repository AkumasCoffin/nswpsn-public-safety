package configapply

import (
	"strings"
	"testing"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/presets"
)

// TestRenderPlaylistMultiChannel verifies the preset's single <channel> block is
// cloned once per configured channel with the right per-channel fields, and that
// streams are pointed at the local rdio.
func TestRenderPlaylistMultiChannel(t *testing.T) {
	keys := map[int]string{1: "key-one", 2: "key-two"}
	channels := []ChannelPlan{
		{Name: "Metro CC", Frequency: 142658000, Decoder: "p25p1", System: "NSW PSN", Site: "Site 1", Order: 1},
		{Name: "Regional CC", Frequency: 419587500, Decoder: "p25p2", System: "NSW PSN", Site: "Site 2", Order: 2},
	}

	out, err := renderPlaylist(presets.DefaultPlaylistXML, channels, keys)
	if err != nil {
		t.Fatalf("renderPlaylist: %v", err)
	}
	s := string(out)

	blocks := reChannelBlock.FindAllString(s, -1)
	if len(blocks) != 2 {
		t.Fatalf("expected 2 channel blocks, got %d\n%s", len(blocks), s)
	}
	for i, blk := range blocks {
		if !strings.Contains(blk, `enabled="true"`) {
			t.Errorf("channel block %d not enabled: %s", i, blk)
		}
	}
	for _, want := range []string{
		`frequency="142658000"`,
		`frequency="419587500"`,
		`name="Metro CC"`,
		`name="Regional CC"`,
		`site="Site 1"`,
		`site="Site 2"`,
		"decodeConfigP25Phase1",
		"decodeConfigP25Phase2",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("rendered playlist missing %q", want)
		}
	}
	// Streams must be re-homed to the local rdio + carry the local keys.
	if !strings.Contains(s, `host="`+localRdioUploadURL+`"`) {
		t.Errorf("streams not pointed at local rdio")
	}
	if !strings.Contains(s, `api_key="key-one"`) {
		t.Errorf("stream api_key not injected")
	}
}

// TestRenderPlaylistNoChannels keeps a valid (disabled) channel so SDR-Trunk
// still loads the playlist when nothing is configured.
func TestRenderPlaylistNoChannels(t *testing.T) {
	out, err := renderPlaylist(presets.DefaultPlaylistXML, nil, map[int]string{})
	if err != nil {
		t.Fatalf("renderPlaylist: %v", err)
	}
	s := string(out)
	blocks := reChannelBlock.FindAllString(s, -1)
	if len(blocks) != 1 {
		t.Fatalf("expected the single template channel, got %d", len(blocks))
	}
	if !strings.Contains(blocks[0], `enabled="false"`) {
		t.Errorf("no-channels playlist should keep the channel disabled: %s", blocks[0])
	}
}
