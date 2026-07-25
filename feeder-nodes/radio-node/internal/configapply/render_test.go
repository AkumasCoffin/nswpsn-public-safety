package configapply

import (
	"strings"
	"testing"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/presets"
)

func ptrBool(b bool) *bool { return &b }
func ptrInt(i int) *int    { return &i }

// TestRenderDecodeP25P1 — modulation CQPSK + full traffic pool + ignore data calls.
func TestRenderDecodeP25P1(t *testing.T) {
	got := renderDecodeConfig("p25p1", &DecoderConfig{
		Modulation:      "CQPSK",
		IgnoreDataCalls: ptrBool(true),
		TrafficPoolSize: ptrInt(50),
	})
	want := `<decode_configuration type="decodeConfigP25Phase1" modulation="CQPSK" ignore_data_calls="true" traffic_channel_pool_size="50"/>`
	if got != want {
		t.Errorf("p25p1 mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestRenderDecodeP25P1Defaults — omitted fields fall back to SDR-Trunk defaults.
func TestRenderDecodeP25P1Defaults(t *testing.T) {
	got := renderDecodeConfig("p25p1", nil)
	want := `<decode_configuration type="decodeConfigP25Phase1" modulation="C4FM" ignore_data_calls="false" traffic_channel_pool_size="20"/>`
	if got != want {
		t.Errorf("p25p1 defaults mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestRenderDecodeP25P2Scramble — manual scramble emits the child + auto-detect off.
func TestRenderDecodeP25P2Scramble(t *testing.T) {
	got := renderDecodeConfig("p25p2", &DecoderConfig{
		Scramble: &Scramble{Wacn: 12345, System: 291, Nac: 659},
	})
	want := `<decode_configuration type="decodeConfigP25Phase2" auto_detect_scramble_parameters="false" ignore_data_calls="false" traffic_channel_pool_size="20">
    <scramble_parameters wacn="12345" system="291" nac="659"/>
  </decode_configuration>`
	if got != want {
		t.Errorf("p25p2 scramble mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestRenderDecodeP25P2Defaults — no scramble → auto-detect on, self-closing.
func TestRenderDecodeP25P2Defaults(t *testing.T) {
	got := renderDecodeConfig("p25p2", nil)
	want := `<decode_configuration type="decodeConfigP25Phase2" auto_detect_scramble_parameters="true" ignore_data_calls="false" traffic_channel_pool_size="20"/>`
	if got != want {
		t.Errorf("p25p2 defaults mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestRenderDecodeDMR — two timeslots, ignore_data_calls default true.
func TestRenderDecodeDMR(t *testing.T) {
	got := renderDecodeConfig("dmr", &DecoderConfig{
		Timeslots: []DmrTimeslot{
			{Lcn: 187, Downlink: 166408000, Uplink: 0},
			{Lcn: 188, Downlink: 166658000, Uplink: 156658000},
		},
	})
	want := `<decode_configuration type="decodeConfigDMR" ignore_crc="false" use_compressed_talkgroups="false" ignore_data_calls="true" traffic_channel_pool_size="20">
    <timeslot lsn="187" downlink="166408000" uplink="0"/>
    <timeslot lsn="188" downlink="166658000" uplink="156658000"/>
  </decode_configuration>`
	if got != want {
		t.Errorf("dmr mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestRenderDecodeNBFM — bandwidth BW_25_0; non-GUI squelch* defaults emitted.
func TestRenderDecodeNBFM(t *testing.T) {
	got := renderDecodeConfig("nbfm", &DecoderConfig{
		Bandwidth:   "BW_25_0",
		Talkgroup:   ptrInt(42),
		AudioFilter: ptrBool(false),
	})
	want := `<decode_configuration type="decodeConfigNBFM" bandwidth="BW_25_0" talkgroup="42" audioFilter="false" squelchNoiseOpenThreshold="0.1" squelchNoiseCloseThreshold="0.19" squelchHysteresisOpenThreshold="4" squelchHysteresisCloseThreshold="6"/>`
	if got != want {
		t.Errorf("nbfm mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestRenderDecodeAM — squelch -50, autoTrack off.
func TestRenderDecodeAM(t *testing.T) {
	got := renderDecodeConfig("am", &DecoderConfig{
		Bandwidth: "BW_8_33",
		Talkgroup: ptrInt(5),
		Squelch:   ptrInt(-50),
		AutoTrack: ptrBool(false),
	})
	want := `<decode_configuration type="decodeConfigAM" bandwidth="BW_8_33" talkgroup="5" autoTrack="false" squelch="-50"/>`
	if got != want {
		t.Errorf("am mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestRenderDecodeAMDefaults — omitted fields → BW_15_0 / tg 1 / autoTrack true / squelch -78.
func TestRenderDecodeAMDefaults(t *testing.T) {
	got := renderDecodeConfig("am", nil)
	want := `<decode_configuration type="decodeConfigAM" bandwidth="BW_15_0" talkgroup="1" autoTrack="true" squelch="-78"/>`
	if got != want {
		t.Errorf("am defaults mismatch:\n got: %s\nwant: %s", got, want)
	}
}

// TestRenderPlaylistSwapsDecodeConfig verifies the wholesale swap through the full
// renderer: the preset's self-closing p25p1 element is replaced by a p25p2 element
// WITH a <scramble_parameters> child, and the old p25p1-only attrs are gone.
func TestRenderPlaylistSwapsDecodeConfig(t *testing.T) {
	channels := []ChannelPlan{{
		Name: "Metro CC", Frequency: 142658000, Decoder: "p25p2", Order: 1,
		DecoderConfig: &DecoderConfig{Scramble: &Scramble{Wacn: 12345, System: 291, Nac: 659}},
	}}
	out, err := renderPlaylist(presets.DefaultPlaylistXML, channels, nil, map[int]string{})
	if err != nil {
		t.Fatalf("renderPlaylist: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `<scramble_parameters wacn="12345" system="291" nac="659"/>`) {
		t.Errorf("scramble child not rendered:\n%s", s)
	}
	if !strings.Contains(s, `type="decodeConfigP25Phase2"`) {
		t.Errorf("decode type not swapped to p25p2")
	}
	// The preset's p25p1-only modulation attr must be gone after the wholesale swap.
	if strings.Contains(s, `modulation="C4FM"`) {
		t.Errorf("old p25p1 attributes leaked after decode_configuration swap")
	}
	// Exactly one decode_configuration in the rendered channel.
	if n := strings.Count(s, "<decode_configuration"); n != 1 {
		t.Errorf("expected 1 decode_configuration, got %d", n)
	}
	if strings.Count(s, "</decode_configuration>") != 1 {
		t.Errorf("expected 1 decode_configuration close tag")
	}
}

// TestRenderPlaylistMultiChannel verifies the preset's single <channel> block is
// cloned once per configured channel with the right per-channel fields, and that
// streams are pointed at the local rdio.
func TestRenderPlaylistMultiChannel(t *testing.T) {
	keys := map[int]string{1: "key-one", 2: "key-two"}
	channels := []ChannelPlan{
		{Name: "Metro CC", Frequency: 142658000, Decoder: "p25p1", System: "NSW PSN", Site: "Site 1", Order: 1},
		{Name: "Regional CC", Frequency: 419587500, Decoder: "p25p2", System: "NSW PSN", Site: "Site 2", Order: 2},
	}

	out, err := renderPlaylist(presets.DefaultPlaylistXML, channels, nil, keys)
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

// TestRenderPlaylistNoChannels emits NO <channel> element when nothing is
// configured (an empty channel set is a valid playlist) — so the UI never shows
// a non-removable "preset" channel.
func TestRenderPlaylistNoChannels(t *testing.T) {
	out, err := renderPlaylist(presets.DefaultPlaylistXML, nil, nil, map[int]string{})
	if err != nil {
		t.Fatalf("renderPlaylist: %v", err)
	}
	s := string(out)
	blocks := reChannelBlock.FindAllString(s, -1)
	if len(blocks) != 0 {
		t.Fatalf("expected no channel blocks, got %d:\n%s", len(blocks), s)
	}
	// The rest of the playlist (streams, aliases) must still be intact.
	if !strings.Contains(s, "<stream") || !strings.Contains(s, "</playlist>") {
		t.Errorf("no-channels playlist lost its streams/root")
	}
}

// TestRenderPlaylistGlobalAliases verifies the preset's alias region is replaced
// by the supplied global aliases, and the channels/streams still render.
func TestRenderPlaylistGlobalAliases(t *testing.T) {
	aliases := []Alias{
		{Name: "Fireground 1", List: "catch all PSN", Group: "RFS", Color: "-16776961", IDs: []AliasID{
			{Type: "talkgroup", Attrs: map[string]string{"value": "1201"}},
			{Type: "priority", Attrs: map[string]string{"priority": "1"}},
		}},
		{Name: "Command Net", List: "catch all PSN", IDs: []AliasID{
			{Type: "talkgroup", Attrs: map[string]string{"value": "1400"}},
		}},
	}
	channels := []ChannelPlan{{Name: "CC", Frequency: 142658000, Decoder: "p25p1", Order: 1}}

	out, err := renderPlaylist(presets.DefaultPlaylistXML, channels, aliases, map[int]string{})
	if err != nil {
		t.Fatalf("renderPlaylist: %v", err)
	}
	s := string(out)
	for _, want := range []string{
		`name="Fireground 1"`,
		`name="Command Net"`,
		`<id type="talkgroup" value="1201"/>`,
		`<id type="priority" priority="1"/>`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("rendered aliases missing %q", want)
		}
	}
	// Channel + streams still present.
	if !strings.Contains(s, `frequency="142658000"`) {
		t.Errorf("channel frequency lost after alias render")
	}
	if !strings.Contains(s, `host="`+localRdioUploadURL+`"`) {
		t.Errorf("streams lost after alias render")
	}
	// The whole preset alias region is replaced — exactly 2 aliases now. Count
	// closing tags (</alias>) so <alias_list_name> inside the channel isn't
	// miscounted.
	if n := strings.Count(s, "</alias>"); n != 2 {
		t.Errorf("expected exactly 2 aliases after replace, got %d", n)
	}
}
