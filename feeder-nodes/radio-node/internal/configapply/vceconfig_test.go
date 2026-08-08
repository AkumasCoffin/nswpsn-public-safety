package configapply

import (
	"encoding/json"
	"strings"
	"testing"
)

func ptrBool(b bool) *bool { return &b }
func ptrInt(i int) *int    { return &i }

// marshalState builds + marshals a payload and returns the decoded generic JSON
// document, so assertions exercise the exact wire shape (field names + type
// discriminators) rather than Go struct internals.
func marshalState(t *testing.T, payload ConfigPayload, keys map[int]string) map[string]any {
	t.Helper()
	state := buildVceConfig(payload, keys, "catch all PSN")
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("marshal vce config: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("re-decode vce config: %v", err)
	}
	return doc
}

func arr(t *testing.T, doc map[string]any, key string) []any {
	t.Helper()
	v, ok := doc[key].([]any)
	if !ok {
		t.Fatalf("top-level %q missing or not an array: %T", key, doc[key])
	}
	return v
}

// TestBuildVceConfigTopLevel — the four ConfigurationState properties are always
// present (empty arrays, never null) and correctly named.
func TestBuildVceConfigTopLevel(t *testing.T) {
	doc := marshalState(t, ConfigPayload{}, nil)
	for _, key := range []string{"aliasListDefinitions", "aliases", "channels", "broadcastConfigurations"} {
		if _, ok := doc[key].([]any); !ok {
			t.Errorf("top-level %q missing or not an array (got %T)", key, doc[key])
		}
	}
}

// TestBuildVceConfigChannelShape — channel bean fields, the sourceConfigTuner
// discriminator, and the decodeConfigP25Phase1 discriminator/fields.
func TestBuildVceConfigChannelShape(t *testing.T) {
	payload := ConfigPayload{
		Channels: []ChannelPlan{{
			Name: "Metro CC", Frequency: 142658000, Decoder: "p25p1",
			System: "NSW PSN", Site: "Site 1", AutoStart: true, Order: 2, SDR: "00000101",
			DecoderConfig: &DecoderConfig{Modulation: "CQPSK", IgnoreDataCalls: ptrBool(true), TrafficPoolSize: ptrInt(50)},
		}},
	}
	doc := marshalState(t, payload, nil)
	channels := arr(t, doc, "channels")
	if len(channels) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(channels))
	}
	ch := channels[0].(map[string]any)
	for k, want := range map[string]any{
		"name": "Metro CC", "system": "NSW PSN", "site": "Site 1",
		"aliasListName": "catch all PSN", "autoStart": true, "autoStartOrder": float64(2),
	} {
		if ch[k] != want {
			t.Errorf("channel %q = %v, want %v", k, ch[k], want)
		}
	}
	src := ch["sourceConfiguration"].(map[string]any)
	for k, want := range map[string]any{
		"type": "sourceConfigTuner", "sourceType": "TUNER",
		"frequency": float64(142658000), "preferredTuner": "00000101",
	} {
		if src[k] != want {
			t.Errorf("sourceConfiguration %q = %v, want %v", k, src[k], want)
		}
	}
	dec := ch["decodeConfiguration"].(map[string]any)
	for k, want := range map[string]any{
		"type": "decodeConfigP25Phase1", "modulation": "CQPSK",
		"ignoreDataCalls": true, "trafficChannelPoolSize": float64(50),
		// learn-announced-control-channels defaults on so one control freq suffices.
		"learnAnnouncedControlChannels": true,
	} {
		if dec[k] != want {
			t.Errorf("decodeConfiguration %q = %v, want %v", k, dec[k], want)
		}
	}
}

// TestBuildVceConfigDecoders — per-decoder discriminators + defaults; the
// retired AM decoder is skipped entirely.
func TestBuildVceConfigDecoders(t *testing.T) {
	payload := ConfigPayload{
		Channels: []ChannelPlan{
			{Name: "P2", Frequency: 1, Decoder: "p25p2",
				DecoderConfig: &DecoderConfig{Scramble: &Scramble{Wacn: 12345, System: 291, Nac: 659}}},
			{Name: "DMR", Frequency: 2, Decoder: "dmr",
				DecoderConfig: &DecoderConfig{Timeslots: []DmrTimeslot{{Lcn: 187, Downlink: 166408000, Uplink: 156408000}}}},
			{Name: "FM", Frequency: 3, Decoder: "nbfm", DecoderConfig: &DecoderConfig{Bandwidth: "BW_25_0", Talkgroup: ptrInt(42)}},
			{Name: "Air", Frequency: 4, Decoder: "am"}, // retired in vce → skipped
		},
	}
	doc := marshalState(t, payload, nil)
	channels := arr(t, doc, "channels")
	if len(channels) != 3 {
		t.Fatalf("expected 3 channels (am skipped), got %d", len(channels))
	}

	p2 := channels[0].(map[string]any)["decodeConfiguration"].(map[string]any)
	if p2["type"] != "decodeConfigP25Phase2" {
		t.Errorf("p25p2 discriminator = %v", p2["type"])
	}
	if p2["autoDetectScrambleParameters"] != false {
		t.Errorf("manual scramble should disable auto-detect, got %v", p2["autoDetectScrambleParameters"])
	}
	sp := p2["scrambleParameters"].(map[string]any)
	if sp["wacn"] != float64(12345) || sp["system"] != float64(291) || sp["nac"] != float64(659) {
		t.Errorf("scrambleParameters mismatch: %v", sp)
	}

	dmr := channels[1].(map[string]any)["decodeConfiguration"].(map[string]any)
	if dmr["type"] != "decodeConfigDMR" {
		t.Errorf("dmr discriminator = %v", dmr["type"])
	}
	if dmr["ignoreDataCalls"] != true { // DMR default is true
		t.Errorf("dmr ignoreDataCalls default = %v, want true", dmr["ignoreDataCalls"])
	}
	if dmr["ignoreCRCChecksums"] != false || dmr["useCompressedTalkgroups"] != false {
		t.Errorf("dmr crc/compressed defaults wrong: %v", dmr)
	}
	ts := dmr["timeslotMap"].([]any)[0].(map[string]any)
	for k, want := range map[string]any{
		"number": float64(187), "downlinkFrequency": float64(166408000), "uplinkFrequency": float64(156408000),
	} {
		if ts[k] != want {
			t.Errorf("timeslotMap %q = %v, want %v", k, ts[k], want)
		}
	}

	fm := channels[2].(map[string]any)["decodeConfiguration"].(map[string]any)
	if fm["type"] != "decodeConfigNBFM" || fm["bandwidth"] != "BW_25_0" || fm["talkgroup"] != float64(42) {
		t.Errorf("nbfm mismatch: %v", fm)
	}
	if fm["audioFilter"] != true {
		t.Errorf("nbfm audioFilter default = %v, want true", fm["audioFilter"])
	}
	// A non-P25 channel must not reference the P25 preset alias list.
	if al, ok := channels[2].(map[string]any)["aliasListName"]; ok && al != "" {
		t.Errorf("nbfm channel should not carry the P25 alias list, got %v", al)
	}
}

// TestBuildVceConfigNBFMBandwidthValidated — an AM-era bandwidth constant that
// doesn't exist in vce's NBFM enum falls back to BW_12_5 (an unknown enum value
// could fail the whole import).
func TestBuildVceConfigNBFMBandwidthValidated(t *testing.T) {
	dc, ok := buildDecodeConfig("nbfm", &DecoderConfig{Bandwidth: "BW_15_0"})
	if !ok {
		t.Fatal("nbfm must be supported")
	}
	if dc.Bandwidth != "BW_12_5" {
		t.Errorf("invalid bandwidth should fall back to BW_12_5, got %s", dc.Bandwidth)
	}
}

// TestBuildVceConfigAliasSplit — a legacy multi-id alias is split into one vce
// alias per matcher, with broadcastChannel/priority ids folded into shared alias
// attributes on EVERY split alias (mirrors vce's LegacyAlias.toAliases).
func TestBuildVceConfigAliasSplit(t *testing.T) {
	payload := ConfigPayload{
		Aliases: []Alias{{
			Name: "Fire & Rescue", List: "catch all PSN", Group: "FRNSW", Color: "-65536", IconName: "Fire",
			IDs: []AliasID{
				{Type: "priority", Attrs: map[string]string{"priority": "-1"}},
				{Type: "broadcastChannel", Attrs: map[string]string{"channel": "FRNSW"}},
				{Type: "talkgroupRange", Attrs: map[string]string{"protocol": "APCO25", "min": "10101", "max": "10199"}},
				{Type: "talkgroup", Attrs: map[string]string{"protocol": "APCO25", "value": "20101"}},
			},
		}},
	}
	doc := marshalState(t, payload, nil)
	aliases := arr(t, doc, "aliases")
	if len(aliases) != 2 {
		t.Fatalf("expected the 2-matcher alias split into 2 vce aliases, got %d", len(aliases))
	}
	for i, raw := range aliases {
		a := raw.(map[string]any)
		for k, want := range map[string]any{
			"name": "Fire & Rescue", "aliasListName": "catch all PSN", "group": "FRNSW",
			"color": float64(-65536), "iconName": "Fire", "callPriority": float64(-1),
		} {
			if a[k] != want {
				t.Errorf("alias[%d] %q = %v, want %v", i, k, a[k], want)
			}
		}
		bc := a["broadcastChannels"].([]any)[0].(map[string]any)
		if bc["type"] != "broadcastChannel" || bc["channelName"] != "FRNSW" {
			t.Errorf("alias[%d] broadcastChannels mismatch: %v", i, bc)
		}
	}
	m0 := aliases[0].(map[string]any)["matchIdentifier"].(map[string]any)
	if m0["type"] != "talkgroupRange" || m0["protocol"] != "APCO25" ||
		m0["minTalkgroup"] != float64(10101) || m0["maxTalkgroup"] != float64(10199) {
		t.Errorf("first matcher mismatch: %v", m0)
	}
	m1 := aliases[1].(map[string]any)["matchIdentifier"].(map[string]any)
	if m1["type"] != "talkgroup" || m1["value"] != float64(20101) {
		t.Errorf("second matcher mismatch: %v", m1)
	}

	defs := arr(t, doc, "aliasListDefinitions")
	if len(defs) != 1 {
		t.Fatalf("expected 1 alias list definition, got %d", len(defs))
	}
	d0 := defs[0].(map[string]any)
	if d0["name"] != "catch all PSN" || d0["family"] != "P25" {
		t.Errorf("alias list definition mismatch: %v", d0)
	}
}

// TestBuildVceConfigDropsRetiredProtocols — a matcher using a protocol vce
// retired (the preset's AM airband talkgroup) is dropped, and an alias left
// with NO matchers is skipped entirely: vce's /config/import requires exactly
// one matchIdentifier per alias and rejects the whole payload otherwise
// ("Alias [...] must have exactly one match identifier").
func TestBuildVceConfigDropsRetiredProtocols(t *testing.T) {
	payload := ConfigPayload{
		Aliases: []Alias{{
			Name: "TWR YSNW", List: "catch all PSN",
			IDs: []AliasID{
				{Type: "broadcastChannel", Attrs: map[string]string{"channel": "AirBand"}},
				{Type: "talkgroup", Attrs: map[string]string{"protocol": "AM", "value": "5"}},
			},
		}},
	}
	doc := marshalState(t, payload, nil)
	aliases := arr(t, doc, "aliases")
	if len(aliases) != 0 {
		t.Fatalf("expected 0 aliases (matcher-less alias skipped), got %d", len(aliases))
	}
}

// TestBuildVceConfigStreamTalkgroupAlias — the "stream as talkgroup" override is
// emitted as a streamAsTalkgroup identifier, and the 0/blank guard holds (a 0
// here would upload every matching call as talkgroup 0).
func TestBuildVceConfigStreamTalkgroupAlias(t *testing.T) {
	tg := func(v string) []AliasID {
		return []AliasID{{Type: "talkgroup", Attrs: map[string]string{"protocol": "APCO25", "value": v}}}
	}
	payload := ConfigPayload{
		Aliases: []Alias{
			{Name: "A", StreamTalkgroupAlias: "1400", IDs: tg("101")},
			{Name: "B", StreamTalkgroupAlias: "0", IDs: tg("102")},
			{Name: "C", IDs: tg("103")},
		},
	}
	doc := marshalState(t, payload, nil)
	aliases := arr(t, doc, "aliases")
	sta := aliases[0].(map[string]any)["streamTalkgroupAlias"].(map[string]any)
	if sta["type"] != "streamAsTalkgroup" || sta["value"] != float64(1400) {
		t.Errorf("streamTalkgroupAlias mismatch: %v", sta)
	}
	for i := 1; i <= 2; i++ {
		if _, ok := aliases[i].(map[string]any)["streamTalkgroupAlias"]; ok {
			t.Errorf("alias %d must not emit a zero/blank streamAsTalkgroup", i)
		}
	}
}

// TestBuildVceConfigStreams — one RdioScannerConfiguration per stream target
// with the exact discriminator + field names, the local rdio upload URL as host,
// and the per-system local key.
func TestBuildVceConfigStreams(t *testing.T) {
	payload := ConfigPayload{
		StreamTargets: []StreamTarget{
			{SystemId: 2, Name: "FRNSW"},
			{SystemId: 99, Name: "AirBand"},
		},
	}
	keys := map[int]string{2: "key-two", 99: "key-99"}
	doc := marshalState(t, payload, keys)
	streams := arr(t, doc, "broadcastConfigurations")
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(streams))
	}
	s0 := streams[0].(map[string]any)
	for k, want := range map[string]any{
		"type": "RdioScannerConfiguration", "name": "FRNSW",
		"host": localRdioUploadURL, "port": float64(17391),
		"apiKey": "key-two", "systemID": float64(2), "enabled": true,
	} {
		if s0[k] != want {
			t.Errorf("stream %q = %v, want %v", k, s0[k], want)
		}
	}
	if !strings.HasPrefix(s0["host"].(string), "http://127.0.0.1:17391/") {
		t.Errorf("stream host must be the full local rdio upload URL, got %v", s0["host"])
	}
}

// TestBuildVceConfigFeedOff — feed off disables every stream (paired with the
// disabled rdio downstream).
func TestBuildVceConfigFeedOff(t *testing.T) {
	off := false
	payload := ConfigPayload{
		FeedEnabled:   &off,
		StreamTargets: []StreamTarget{{SystemId: 1, Name: "RFS"}},
	}
	doc := marshalState(t, payload, map[int]string{1: "k1"})
	s0 := arr(t, doc, "broadcastConfigurations")[0].(map[string]any)
	if s0["enabled"] != false {
		t.Errorf("feed off should disable the stream, got enabled=%v", s0["enabled"])
	}
}
