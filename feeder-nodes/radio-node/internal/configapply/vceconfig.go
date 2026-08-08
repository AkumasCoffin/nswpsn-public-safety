// vce ConfigurationState builder: maps a backend ConfigPayload onto the JSON
// document sdrtrunk-vce's POST /config/import accepts. The element shapes are
// the Jackson-serialized forms of vce's Java model classes, ground-truthed from
// the sdrtrunk-vce sources:
//
//   - configuration/ConfigurationState.java — top-level properties
//     (aliasListDefinitions, aliases, channels, broadcastConfigurations).
//   - controller/channel/Channel.java — channel bean properties; auto-start is
//     "autoStart" (JSON alias "enabled"), order is "autoStartOrder" (alias
//     "order").
//   - source/config/SourceConfiguration.java — @JsonTypeInfo property "type",
//     subtype "sourceConfigTuner" (SourceConfigTuner: frequency,
//     preferredTuner, sourceType).
//   - module/decode/config/DecodeConfiguration.java — @JsonTypeInfo property
//     "type"; subtypes decodeConfigP25Phase1 / decodeConfigP25Phase2 /
//     decodeConfigDMR / decodeConfigNBFM. decodeConfigAM is RETIRED in vce
//     (see database/importer/LegacyXmlConfigurationImporter.java
//     RETIRED_DECODER_CONFIG_TYPES), so "am" channels are skipped.
//   - alias/Alias.java — vce aliases carry exactly ONE matchIdentifier plus
//     broadcastChannels / callPriority / recordable / streamTalkgroupAlias
//     attributes. A legacy multi-id alias is split into one vce alias per
//     matcher, exactly like the vce legacy importer's LegacyAlias.toAliases.
//   - alias/id/AliasID.java — @JsonTypeInfo property "type"; subtype names
//     talkgroup / talkgroupRange / radio / radioRange / broadcastChannel /
//     streamAsTalkgroup.
//   - alias/AliasListDefinition.java + alias/AliasListFamily.java — each list
//     needs a name and protocol family (P25/DMR/NXDN/NBFM).
//   - audio/broadcast/BroadcastConfiguration.java — @JsonTypeInfo property
//     "type"; subtype "RdioScannerConfiguration" (rdioscanner/
//     RdioScannerConfiguration.java: name, host, port, apiKey, systemID,
//     enabled). The broadcaster uses host as the FULL upload URI
//     (URI.create(configuration.getHost())), so host carries the complete
//     local rdio call-upload URL.
//
// Unknown fields are ignored by the vce importer, but type discriminators and
// field names must match exactly.
package configapply

import (
	"log"
	"regexp"
	"strconv"
	"strings"
)

// vceConfigState is the POST /config/import body (vce ConfigurationState).
type vceConfigState struct {
	AliasListDefinitions    []vceAliasListDef `json:"aliasListDefinitions"`
	Aliases                 []vceAlias        `json:"aliases"`
	Channels                []vceChannel      `json:"channels"`
	BroadcastConfigurations []vceBroadcast    `json:"broadcastConfigurations"`
}

// vceAliasListDef mirrors alias/AliasListDefinition.java.
type vceAliasListDef struct {
	Name   string `json:"name"`
	Family string `json:"family"` // "P25" | "DMR" | "NXDN" | "NBFM"
}

// vceAliasID is the polymorphic alias identifier (alias/id/AliasID.java
// subtypes). One struct covers every subtype the agent emits; omitempty keeps
// each instance to its own subtype's fields.
type vceAliasID struct {
	Type     string `json:"type"`
	Protocol string `json:"protocol,omitempty"` // talkgroup/talkgroupRange/radio/radioRange
	Value    *int   `json:"value,omitempty"`    // talkgroup/radio/streamAsTalkgroup
	// talkgroupRange (TalkgroupRange.java: minTalkgroup/maxTalkgroup)
	MinTalkgroup *int `json:"minTalkgroup,omitempty"`
	MaxTalkgroup *int `json:"maxTalkgroup,omitempty"`
	// radioRange (RadioRange.java: minRadio/maxRadio)
	MinRadio *int `json:"minRadio,omitempty"`
	MaxRadio *int `json:"maxRadio,omitempty"`
	// broadcastChannel (BroadcastChannel.java: channelName)
	ChannelName string `json:"channelName,omitempty"`
}

// vceAlias mirrors alias/Alias.java bean properties.
type vceAlias struct {
	Name                 string       `json:"name"`
	AliasListName        string       `json:"aliasListName,omitempty"`
	Group                string       `json:"group,omitempty"`
	Color                int          `json:"color"`
	IconName             string       `json:"iconName,omitempty"`
	CallPriority         *int         `json:"callPriority,omitempty"`
	Recordable           *bool        `json:"recordable,omitempty"`
	MatchIdentifier      *vceAliasID  `json:"matchIdentifier,omitempty"`
	BroadcastChannels    []vceAliasID `json:"broadcastChannels,omitempty"`
	StreamTalkgroupAlias *vceAliasID  `json:"streamTalkgroupAlias,omitempty"`
}

// vceSourceConfig mirrors source/config/SourceConfigTuner.java.
type vceSourceConfig struct {
	Type           string `json:"type"`       // "sourceConfigTuner"
	SourceType     string `json:"sourceType"` // "TUNER"
	Frequency      int64  `json:"frequency"`
	PreferredTuner string `json:"preferredTuner,omitempty"`
}

// vceScramble mirrors module/decode/p25/phase2/enumeration/ScrambleParameters.java.
type vceScramble struct {
	Wacn   int `json:"wacn"`
	System int `json:"system"`
	Nac    int `json:"nac"`
}

// vceTimeslot mirrors module/decode/dmr/channel/TimeslotFrequency.java.
type vceTimeslot struct {
	Number            int   `json:"number"` // LSN
	DownlinkFrequency int64 `json:"downlinkFrequency"`
	UplinkFrequency   int64 `json:"uplinkFrequency"`
}

// vceDecodeConfig is the polymorphic decoder configuration. One struct covers
// the decoder subtypes the agent emits; omitempty/pointers keep each instance
// to its own subtype's fields.
type vceDecodeConfig struct {
	Type string `json:"type"`
	// p25 (DecodeConfigP25.java): shared by phase1/phase2; ignoreDataCalls is
	// also used by DMR (DecodeConfigDMR.java).
	IgnoreDataCalls        *bool `json:"ignoreDataCalls,omitempty"`
	TrafficChannelPoolSize *int  `json:"trafficChannelPoolSize,omitempty"`
	// p25 (DecodeConfigP25.java, shared by phase1/phase2): learn the alternate
	// control channels announced on the current one, so only ONE control freq
	// needs configuring and the node follows the site as it rotates.
	LearnAnnouncedControlChannels *bool `json:"learnAnnouncedControlChannels,omitempty"`
	// p25p1 (DecodeConfigP25Phase1.java)
	Modulation string `json:"modulation,omitempty"` // "C4FM" | "CQPSK"
	// p25p2 (DecodeConfigP25Phase2.java)
	AutoDetectScrambleParameters *bool        `json:"autoDetectScrambleParameters,omitempty"`
	ScrambleParameters           *vceScramble `json:"scrambleParameters,omitempty"`
	// dmr (DecodeConfigDMR.java)
	IgnoreCRCChecksums      *bool         `json:"ignoreCRCChecksums,omitempty"`
	UseCompressedTalkgroups *bool         `json:"useCompressedTalkgroups,omitempty"`
	TimeslotMap             []vceTimeslot `json:"timeslotMap,omitempty"`
	// nbfm (DecodeConfigNBFM.java / DecodeConfigAnalog.java). The squelch/
	// enhancement fields are deliberately omitted so vce's own defaults apply.
	Bandwidth   string `json:"bandwidth,omitempty"` // BW_7_5 | BW_12_5 | BW_25_0
	Talkgroup   *int   `json:"talkgroup,omitempty"`
	AudioFilter *bool  `json:"audioFilter,omitempty"`
}

// vceChannel mirrors controller/channel/Channel.java bean properties.
type vceChannel struct {
	Name                string          `json:"name"`
	System              string          `json:"system,omitempty"`
	Site                string          `json:"site,omitempty"`
	AliasListName       string          `json:"aliasListName,omitempty"`
	AutoStart           bool            `json:"autoStart"` // JSON alias "enabled" in vce
	AutoStartOrder      *int            `json:"autoStartOrder,omitempty"`
	SourceConfiguration vceSourceConfig `json:"sourceConfiguration"`
	DecodeConfiguration vceDecodeConfig `json:"decodeConfiguration"`
}

// vceBroadcast mirrors audio/broadcast/rdioscanner/RdioScannerConfiguration.java.
type vceBroadcast struct {
	Type     string `json:"type"` // "RdioScannerConfiguration"
	Name     string `json:"name"`
	Host     string `json:"host"` // FULL upload URI (URI.create(getHost()))
	Port     int    `json:"port"`
	ApiKey   string `json:"apiKey"`
	SystemID int    `json:"systemID"`
	Enabled  bool   `json:"enabled"`
}

// localRdioPort is the port of the supervised local rdio-scanner (must match
// localRdioUploadURL).
const localRdioPort = 17391

// defaultAliasListName is the alias list the preset playlist declares; used
// when the preset can't be read/parsed.
const defaultAliasListName = "catch all PSN"

// matcherProtocols are the protocols vce still supports for talkgroup/radio
// alias matchers (protocol/Protocol.java, minus retired entries). A matcher
// with any other protocol (e.g. the retired "AM") is dropped — an unknown enum
// value could otherwise fail the whole import.
var matcherProtocols = map[string]string{ // protocol -> alias-list family
	"APCO25": "P25",
	"DMR":    "DMR",
	"NXDN":   "NXDN",
	"NBFM":   "NBFM",
}

var reAliasListAttr = regexp.MustCompile(`<alias\b[^>]*\blist="([^"]+)"`)

// presetAliasListName extracts the alias-list name declared by the preset
// playlist (the list every P25 channel references).
func (d Deps) presetAliasListName() string {
	m := reAliasListAttr.FindSubmatch(d.loadPlaylistTemplate())
	if len(m) == 2 {
		if name := strings.TrimSpace(unescapeXMLAttr(string(m[1]))); name != "" {
			return name
		}
	}
	return defaultAliasListName
}

// unescapeXMLAttr reverses the basic XML attribute entities the preset uses.
func unescapeXMLAttr(s string) string {
	r := strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">", "&quot;", `"`)
	return r.Replace(s)
}

// buildVceConfig maps a ConfigPayload onto the vce ConfigurationState body for
// POST /config/import. presetList is the alias-list name P25 channels
// reference (from the preset playlist); localKeys are the per-system local
// rdio API keys.
func buildVceConfig(payload ConfigPayload, localKeys map[int]string, presetList string) vceConfigState {
	state := vceConfigState{
		AliasListDefinitions:    []vceAliasListDef{},
		Aliases:                 []vceAlias{},
		Channels:                []vceChannel{},
		BroadcastConfigurations: []vceBroadcast{},
	}

	// ---- channels -----------------------------------------------------------
	// effectiveChannels honours Node on/off: capture-off forces every channel
	// autoStart=false so the import brings up nothing.
	for _, ch := range payload.effectiveChannels() {
		dc, ok := buildDecodeConfig(ch.Decoder, ch.DecoderConfig)
		if !ok {
			log.Printf("configapply: channel %q: decoder %q not supported by sdrtrunk-vce; skipping", ch.Name, ch.Decoder)
			continue
		}
		vch := vceChannel{
			Name:      strings.TrimSpace(ch.Name),
			System:    ch.System,
			Site:      ch.Site,
			AutoStart: ch.AutoStart,
			SourceConfiguration: vceSourceConfig{
				Type:           "sourceConfigTuner",
				SourceType:     "TUNER",
				Frequency:      ch.Frequency,
				PreferredTuner: strings.TrimSpace(ch.SDR),
			},
			DecodeConfiguration: dc,
		}
		if ch.Order > 0 {
			order := ch.Order
			vch.AutoStartOrder = &order
		}
		// Alias lists are protocol-family-owned in vce; the preset list is a P25
		// list, so only P25 channels may reference it (a family mismatch would be
		// rejected/nulled by vce's channel-compatibility policy).
		if ch.Decoder == "p25p1" || ch.Decoder == "p25p2" {
			vch.AliasListName = presetList
		}
		state.Channels = append(state.Channels, vch)
	}

	// ---- aliases + list definitions ----------------------------------------
	// Mirror the vce legacy importer (LegacyAlias.toAliases): each legacy
	// multi-id alias becomes one vce alias per matcher id, with the non-matcher
	// ids (broadcastChannel / priority / record / nonRecordable /
	// streamAsTalkgroup) folded into shared alias attributes.
	listFamilies := map[string]string{} // list name -> family
	if presetList != "" {
		listFamilies[presetList] = "P25"
	}
	for _, a := range payload.Aliases {
		listName := strings.TrimSpace(a.List)
		tmpl := vceAlias{
			Name:          a.Name,
			AliasListName: listName,
			Group:         a.Group,
			Color:         atoiOr(a.Color, 0),
			IconName:      a.IconName,
		}
		if sta := strings.TrimSpace(string(a.StreamTalkgroupAlias)); sta != "" && sta != "0" {
			// Same guard as the legacy playlist render: a 0/blank "stream as
			// talkgroup" would force every matching call to upload as talkgroup 0.
			if v, err := strconv.Atoi(sta); err == nil && v > 0 {
				tmpl.StreamTalkgroupAlias = &vceAliasID{Type: "streamAsTalkgroup", Value: &v}
			}
		}

		var matchers []vceAliasID
		for _, id := range a.IDs {
			switch id.Type {
			case "broadcastChannel":
				if ch := strings.TrimSpace(id.Attrs["channel"]); ch != "" {
					tmpl.BroadcastChannels = append(tmpl.BroadcastChannels,
						vceAliasID{Type: "broadcastChannel", ChannelName: ch})
				}
			case "priority":
				if p, err := strconv.Atoi(strings.TrimSpace(id.Attrs["priority"])); err == nil {
					pp := p
					tmpl.CallPriority = &pp
				}
			case "record":
				t := true
				tmpl.Recordable = &t
			case "nonRecordable":
				f := false
				tmpl.Recordable = &f
			case "streamAsTalkgroup":
				if v, err := strconv.Atoi(strings.TrimSpace(id.Attrs["value"])); err == nil && v > 0 {
					vv := v
					tmpl.StreamTalkgroupAlias = &vceAliasID{Type: "streamAsTalkgroup", Value: &vv}
				}
			case "talkgroup", "radio":
				proto := strings.TrimSpace(id.Attrs["protocol"])
				fam, supported := matcherProtocols[proto]
				v, err := strconv.Atoi(strings.TrimSpace(id.Attrs["value"]))
				if !supported || err != nil {
					log.Printf("configapply: alias %q: dropping %s id (protocol=%q) not supported by sdrtrunk-vce", a.Name, id.Type, proto)
					continue
				}
				vv := v
				matchers = append(matchers, vceAliasID{Type: id.Type, Protocol: proto, Value: &vv})
				rememberFamily(listFamilies, listName, fam)
			case "talkgroupRange", "radioRange":
				proto := strings.TrimSpace(id.Attrs["protocol"])
				fam, supported := matcherProtocols[proto]
				lo, errLo := strconv.Atoi(strings.TrimSpace(id.Attrs["min"]))
				hi, errHi := strconv.Atoi(strings.TrimSpace(id.Attrs["max"]))
				if !supported || errLo != nil || errHi != nil {
					log.Printf("configapply: alias %q: dropping %s id (protocol=%q) not supported by sdrtrunk-vce", a.Name, id.Type, proto)
					continue
				}
				m := vceAliasID{Type: id.Type, Protocol: proto}
				if id.Type == "talkgroupRange" {
					m.MinTalkgroup, m.MaxTalkgroup = &lo, &hi
				} else {
					m.MinRadio, m.MaxRadio = &lo, &hi
				}
				matchers = append(matchers, m)
				rememberFamily(listFamilies, listName, fam)
			default:
				// Retired/unsupported alias id type: drop, like the vce importer.
			}
		}

		if listName != "" {
			rememberFamily(listFamilies, listName, "") // ensure the list exists even without matchers
		}
		if len(matchers) == 0 {
			// vce requires exactly one matchIdentifier per alias; an alias whose
			// matcher ids were all dropped (or that only carried broadcast/priority/
			// record entries) can never match anything, and emitting it matcher-less
			// 400s the whole /config/import. Skip it.
			log.Printf("configapply: alias %q: no usable match identifiers - skipping", a.Name)
			continue
		}
		for i := range matchers {
			alias := tmpl // copy
			m := matchers[i]
			alias.MatchIdentifier = &m
			state.Aliases = append(state.Aliases, alias)
		}
	}

	for name, fam := range listFamilies {
		if fam == "" {
			fam = "P25"
		}
		state.AliasListDefinitions = append(state.AliasListDefinitions, vceAliasListDef{Name: name, Family: fam})
	}
	sortAliasListDefs(state.AliasListDefinitions)

	// ---- streams ------------------------------------------------------------
	// One RdioScanner broadcast per stream target/system, uploading to the
	// supervised local rdio with that system's stable local key. Feed off →
	// streams disabled (paired with the disabled rdio downstream).
	feed := payload.feedOn()
	for _, t := range payload.StreamTargets {
		state.BroadcastConfigurations = append(state.BroadcastConfigurations, vceBroadcast{
			Type:     "RdioScannerConfiguration",
			Name:     t.Name,
			Host:     localRdioUploadURL,
			Port:     localRdioPort,
			ApiKey:   localKeys[t.SystemId],
			SystemID: t.SystemId,
			Enabled:  feed,
		})
	}

	return state
}

// rememberFamily records a family claim for a list; the first protocol-specific
// claim wins (payload lists are single-protocol in practice).
func rememberFamily(families map[string]string, list, fam string) {
	if list == "" {
		return
	}
	if cur, ok := families[list]; !ok || (cur == "" && fam != "") {
		families[list] = fam
	}
}

// sortAliasListDefs sorts definitions by name for deterministic output.
func sortAliasListDefs(defs []vceAliasListDef) {
	for i := 1; i < len(defs); i++ {
		for j := i; j > 0 && defs[j].Name < defs[j-1].Name; j-- {
			defs[j], defs[j-1] = defs[j-1], defs[j]
		}
	}
}

// nbfmBandwidths are the valid vce NBFM bandwidth enum constants
// (DecodeConfigAnalog.Bandwidth).
var nbfmBandwidths = map[string]bool{"BW_7_5": true, "BW_12_5": true, "BW_25_0": true}

// buildDecodeConfig maps an agent decoder name + optional DecoderConfig onto
// the vce decode configuration, filling the same defaults the legacy playlist
// render used. ok=false means the decoder has no vce equivalent (e.g. the
// retired AM decoder) and the channel must be skipped.
func buildDecodeConfig(decoder string, cfg *DecoderConfig) (vceDecodeConfig, bool) {
	if cfg == nil {
		cfg = &DecoderConfig{}
	}
	switch decoder {
	case "p25p1":
		mod := cfg.Modulation
		if mod != "CQPSK" {
			mod = "C4FM"
		}
		return vceDecodeConfig{
			Type:                          "decodeConfigP25Phase1",
			Modulation:                    mod,
			LearnAnnouncedControlChannels: boolPtrOr(cfg.LearnControlChannels, true),
			IgnoreDataCalls:               boolPtrOr(cfg.IgnoreDataCalls, false),
			TrafficChannelPoolSize:        intPtrOr(cfg.TrafficPoolSize, 20),
		}, true

	case "p25p2":
		// Auto-detect defaults to true when no manual scramble is supplied.
		auto := cfg.Scramble == nil
		if cfg.AutoDetectScramble != nil {
			auto = *cfg.AutoDetectScramble
		}
		dc := vceDecodeConfig{
			Type:                          "decodeConfigP25Phase2",
			AutoDetectScrambleParameters:  &auto,
			LearnAnnouncedControlChannels: boolPtrOr(cfg.LearnControlChannels, true),
			IgnoreDataCalls:               boolPtrOr(cfg.IgnoreDataCalls, false),
			TrafficChannelPoolSize:        intPtrOr(cfg.TrafficPoolSize, 20),
		}
		if cfg.Scramble != nil {
			dc.ScrambleParameters = &vceScramble{
				Wacn:   cfg.Scramble.Wacn,
				System: cfg.Scramble.System,
				Nac:    cfg.Scramble.Nac,
			}
		}
		return dc, true

	case "dmr":
		dc := vceDecodeConfig{
			Type:                    "decodeConfigDMR",
			IgnoreCRCChecksums:      boolPtrOr(cfg.IgnoreCrc, false),
			UseCompressedTalkgroups: boolPtrOr(cfg.UseCompressedTalkgroups, false),
			IgnoreDataCalls:         boolPtrOr(cfg.IgnoreDataCalls, true),
			TrafficChannelPoolSize:  intPtrOr(cfg.TrafficPoolSize, 20),
		}
		for _, ts := range cfg.Timeslots {
			dc.TimeslotMap = append(dc.TimeslotMap, vceTimeslot{
				Number:            ts.Lcn,
				DownlinkFrequency: ts.Downlink,
				UplinkFrequency:   ts.Uplink,
			})
		}
		return dc, true

	case "nbfm":
		bw := cfg.Bandwidth
		if !nbfmBandwidths[bw] {
			bw = "BW_12_5"
		}
		return vceDecodeConfig{
			Type:        "decodeConfigNBFM",
			Bandwidth:   bw,
			Talkgroup:   intPtrOr(cfg.Talkgroup, 1),
			AudioFilter: boolPtrOr(cfg.AudioFilter, true),
		}, true

	default:
		// "am" (retired in sdrtrunk-vce) and anything unknown.
		return vceDecodeConfig{}, false
	}
}

// boolPtrOr returns p when set, else a pointer to def.
func boolPtrOr(p *bool, def bool) *bool {
	if p != nil {
		return p
	}
	return &def
}

// intPtrOr returns p when set, else a pointer to def.
func intPtrOr(p *int, def int) *int {
	if p != nil {
		return p
	}
	return &def
}

// atoiOr parses s as an int, returning def on failure.
func atoiOr(s string, def int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
		return v
	}
	return def
}
