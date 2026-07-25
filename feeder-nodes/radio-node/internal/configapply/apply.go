// Package configapply turns a backend configPush payload into concrete local
// state on the node: stable per-agency API keys, a full rdio-scanner config PUT
// (apiKeys + a single downstream pointing at the agent's relay), and a rendered
// SDR-Trunk playlist, followed by a playlist reload.
//
// PLAYLIST FIDELITY WARNING
// -------------------------
// SDR-Trunk loads its playlist with Jackson's XmlMapper (PlaylistV2), which is
// intolerant of structural drift: attribute-order changes, dropped/renamed
// xmlns:wstxnsN prefixes on <stream>, collapsed self-closing tags, or altered
// entity encoding can make it silently reject channels/streams. For that reason
// this package NEVER regenerates the playlist from a Go struct. It performs
// narrow, attribute-level string edits against the known preset structure
// (default.xml), touching ONLY: the channel enabled flag + system/site labels,
// the channel source frequency(ies), and each stream's api_key + host. Every
// byte outside those edits is preserved verbatim from the preset.
//
// This still MUST be load-tested in real SDR-Trunk on an operator machine —
// especially the multi-control-frequency path (sourceConfigTunerMultiple), whose
// exact element shape is a best-effort match to SDR-Trunk's schema and has NOT
// been round-tripped through the real decoder here.
package configapply

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/keys"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/presets"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/rdioctl"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
)

// Local endpoints the rendered config points at. The agent's relay listener
// (P2) impersonates rdio call-upload at 127.0.0.1:17390; the supervised local
// rdio-scanner serves its call-upload + admin API at 127.0.0.1:17391.
const (
	relayDownstreamURL = "http://127.0.0.1:17390"
	localRdioUploadURL = "http://127.0.0.1:17391/api/call-upload"
)

// ChannelPlan is one SDR-Trunk channel from a configPush: the frequency to
// decode, its decoder type, and SDR-Trunk display/ordering metadata.
type ChannelPlan struct {
	Name      string `json:"name"`
	Frequency int64  `json:"frequency"` // Hz (control-channel freq for trunked P25)
	Decoder   string `json:"decoder"`   // p25p2|p25p1|dmr|nbfm|am
	System    string `json:"system"`
	Site      string `json:"site"`
	AutoStart bool   `json:"autoStart"`
	Order     int    `json:"order"`
	SDR       string `json:"sdr"` // device serial to pin to; "" = any tuner
	// DecoderConfig carries decoder-specific settings; nil = all defaults. The
	// renderer emits the matching <decode_configuration> and fills SDR-Trunk
	// defaults for any unset field.
	DecoderConfig *DecoderConfig `json:"decoderConfig"`
}

// DecoderConfig is a loose superset of every decoder's settings (see the
// SDR-Trunk config reference "decoderConfig JSON contract"). Which fields apply
// is decided by ChannelPlan.Decoder at render time. Pointers distinguish "unset
// → use SDR-Trunk default" from a real zero/false value.
type DecoderConfig struct {
	// p25p1
	Modulation string `json:"modulation"` // "C4FM" | "CQPSK"
	// shared: p25p1 / p25p2 / dmr
	IgnoreDataCalls *bool `json:"ignoreDataCalls"`
	TrafficPoolSize *int  `json:"trafficPoolSize"`
	// p25p2
	AutoDetectScramble *bool     `json:"autoDetectScramble"`
	Scramble           *Scramble `json:"scramble"`
	// dmr
	IgnoreCrc               *bool         `json:"ignoreCrc"`
	UseCompressedTalkgroups *bool         `json:"useCompressedTalkgroups"`
	Timeslots               []DmrTimeslot `json:"timeslots"`
	// nbfm / am
	Bandwidth   string `json:"bandwidth"`
	Talkgroup   *int   `json:"talkgroup"`
	AudioFilter *bool  `json:"audioFilter"` // nbfm
	// am
	Squelch   *int  `json:"squelch"` // dB, may be negative
	AutoTrack *bool `json:"autoTrack"`
}

// Scramble is a P25 Phase-2 manual scramble (descrambler) parameter set.
type Scramble struct {
	Wacn   int `json:"wacn"`
	System int `json:"system"`
	Nac    int `json:"nac"`
}

// DmrTimeslot maps a DMR logical slot: Lcn carries the LCN (rendered as the
// `lsn` attr), Downlink/Uplink are Hz.
type DmrTimeslot struct {
	Lcn      int   `json:"lcn"`
	Downlink int64 `json:"downlink"`
	Uplink   int64 `json:"uplink"`
}

// TunerSettings is per-SDR tuner config, keyed by device serial ("*" = all).
// Pointers distinguish "unset" from a real zero so we don't clobber a tuner
// with 0 gain/ppm the operator never set.
type TunerSettings struct {
	Serial     string         `json:"serial"`
	Label      string         `json:"label"`
	SampleRate float64        `json:"sampleRate"`
	Gain       *float64       `json:"gain"`
	AutoGain   bool           `json:"autoGain"`
	GainParams map[string]any `json:"gainParams"`
	PPM        *float64       `json:"ppm"`
	AutoPpm    bool           `json:"autoPpm"`
	Type       string         `json:"type"`
}

// StreamTarget is one agency system the node uploads for. Each gets a stable
// local API key.
type StreamTarget struct {
	SystemId int    `json:"systemId"`
	Name     string `json:"name"`
}

// AliasID is one <id> inside an SDR-Trunk alias. Type is the id type; Attrs
// holds every other attribute verbatim so the element re-emits faithfully.
type AliasID struct {
	Type  string            `json:"type"`
	Attrs map[string]string `json:"attrs"`
}

// Alias is a global SDR-Trunk alias, rendered into the playlist's <alias> region.
type Alias struct {
	Name string `json:"name"`
	List string `json:"list"`
	// Group is the alias-list group. Color is an ARGB int (stored as its string
	// form). IconName is the named-icon; StreamTalkgroupAlias is the "stream as
	// talkgroup" int — both re-emit as the iconName / stream_talkgroup_alias attrs.
	Group                string    `json:"group"`
	Color                string    `json:"color"`
	IconName             string    `json:"iconName"`
	StreamTalkgroupAlias flexStr   `json:"streamTalkgroupAlias"`
	IDs                  []AliasID `json:"ids"`
}

// flexStr unmarshals a JSON string OR number into its string form, so an alias
// field that can arrive as either (streamTalkgroupAlias) round-trips cleanly.
type flexStr string

func (f *flexStr) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "" || s == "null" {
		*f = ""
		return nil
	}
	if s[0] == '"' {
		var str string
		if err := json.Unmarshal(b, &str); err != nil {
			return err
		}
		*f = flexStr(str)
		return nil
	}
	*f = flexStr(s) // numeric literal — keep verbatim
	return nil
}

// ConfigPayload is the full configPush document from the backend.
type ConfigPayload struct {
	ConfigVersion string          `json:"configVersion"`
	Channels      []ChannelPlan   `json:"channels"`
	Tuners        []TunerSettings `json:"tuners"`
	Aliases       []Alias         `json:"aliases"`
	RdioConfig    map[string]any  `json:"rdioConfig"`
	StreamTargets []StreamTarget  `json:"streamTargets"`
}

// Restarter is the slice of the supervisor configapply needs (best-effort
// sdrtrunk restart when a live playlist reload fails).
type Restarter interface {
	Restart(name string) error
}

// Deps carries the collaborators + paths Apply needs, wired from main.
type Deps struct {
	DataDir         string          // agent data dir (holds keys.json, rdio-admin.secret)
	PresetsDir      string          // on-disk preset dir; falls back to embedded presets
	SDRTrunkAppRoot string          // playlist written to <root>/playlist/default.xml
	Rdio            *rdioctl.Client // local rdio admin client (127.0.0.1:17391)
	SDR             *sdrctl.Client  // SDR-Trunk control server client
	Supervisor      Restarter       // for best-effort sdrtrunk restart
	RdioPassword    string          // resolved local rdio admin password
}

// StageError identifies which apply stage failed, for a configError ack.
type StageError struct {
	Stage   string
	Message string
	Err     error
}

func (e *StageError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("config apply failed at stage %q: %s: %v", e.Stage, e.Message, e.Err)
	}
	return fmt.Sprintf("config apply failed at stage %q: %s", e.Stage, e.Message)
}

func (e *StageError) Unwrap() error { return e.Err }

func stageErr(stage, message string, err error) *StageError {
	return &StageError{Stage: stage, Message: message, Err: err}
}

// rdioLoginReady logs in to the local rdio admin API, tolerating the brief
// window right after a cold start where rdio hasn't bound its port yet
// ("connection refused" on Linux, "actively refused" on Windows). It retries
// only those dial-time failures; a real error (e.g. a wrong password) returns
// immediately so it isn't masked by the readiness wait.
func rdioLoginReady(c *rdioctl.Client, password string) error {
	const attempts = 30 // ~30s: process spawn + sqlite init on a cold node
	var err error
	for i := 0; i < attempts; i++ {
		if err = c.Login(password); err == nil {
			return nil
		}
		m := err.Error()
		if !strings.Contains(m, "refused") && !strings.Contains(m, "connection reset") {
			return err
		}
		time.Sleep(time.Second)
	}
	return err
}

// Apply runs the full config-apply pipeline for one payload. On any failure it
// returns a *StageError naming the failed stage.
func Apply(payload ConfigPayload, d Deps) error {
	// ---- stage: keys --------------------------------------------------------
	// Seed from the stream targets, then widen to every system id referenced by
	// the rdio apiKeys and the playlist streams so nothing ends up keyless.
	sysIDs := systemIDsFrom(payload)
	localKeys, err := keys.EnsureKeys(d.DataDir, sysIDs)
	if err != nil {
		return stageErr("keys", "ensure local api keys", err)
	}

	// ---- stage: rdio --------------------------------------------------------
	rdioCfg, err := d.resolveRdioConfig(payload)
	if err != nil {
		return stageErr("rdio", "load rdio config", err)
	}
	if err := applyRdioKeys(rdioCfg, localKeys); err != nil {
		return stageErr("rdio", "inject api keys", err)
	}
	if err := rdioLoginReady(d.Rdio, d.RdioPassword); err != nil {
		return stageErr("rdio", "admin login", err)
	}
	if err := d.Rdio.PutConfig(rdioCfg); err != nil {
		return stageErr("rdio", "put config", err)
	}

	// ---- stage: playlist ----------------------------------------------------
	tmpl := d.loadPlaylistTemplate()
	rendered, err := renderPlaylist(tmpl, payload.Channels, payload.Aliases, localKeys)
	if err != nil {
		return stageErr("playlist", "render", err)
	}
	if err := d.writePlaylist(rendered); err != nil {
		return stageErr("playlist", "write", err)
	}

	// ---- stage: reload ------------------------------------------------------
	liveReloaded := false
	if err := d.SDR.ReloadPlaylist(); err != nil {
		// Live reload failed. The new playlist file is already on disk, so a
		// process restart will pick it up on boot — request one best-effort.
		if d.Supervisor != nil {
			if rerr := d.Supervisor.Restart("sdrtrunk"); rerr != nil {
				return stageErr("reload", "playlist reload failed and sdrtrunk restart request failed", rerr)
			}
			// Restart requested; sdrtrunk will load the new playlist on startup.
		} else {
			return stageErr("reload", "playlist reload failed and no supervisor to restart sdrtrunk", err)
		}
	} else {
		liveReloaded = true
	}

	// A live playlist reload swaps the playlist model but does NOT re-apply the
	// new config to channels that were already running — they keep decoding with
	// their previous settings until bounced. So restart every started channel so
	// the edit actually takes effect. Skipped when we fell back to a process
	// restart above (that already loads everything fresh from the new playlist).
	if liveReloaded {
		d.restartRunningChannels()
	}

	// Tuner gain/ppm live in SDR-Trunk's tuner config, not the playlist, so they
	// are applied via the control API against the live tuners. Best-effort only:
	// failures here never fail the apply (the tuner may still be spinning up).
	d.applyTuners(payload.Tuners)

	return nil
}

// systemIDsFrom collects every system id referenced by the payload: the stream
// targets (authoritative), plus any ids appearing in the rdio apiKeys — so a
// downstream/apiKey never references a system without a persisted key.
func systemIDsFrom(payload ConfigPayload) []int {
	set := map[int]struct{}{}
	for _, t := range payload.StreamTargets {
		set[t.SystemId] = struct{}{}
	}
	if aks, ok := payload.RdioConfig["apiKeys"].([]any); ok {
		for _, e := range aks {
			if id, ok := apiKeySystemID(e); ok {
				set[id] = struct{}{}
			}
		}
	}
	out := make([]int, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Ints(out)
	return out
}

// resolveRdioConfig returns the rdio config to apply: the payload's document
// when present, otherwise the preset (on-disk then embedded) as a fallback.
func (d Deps) resolveRdioConfig(payload ConfigPayload) (map[string]any, error) {
	if len(payload.RdioConfig) > 0 {
		return payload.RdioConfig, nil
	}
	raw, err := os.ReadFile(filepath.Join(d.PresetsDir, "rdio-scanner.json"))
	if err != nil {
		raw = presets.RdioConfigJSON
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse rdio preset: %w", err)
	}
	return cfg, nil
}

// applyRdioKeys sets each apiKeys[i].key to the local key for its system id and
// replaces downstreams with a single entry pointing at the agent relay.
func applyRdioKeys(cfg map[string]any, localKeys map[int]string) error {
	aks, ok := cfg["apiKeys"].([]any)
	if !ok {
		return fmt.Errorf("rdio config has no apiKeys array")
	}
	for _, e := range aks {
		m, ok := e.(map[string]any)
		if !ok {
			continue
		}
		if id, ok := apiKeySystemID(e); ok {
			if k, ok := localKeys[id]; ok {
				m["key"] = k
			}
		}
	}

	// Single downstream -> the agent's relay listener. Shape mirrors the preset
	// downstream object (rdio-scanner.json): _id / apiKey / disabled / order /
	// systems / url. "systems":"*" is rdio's wildcard for "all systems".
	cfg["downstreams"] = []any{
		map[string]any{
			"_id":      1,
			"apiKey":   anyLocalKey(localKeys),
			"disabled": false,
			"order":    nil,
			"systems":  "*",
			"url":      relayDownstreamURL,
		},
	}
	return nil
}

// apiKeySystemID extracts the system id an apiKey entry grants access to, from
// its systems[0].id.
func apiKeySystemID(e any) (int, bool) {
	m, ok := e.(map[string]any)
	if !ok {
		return 0, false
	}
	sys, ok := m["systems"].([]any)
	if !ok || len(sys) == 0 {
		return 0, false
	}
	first, ok := sys[0].(map[string]any)
	if !ok {
		return 0, false
	}
	return toInt(first["id"])
}

// anyLocalKey returns a deterministic local key value (the lowest system id's
// key) for the downstream apiKey field — rdio only needs *a* valid local key
// there to authenticate the relay POST.
func anyLocalKey(localKeys map[int]string) string {
	ids := make([]int, 0, len(localKeys))
	for id := range localKeys {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return ""
	}
	sort.Ints(ids)
	return localKeys[ids[0]]
}

// loadPlaylistTemplate returns the preset playlist bytes: on-disk presets_dir
// first, embedded fallback otherwise.
func (d Deps) loadPlaylistTemplate() []byte {
	if b, err := os.ReadFile(filepath.Join(d.PresetsDir, "default.xml")); err == nil && len(b) > 0 {
		return b
	}
	return presets.DefaultPlaylistXML
}

// writePlaylist writes the rendered playlist to <appRoot>/playlist/default.xml
// atomically (temp + rename), creating the playlist dir if needed.
func (d Deps) writePlaylist(data []byte) error {
	dir := filepath.Join(d.SDRTrunkAppRoot, "playlist")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create playlist dir: %w", err)
	}
	path := filepath.Join(dir, "default.xml")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write temp playlist: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace playlist: %w", err)
	}
	return nil
}

// applyTuners pushes per-SDR gain/ppm to the live tuners via the control API.
// Settings are keyed by device serial; a "*" entry applies to every SDR. Live
// tuners are matched by serial when the control server reports one, else by the
// tuner id (loose) — precise serial matching + sampleRate/autoPpm land with the
// SDR-Trunk control-server changes (Phase D). Best-effort: all failures are
// swallowed since the playlist apply already succeeded.
func (d Deps) applyTuners(tuners []TunerSettings) {
	if len(tuners) == 0 || d.SDR == nil {
		return
	}
	live, err := d.SDR.Tuners()
	if err != nil {
		return
	}

	bySerial := make(map[string]TunerSettings, len(tuners))
	var wildcard *TunerSettings
	for i := range tuners {
		if tuners[i].Serial == "*" {
			wildcard = &tuners[i]
		} else if tuners[i].Serial != "" {
			bySerial[tuners[i].Serial] = tuners[i]
		}
	}

	for _, lt := range live {
		ts, ok := bySerial[lt.ID]
		if !ok {
			if wildcard == nil {
				continue
			}
			ts = *wildcard
		}
		if ts.SampleRate > 0 {
			_ = d.SDR.SetSampleRate(lt.ID, ts.SampleRate)
		}
		// Gain: auto/AGC wins; then an explicit multi-axis params object; then a
		// plain scalar. Mirrors the panel's Apply order so a restart reproduces
		// exactly what the operator last set live.
		if ts.AutoGain {
			_ = d.SDR.SetGainParams(lt.ID, map[string]any{"auto": true})
		} else if len(ts.GainParams) > 0 {
			_ = d.SDR.SetGainParams(lt.ID, ts.GainParams)
		} else if ts.Gain != nil {
			_ = d.SDR.SetGain(lt.ID, int(*ts.Gain))
		}
		if ts.AutoPpm {
			_ = d.SDR.SetAutoPPM(lt.ID, true)
		} else if ts.PPM != nil {
			// Manual PPM only when auto-ppm is off (auto would override it).
			_ = d.SDR.SetPPM(lt.ID, *ts.PPM)
		}
	}
}

// restartRunningChannels bounces (stop → start) every channel SDR-Trunk currently
// reports as processing, so a freshly-reloaded playlist's config takes effect on
// channels that were already running (a live reload leaves running channels on
// their old settings). Best-effort: failures are logged but never fail the apply.
// A short settle between stop and start lets the decode chain tear down before it
// is rebuilt, avoiding an "already processing" bounce.
func (d Deps) restartRunningChannels() {
	if d.SDR == nil {
		return
	}
	// A live playlist reload rebuilds the channel model asynchronously, so an
	// immediate Channels() can catch a transient moment where the list is empty or
	// channels momentarily report Processing=false — which would make us bounce
	// nothing and leave them on the old config. Retry briefly until we see at least
	// one processing channel (or give up after a short window).
	var channels []sdrctl.Channel
	for attempt := 0; attempt < 6; attempt++ {
		var err error
		channels, _, err = d.SDR.Channels()
		if err != nil {
			log.Printf("configapply: list channels for restart failed: %v", err)
			return
		}
		anyProcessing := false
		for _, ch := range channels {
			if ch.Processing {
				anyProcessing = true
				break
			}
		}
		if anyProcessing {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	for _, ch := range channels {
		if !ch.Processing {
			continue
		}
		if err := d.SDR.StopChannel(ch.ID); err != nil {
			log.Printf("configapply: stop channel %d (%s) for restart failed: %v", ch.ID, ch.Name, err)
			continue
		}
		time.Sleep(250 * time.Millisecond)
		if err := d.SDR.StartChannel(ch.ID); err != nil {
			log.Printf("configapply: restart channel %d (%s) failed: %v", ch.ID, ch.Name, err)
		}
	}
}

// --- playlist rendering (targeted string edits; see package doc) -------------

var (
	reAliasRegion  = regexp.MustCompile(`(?s)<alias\b.*</alias>`)
	reChannelBlock = regexp.MustCompile(`(?s)<channel\b.*?</channel>`)
	reChannelTag   = regexp.MustCompile(`(?s)<channel\b[^>]*>`)
	reSourceCfg    = regexp.MustCompile(`(?s)<source_configuration\b[^>]*?/>`)
	reStreamTag    = regexp.MustCompile(`(?s)<stream\b[^>]*>`)
	reEnabled      = regexp.MustCompile(`enabled="[^"]*"`)
	reSystemAttr   = regexp.MustCompile(`system="[^"]*"`)
	reSiteAttr     = regexp.MustCompile(`site="[^"]*"`)
	reNameAttr     = regexp.MustCompile(`name="[^"]*"`)
	reOrderAttr    = regexp.MustCompile(`order="[^"]*"`)
	// Matches the whole <decode_configuration> element — self-closing (`.../>`)
	// OR with children (`...>…</decode_configuration>`) — so it can be swapped
	// wholesale for the per-decoder rendering.
	reDecodeConfig = regexp.MustCompile(`(?s)<decode_configuration\b[^>]*?(?:/>|>.*?</decode_configuration>)`)
	reSystemID     = regexp.MustCompile(`system_id="([^"]*)"`)
	reApiKey       = regexp.MustCompile(`api_key="[^"]*"`)
	reHost         = regexp.MustCompile(`host="[^"]*"`)
)

// renderPlaylist clones the preset's single <channel> block once per configured
// channel (each with its own frequency / decoder / labels / order), then points
// every <stream>'s api_key + host at the local rdio. With no channels it keeps
// the template channel but disabled, so SDR-Trunk still loads a valid playlist.
// It never regenerates XML from scratch — see the package fidelity warning.
func renderPlaylist(template []byte, channels []ChannelPlan, aliases []Alias, localKeys map[int]string) ([]byte, error) {
	out := string(template)

	tmplBlock := reChannelBlock.FindString(out)
	if tmplBlock == "" {
		return nil, fmt.Errorf("preset has no <channel> element")
	}

	// Global aliases: replace the preset's contiguous <alias>…</alias> region
	// with the fleet-wide aliases. Only when some are provided — an empty global
	// alias set leaves the preset's aliases untouched. NOTE: aliases are
	// regenerated from structured data (not edited in place); attribute order/
	// whitespace may differ from the preset. SDR-Trunk (Jackson) reads attributes
	// by name so this is safe, but per the fidelity warning it should be spot-
	// checked on real SDR-Trunk.
	if len(aliases) > 0 && reAliasRegion.MatchString(out) {
		rendered := renderAliases(aliases)
		out = reAliasRegion.ReplaceAllStringFunc(out, func(string) string { return rendered })
	}

	// With no configured channels, emit NO <channel> element at all — an empty
	// channel set is a valid SDR-Trunk playlist, and this avoids surfacing a
	// non-removable "preset" channel in the UI. The template block is still the
	// clone source when channels ARE present.
	var rendered string
	if len(channels) > 0 {
		var b strings.Builder
		for i, ch := range channels {
			if i > 0 {
				b.WriteString("\n  ")
			}
			b.WriteString(renderChannelBlock(tmplBlock, ch, true))
		}
		rendered = b.String()
	}
	// Replace the single template block with the rendered block(s) (or "" to drop
	// it). Use a literal replacement (not ReplaceAllString) so `$` in the rendered
	// XML isn't treated as a capture reference.
	out = reChannelBlock.ReplaceAllStringFunc(out, func(string) string { return rendered })

	// Streams: point api_key + host at the local rdio (global, not per-channel).
	out = reStreamTag.ReplaceAllStringFunc(out, func(tag string) string {
		m := reSystemID.FindStringSubmatch(tag)
		key := ""
		if len(m) == 2 {
			if id, err := strconv.Atoi(strings.TrimSpace(m[1])); err == nil {
				key = localKeys[id]
			}
		}
		tag = reApiKey.ReplaceAllString(tag, `api_key="`+xmlAttr(key)+`"`)
		tag = reHost.ReplaceAllString(tag, `host="`+localRdioUploadURL+`"`)
		return tag
	})

	return []byte(out), nil
}

// renderChannelBlock stamps one channel's fields onto a clone of the preset's
// <channel> block: enabled flag, name/system/site/order attributes, decoder
// type, and the source frequency. When enabled is false the block is emitted
// disabled and the other fields are left as the template's.
func renderChannelBlock(tmpl string, ch ChannelPlan, enabled bool) string {
	blk := reChannelTag.ReplaceAllStringFunc(tmpl, func(tag string) string {
		if enabled {
			tag = reEnabled.ReplaceAllString(tag, `enabled="true"`)
		} else {
			tag = reEnabled.ReplaceAllString(tag, `enabled="false"`)
			return tag
		}
		if ch.Name != "" {
			tag = setOrAddChannelAttr(tag, reNameAttr, "name", ch.Name)
		}
		if ch.System != "" {
			tag = setOrAddChannelAttr(tag, reSystemAttr, "system", ch.System)
		}
		if ch.Site != "" {
			tag = setOrAddChannelAttr(tag, reSiteAttr, "site", ch.Site)
		}
		if ch.Order > 0 {
			tag = setOrAddChannelAttr(tag, reOrderAttr, "order", strconv.Itoa(ch.Order))
		}
		return tag
	})

	if !enabled {
		return blk
	}

	// Decoder config. Render the FULL <decode_configuration> element for this
	// decoder (correct @type + attributes + child elements), filling SDR-Trunk
	// defaults for any unset field, and swap the preset's element wholesale.
	if dc := renderDecodeConfig(ch.Decoder, ch.DecoderConfig); dc != "" {
		blk = reDecodeConfig.ReplaceAllStringFunc(blk, func(string) string { return dc })
	}

	if ch.Frequency > 0 {
		blk = reSourceCfg.ReplaceAllStringFunc(blk, func(string) string {
			return renderSource(ch.Frequency)
		})
	}
	return blk
}

// setOrAddChannelAttr replaces an attribute on the <channel> opening tag, or
// inserts it right after the element name when the preset omits it.
func setOrAddChannelAttr(tag string, re *regexp.Regexp, name, val string) string {
	attr := name + `="` + xmlAttr(val) + `"`
	if re.MatchString(tag) {
		return re.ReplaceAllString(tag, attr)
	}
	return strings.Replace(tag, "<channel", "<channel "+attr, 1)
}

// renderDecodeConfig builds the full <decode_configuration> element for a
// decoder from its (optional) DecoderConfig, filling SDR-Trunk defaults (per the
// config reference) for any unset field. Returns "" for an unknown decoder so
// the preset's element is left untouched.
func renderDecodeConfig(decoder string, cfg *DecoderConfig) string {
	if cfg == nil {
		cfg = &DecoderConfig{}
	}
	switch decoder {
	case "p25p1":
		mod := cfg.Modulation
		if mod == "" {
			mod = "C4FM"
		}
		return fmt.Sprintf(
			`<decode_configuration type="decodeConfigP25Phase1" modulation="%s" ignore_data_calls="%s" traffic_channel_pool_size="%d"/>`,
			xmlAttr(mod), boolOr(cfg.IgnoreDataCalls, false), intOr(cfg.TrafficPoolSize, 20))

	case "p25p2":
		// auto-detect defaults to true when no manual scramble is supplied.
		auto := cfg.Scramble == nil
		if cfg.AutoDetectScramble != nil {
			auto = *cfg.AutoDetectScramble
		}
		head := fmt.Sprintf(
			`<decode_configuration type="decodeConfigP25Phase2" auto_detect_scramble_parameters="%s" ignore_data_calls="%s" traffic_channel_pool_size="%d"`,
			strconv.FormatBool(auto), boolOr(cfg.IgnoreDataCalls, false), intOr(cfg.TrafficPoolSize, 20))
		if cfg.Scramble != nil {
			return head + ">" +
				fmt.Sprintf("\n    <scramble_parameters wacn=\"%d\" system=\"%d\" nac=\"%d\"/>",
					cfg.Scramble.Wacn, cfg.Scramble.System, cfg.Scramble.Nac) +
				"\n  </decode_configuration>"
		}
		return head + "/>"

	case "dmr":
		head := fmt.Sprintf(
			`<decode_configuration type="decodeConfigDMR" ignore_crc="%s" use_compressed_talkgroups="%s" ignore_data_calls="%s" traffic_channel_pool_size="%d"`,
			boolOr(cfg.IgnoreCrc, false), boolOr(cfg.UseCompressedTalkgroups, false),
			boolOr(cfg.IgnoreDataCalls, true), intOr(cfg.TrafficPoolSize, 20))
		if len(cfg.Timeslots) == 0 {
			return head + "/>"
		}
		var b strings.Builder
		b.WriteString(head)
		b.WriteString(">")
		for _, ts := range cfg.Timeslots {
			b.WriteString(fmt.Sprintf("\n    <timeslot lsn=\"%d\" downlink=\"%d\" uplink=\"%d\"/>",
				ts.Lcn, ts.Downlink, ts.Uplink))
		}
		b.WriteString("\n  </decode_configuration>")
		return b.String()

	case "nbfm":
		bw := cfg.Bandwidth
		if bw == "" {
			bw = "BW_12_5"
		}
		// The 4 squelch* fields are not user-facing; emit the doc defaults.
		return fmt.Sprintf(
			`<decode_configuration type="decodeConfigNBFM" bandwidth="%s" talkgroup="%d" audioFilter="%s" squelchNoiseOpenThreshold="0.1" squelchNoiseCloseThreshold="0.19" squelchHysteresisOpenThreshold="4" squelchHysteresisCloseThreshold="6"/>`,
			xmlAttr(bw), intOr(cfg.Talkgroup, 1), boolOr(cfg.AudioFilter, true))

	case "am":
		bw := cfg.Bandwidth
		if bw == "" {
			bw = "BW_15_0"
		}
		return fmt.Sprintf(
			`<decode_configuration type="decodeConfigAM" bandwidth="%s" talkgroup="%d" autoTrack="%s" squelch="%d"/>`,
			xmlAttr(bw), intOr(cfg.Talkgroup, 1), boolOr(cfg.AutoTrack, true), intOr(cfg.Squelch, -78))

	default:
		return ""
	}
}

// boolOr formats *p as an XML bool, using def when p is nil (unset).
func boolOr(p *bool, def bool) string {
	v := def
	if p != nil {
		v = *p
	}
	return strconv.FormatBool(v)
}

// intOr returns *p, or def when p is nil (unset).
func intOr(p *int, def int) int {
	if p != nil {
		return *p
	}
	return def
}

// renderSource builds the channel's <source_configuration> for a single tuner
// frequency, preserving the preset's sourceConfigTuner shape.
func renderSource(freq int64) string {
	return fmt.Sprintf(`<source_configuration type="sourceConfigTuner" source_type="TUNER" frequency="%d"/>`, freq)
}

// renderAliases renders the global aliases into the playlist's <alias> region.
// Attributes are emitted in SDR-Trunk's conventional order; extra <id>
// attributes are sorted for deterministic output (Jackson reads by name).
func renderAliases(aliases []Alias) string {
	var b strings.Builder
	for i, a := range aliases {
		if i > 0 {
			b.WriteString("\n  ")
		}
		b.WriteString(renderAlias(a))
	}
	return b.String()
}

func renderAlias(a Alias) string {
	var b strings.Builder
	b.WriteString("<alias")
	writeXMLAttr(&b, "color", a.Color)
	writeXMLAttr(&b, "list", a.List)
	writeXMLAttr(&b, "group", a.Group)
	writeXMLAttr(&b, "name", a.Name)
	writeXMLAttr(&b, "iconName", a.IconName)
	// "Stream As Talkgroup" OVERRIDES the decoded talkgroup in SDR-Trunk's
	// RdioScanner uploader (getTo() returns this value verbatim). A 0/blank here
	// would force every matching call to upload as talkgroup 0, which rdio drops
	// as "Incomplete call data: no talkgroup". Only emit a real, positive value.
	if sta := strings.TrimSpace(string(a.StreamTalkgroupAlias)); sta != "" && sta != "0" {
		writeXMLAttr(&b, "stream_talkgroup_alias", sta)
	}
	if len(a.IDs) == 0 {
		b.WriteString("/>")
		return b.String()
	}
	b.WriteString(">")
	for _, id := range a.IDs {
		b.WriteString("\n    <id")
		writeXMLAttr(&b, "type", id.Type)
		keys := make([]string, 0, len(id.Attrs))
		for k := range id.Attrs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			writeXMLAttr(&b, k, id.Attrs[k])
		}
		b.WriteString("/>")
	}
	b.WriteString("\n  </alias>")
	return b.String()
}

// writeXMLAttr appends ` name="escaped-value"` when value is non-empty.
func writeXMLAttr(b *strings.Builder, name, val string) {
	if val == "" {
		return
	}
	b.WriteString(" ")
	b.WriteString(name)
	b.WriteString(`="`)
	b.WriteString(xmlAttr(val))
	b.WriteString(`"`)
}

// xmlAttr escapes a string for use inside a double-quoted XML attribute,
// matching the entity style SDR-Trunk's writer emits (e.g. & -> &amp;).
func xmlAttr(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
	)
	return r.Replace(s)
}

// toInt coerces a JSON-decoded numeric/string value to int.
func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(n))
		return i, err == nil
	default:
		return 0, false
	}
}

// toFloat coerces a JSON-decoded numeric/string value to float64.
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, err == nil
	default:
		return 0, false
	}
}
