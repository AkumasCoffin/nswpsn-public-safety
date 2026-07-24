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
}

// TunerSettings is per-SDR tuner config, keyed by device serial ("*" = all).
// Pointers distinguish "unset" from a real zero so we don't clobber a tuner
// with 0 gain/ppm the operator never set.
type TunerSettings struct {
	Serial     string   `json:"serial"`
	Label      string   `json:"label"`
	SampleRate float64  `json:"sampleRate"`
	Gain       *float64 `json:"gain"`
	PPM        *float64 `json:"ppm"`
	AutoPpm    bool     `json:"autoPpm"`
	Type       string   `json:"type"`
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
	Name  string    `json:"name"`
	List  string    `json:"list"`
	Group string    `json:"group"`
	Color string    `json:"color"`
	IDs   []AliasID `json:"ids"`
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
		if ts.Gain != nil {
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
	reDecodeType   = regexp.MustCompile(`(<decode_configuration\b[^>]*\btype=")[^"]*(")`)
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

	var rendered string
	if len(channels) == 0 {
		rendered = renderChannelBlock(tmplBlock, ChannelPlan{}, false)
	} else {
		var b strings.Builder
		for i, ch := range channels {
			if i > 0 {
				b.WriteString("\n  ")
			}
			b.WriteString(renderChannelBlock(tmplBlock, ch, true))
		}
		rendered = b.String()
	}
	// Replace the single template block with the rendered block(s). Use a literal
	// replacement (not ReplaceAllString) so `$` in the rendered XML isn't treated
	// as a capture reference.
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

	// Decoder type. NOTE: only the type attribute is swapped; the preset's other
	// decode_configuration attributes (modulation, traffic_channel_pool_size) are
	// kept. Non-P25 decoders may need a different attribute set — this is
	// best-effort and MUST be validated against real SDR-Trunk (fidelity warning).
	if dt := decodeConfigType(ch.Decoder); dt != "" {
		blk = reDecodeType.ReplaceAllString(blk, `${1}`+dt+`${2}`)
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

// decodeConfigType maps a payload decoder key to SDR-Trunk's
// decode_configuration @type. Empty = leave the preset's type untouched.
func decodeConfigType(decoder string) string {
	switch decoder {
	case "p25p1":
		return "decodeConfigP25Phase1"
	case "p25p2":
		return "decodeConfigP25Phase2"
	case "dmr":
		return "decodeConfigDMR"
	case "nbfm":
		return "decodeConfigNBFM"
	case "am":
		return "decodeConfigAM"
	default:
		return ""
	}
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
