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

// ChannelPlan is the SDR-Trunk side of a configPush: which control channel to
// decode and how to drive the tuner.
type ChannelPlan struct {
	System             string         `json:"system"`
	SiteName           string         `json:"siteName"`
	ControlFrequencies []int64        `json:"controlFrequencies"`
	Tuner              map[string]any `json:"tuner"`
}

// StreamTarget is one agency system the node uploads for. Each gets a stable
// local API key.
type StreamTarget struct {
	SystemId int    `json:"systemId"`
	Name     string `json:"name"`
}

// ConfigPayload is the full configPush document from the backend.
type ConfigPayload struct {
	ConfigVersion string         `json:"configVersion"`
	ChannelPlan   ChannelPlan    `json:"channelPlan"`
	RdioConfig    map[string]any `json:"rdioConfig"`
	StreamTargets []StreamTarget `json:"streamTargets"`
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
	if err := d.Rdio.Login(d.RdioPassword); err != nil {
		return stageErr("rdio", "admin login", err)
	}
	if err := d.Rdio.PutConfig(rdioCfg); err != nil {
		return stageErr("rdio", "put config", err)
	}

	// ---- stage: playlist ----------------------------------------------------
	tmpl := d.loadPlaylistTemplate()
	rendered, err := renderPlaylist(tmpl, payload.ChannelPlan, localKeys)
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
	d.applyTunerParams(payload.ChannelPlan.Tuner)

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

// applyTunerParams pushes gain/ppm from the channel plan to every live tuner via
// the control API. Best-effort: all failures are swallowed (logged by callers of
// the control API is not available here, so we stay silent) since the playlist
// apply already succeeded.
func (d Deps) applyTunerParams(tuner map[string]any) {
	if len(tuner) == 0 || d.SDR == nil {
		return
	}
	gain, hasGain := toInt(tuner["gain"])
	ppm, hasPPM := toFloat(tuner["ppm"])
	if !hasGain && !hasPPM {
		return
	}
	tuners, err := d.SDR.Tuners()
	if err != nil {
		return
	}
	for _, t := range tuners {
		if hasGain {
			_ = d.SDR.SetGain(t.ID, gain)
		}
		if hasPPM {
			_ = d.SDR.SetPPM(t.ID, ppm)
		}
	}
}

// --- playlist rendering (targeted string edits; see package doc) -------------

var (
	reChannelTag = regexp.MustCompile(`(?s)<channel\b[^>]*>`)
	reSourceCfg  = regexp.MustCompile(`(?s)<source_configuration\b[^>]*?/>`)
	reStreamTag  = regexp.MustCompile(`(?s)<stream\b[^>]*>`)
	reEnabled    = regexp.MustCompile(`enabled="[^"]*"`)
	reSystemAttr = regexp.MustCompile(`system="[^"]*"`)
	reSiteAttr   = regexp.MustCompile(`site="[^"]*"`)
	reSystemID   = regexp.MustCompile(`system_id="([^"]*)"`)
	reApiKey     = regexp.MustCompile(`api_key="[^"]*"`)
	reHost       = regexp.MustCompile(`host="[^"]*"`)
)

// renderPlaylist applies the narrow edits described in the package doc to the
// preset template and returns the node-specific playlist bytes.
func renderPlaylist(template []byte, plan ChannelPlan, localKeys map[int]string) ([]byte, error) {
	out := string(template)

	if !reChannelTag.MatchString(out) {
		return nil, fmt.Errorf("preset has no <channel> element")
	}

	// 1) Channel opening tag: enable it, and stamp the node's system/site labels.
	out = reChannelTag.ReplaceAllStringFunc(out, func(tag string) string {
		tag = reEnabled.ReplaceAllString(tag, `enabled="true"`)
		if plan.System != "" {
			tag = reSystemAttr.ReplaceAllString(tag, `system="`+xmlAttr(plan.System)+`"`)
		}
		if plan.SiteName != "" {
			tag = reSiteAttr.ReplaceAllString(tag, `site="`+xmlAttr(plan.SiteName)+`"`)
		}
		// The preset's order="N" attribute is SDR-Trunk's auto-start order; its
		// presence already marks the channel for auto-start, so enabling is
		// sufficient. (Left untouched to preserve auto-start ordering.)
		return tag
	})

	// 2) Source frequency(ies).
	if len(plan.ControlFrequencies) > 0 {
		if !reSourceCfg.MatchString(out) {
			return nil, fmt.Errorf("preset channel has no self-closing <source_configuration>")
		}
		replaced := false
		out = reSourceCfg.ReplaceAllStringFunc(out, func(string) string {
			replaced = true
			return renderSource(plan.ControlFrequencies)
		})
		if !replaced {
			return nil, fmt.Errorf("could not substitute <source_configuration>")
		}
	}

	// 3) Streams: point api_key + host at the local rdio.
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

// renderSource builds the channel <source_configuration> element. A single
// control frequency keeps the preset's sourceConfigTuner shape (adding a
// frequency attribute); multiple frequencies emit sourceConfigTunerMultiple with
// child <frequency> elements. The multi-frequency element shape is a best-effort
// match to SDR-Trunk's schema — see the package-level fidelity warning.
func renderSource(freqs []int64) string {
	if len(freqs) == 1 {
		return fmt.Sprintf(`<source_configuration type="sourceConfigTuner" source_type="TUNER" frequency="%d"/>`, freqs[0])
	}
	var b strings.Builder
	b.WriteString(`<source_configuration type="sourceConfigTunerMultiple" source_type="TUNER">`)
	for _, f := range freqs {
		b.WriteString("\n      <frequency>")
		b.WriteString(strconv.FormatInt(f, 10))
		b.WriteString("</frequency>")
	}
	b.WriteString("\n    </source_configuration>")
	return b.String()
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
