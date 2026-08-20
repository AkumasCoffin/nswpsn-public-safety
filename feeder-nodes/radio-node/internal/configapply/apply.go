// Package configapply turns a backend configPush payload into concrete local
// state on the node: stable per-agency API keys, a full rdio-scanner config PUT
// (apiKeys + a single downstream pointing at the agent's relay), and a full
// sdrtrunk-vce configuration import.
//
// The supervised SDR-Trunk build is the sdrtrunk-vce fork, whose configuration
// lives in a SQLite database rather than a playlist XML file. The agent
// therefore never writes a playlist: it builds the vce ConfigurationState JSON
// document (see vceconfig.go for the ground-truthed shapes) and POSTs it to the
// control server's /config/import — a full-overwrite, idempotent operation that
// also (re)starts auto-start channels itself.
package configapply

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
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
	// Extra control frequencies for the same site, Hz. Frequency above stays
	// the primary and is always required — SDR-Trunk has to lock onto one
	// before it can learn a site's alternates. With any extras present the
	// renderer switches to vce's multiple-frequency source config.
	Frequencies []int64 `json:"frequencies,omitempty"`
	Decoder     string  `json:"decoder"` // p25p2|p25p1|dmr|nbfm|am
	System      string  `json:"system"`
	Site        string  `json:"site"`
	AutoStart   bool    `json:"autoStart"`
	Order       int     `json:"order"`
	SDR         string  `json:"sdr"` // device serial to pin to; "" = any tuner
	// DecoderConfig carries decoder-specific settings; nil = all defaults. The
	// vce config builder emits the matching decodeConfiguration and fills
	// SDR-Trunk defaults for any unset field.
	DecoderConfig *DecoderConfig `json:"decoderConfig"`
}

// DecoderConfig is a loose superset of every decoder's settings (see the
// SDR-Trunk config reference "decoderConfig JSON contract"). Which fields apply
// is decided by ChannelPlan.Decoder at render time. Pointers distinguish "unset
// → use SDR-Trunk default" from a real zero/false value.
type DecoderConfig struct {
	// p25p1
	Modulation string `json:"modulation"` // "C4FM" | "CQPSK"
	// p25 (phase1/phase2): learn alternate control channels (default on) so only
	// one control frequency needs configuring per site.
	LearnControlChannels *bool `json:"learnControlChannels"`
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
	// The rdio apiKey for this system, generated server-side. Used on BOTH the
	// sdrtrunk stream and the matching rdio apiKey so uploads route. When empty
	// (older server), the agent falls back to a locally-minted key.
	Key string `json:"key"`
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
	// Node on/off (capture) + feed on/off. POINTERS so a nil (an older payload,
	// e.g. a persisted applied-config.json from before these fields existed,
	// re-rendered at startup) means "true" — a plain bool's zero value would
	// falsely disable capture/feed.
	CaptureEnabled *bool `json:"captureEnabled"`
	FeedEnabled    *bool `json:"feedEnabled"`
}

// captureOn reports whether capture (decoding) is enabled. nil = on.
func (p ConfigPayload) captureOn() bool { return p.CaptureEnabled == nil || *p.CaptureEnabled }

// feedOn reports whether feeding (rdio downstream upload) is enabled. nil = on.
func (p ConfigPayload) feedOn() bool { return p.FeedEnabled == nil || *p.FeedEnabled }

// effectiveChannels returns the channels to render/reconcile. When capture is OFF
// (Node off), every channel is forced auto-start=false so the playlist renders
// enabled="false" and the reconcile stops them all — SDR-Trunk stays up but
// decodes nothing, and the agent stays connected.
func (p ConfigPayload) effectiveChannels() []ChannelPlan {
	if p.captureOn() {
		return p.Channels
	}
	out := make([]ChannelPlan, len(p.Channels))
	for i, ch := range p.Channels {
		ch.AutoStart = false
		out[i] = ch
	}
	return out
}

// Restarter is the slice of the supervisor configapply needs (best-effort
// sdrtrunk restart; retained for the WS-client wiring even though the vce
// import path no longer restarts sdrtrunk itself).
type Restarter interface {
	Restart(name string) error
}

// Deps carries the collaborators + paths Apply needs, wired from main.
type Deps struct {
	DataDir         string          // agent data dir (holds keys.json, rdio-admin.secret)
	PresetsDir      string          // on-disk preset dir; falls back to embedded presets
	SDRTrunkAppRoot string          // sdrtrunk-vce app root (--app-root; DB at <root>/database/sdrtrunk.sqlite)
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

// Apply runs the config-apply pipeline for one payload. The sdrtrunk-vce config
// import and the rdio-scanner config are applied INDEPENDENTLY, and sdrtrunk is
// applied FIRST: the imported config governs channel auto-start, so it must be
// attempted even if the rdio config PUT fails (e.g. the local rdio admin API
// times out on a large config). Returns a combined error carrying each failed
// stage's *StageError, but only after BOTH stages were attempted.
func Apply(payload ConfigPayload, d Deps) error {
	// ---- stage: keys (hard requirement — both the vce streams and rdio need them)
	// Seed from the stream targets, then widen to every system id referenced by
	// the rdio apiKeys so nothing ends up keyless.
	sysIDs := systemIDsFrom(payload)
	localKeys, err := keys.EnsureKeys(d.DataDir, sysIDs)
	if err != nil {
		return stageErr("keys", "ensure local api keys", err)
	}

	var errs []error

	// sdrtrunk-vce config FIRST — a flaky rdio must not prevent the channel/
	// alias/stream import (which governs what actually decodes).
	if perr := d.applySdrtrunkConfig(payload, localKeys); perr != nil {
		errs = append(errs, perr)
	}

	// rdio config is independent: a PutConfig timeout here must NOT prevent the
	// sdrtrunk import above.
	if rerr := d.applyRdio(payload, localKeys); rerr != nil {
		errs = append(errs, rerr)
	}

	return errors.Join(errs...)
}

// ImportOnBoot re-imports the last-applied payload into a freshly-launched
// sdrtrunk-vce. Called at agent startup AFTER the control server answers
// /status, so sdrtrunk always runs the agent's current config regardless of
// what its SQLite database held from the last session. Idempotent (the import
// is a full overwrite). Needs only DataDir/PresetsDir/SDR on Deps.
func ImportOnBoot(payload ConfigPayload, d Deps) error {
	sysIDs := systemIDsFrom(payload)
	localKeys, err := keys.EnsureKeys(d.DataDir, sysIDs)
	if err != nil {
		return stageErr("keys", "ensure local api keys", err)
	}
	state := buildVceConfig(payload, localKeys, d.presetAliasListName())
	body, err := json.Marshal(state)
	if err != nil {
		return stageErr("playlist", "encode vce config", err)
	}

	// Tuner settings must be (re)applied on boot, BEFORE the import starts
	// channels — same ordering and same reason as applySdrtrunkConfig.
	//
	// SDR-Trunk restores its channels from its own database at startup but does
	// NOT restore what the operator set here: a tuner comes up reporting
	// gain:null, i.e. AGC was never actually engaged, not "AGC is on". Boot used
	// to import the config and return without ever touching the tuners, so the
	// node ran on whatever gain the hardware powered up with and decoded poorly
	// (channels IDLE, or CONTROL at 0-20%) until someone opened the panel and
	// hit Apply — which was the ONLY thing that ever sent the gain. Hence
	// "applying SDR settings fixes it" after every single restart.
	//
	// Best-effort, matching Apply: a tuner failure must not stop the playlist
	// import, or a single bad SDR would leave the node with no channels at all.
	d.applyTuners(payload.Tuners)

	if err := d.importWithRetry(body); err != nil {
		return stageErr("playlist", "import config", err)
	}
	return nil
}

// HasStage reports whether err (possibly an errors.Join of several) carries a
// *StageError for the named stage. Lets the caller tell "the playlist stage
// succeeded but rdio failed" apart from a playlist failure.
func HasStage(err error, stage string) bool {
	for _, e := range flatten(err) {
		var se *StageError
		if errors.As(e, &se) && se.Stage == stage {
			return true
		}
	}
	return false
}

func flatten(err error) []error {
	if err == nil {
		return nil
	}
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		var out []error
		for _, e := range j.Unwrap() {
			out = append(out, flatten(e)...)
		}
		return out
	}
	return []error{err}
}

// applySdrtrunkConfig builds the vce ConfigurationState from the payload and
// POSTs it to the control server's /config/import (full overwrite). The import
// itself (re)starts auto-start channels and stops removed ones, so no explicit
// reload/bounce pass is needed. Independent of the rdio stage.
//
// The stage name stays "playlist" so the backend's configError acks and the
// WS client's persist-on-partial-failure logic keep working unchanged.
func (d Deps) applySdrtrunkConfig(payload ConfigPayload, localKeys map[int]string) error {
	// Effective channels honour Node on/off: capture-off forces every channel
	// auto-start=false so the import brings nothing up AND the backstop below
	// stops anything still running.
	chans := payload.effectiveChannels()
	state := buildVceConfig(payload, localKeys, d.presetAliasListName())
	body, err := json.Marshal(state)
	if err != nil {
		return stageErr("playlist", "encode vce config", err)
	}

	// Tuner settings go on BEFORE the import. The import (re)starts auto-start
	// channels, and a channel sourcing a tuner LOCKS it — after which SDR-Trunk
	// refuses a sample-rate change outright ("Cannot change sample rate while
	// tuner is is LOCKED state") and the setting can never land. Applied here
	// the tuner is still free, so a genuine rate change takes effect on the
	// first apply instead of being retried and rejected on every one.
	// Best-effort: failures never fail the apply.
	d.applyTuners(payload.Tuners)

	if err := d.importWithRetry(body); err != nil {
		return stageErr("playlist", "import config", err)
	}

	// AUTHORITATIVE BACKSTOP: regardless of what the import's own channel
	// reconciliation does, guarantee that a channel the config marks
	// auto-start=off (incl. ALL channels when capture is off) is not left
	// decoding.
	d.enforceDisabledChannels(chans)

	return nil
}

// importWithRetry POSTs the ConfigurationState body to /config/import, retrying
// a couple of times with backoff: right after a (re)launch the control server
// may not be listening yet, and the import is idempotent so re-sending is safe.
func (d Deps) importWithRetry(body []byte) error {
	if d.SDR == nil {
		return fmt.Errorf("no sdrtrunk control client")
	}
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*2) * time.Second)
		}
		var res sdrctl.ImportResult
		res, err = d.SDR.ImportConfig(body)
		if err == nil {
			log.Printf("configapply: vce config imported (channels=%d aliases=%d streams=%d)",
				res.Channels, res.Aliases, res.Streams)
			return nil
		}
		log.Printf("configapply: config import attempt %d failed: %v", attempt+1, err)
	}
	return err
}

// applyRdio pushes the rdio-scanner config (systems/talkgroups + injected local api
// keys + the downstream to the agent relay). Independent of the playlist stage.
//
// The config is written DIRECTLY into rdio-scanner's own SQLite database rather
// than PUT to its HTTP admin endpoint (/api/admin/config), which hangs/EOFs/times
// out on the operator's node even at a 120s ceiling when applying the ~120KB
// document. The write is a single reconciling transaction (upsert by natural key
// + delete orphans), then rdio is bounced via the supervisor so it reloads the
// config at boot — rdio only reads its config on startup. This mirrors the
// rock-solid vce side, which also writes its DB directly. The desired document is
// built exactly as before (resolveRdioConfig + applyRdioKeys); only the SINK
// changed from HTTP to SQLite.
func (d Deps) applyRdio(payload ConfigPayload, localKeys map[int]string) error {
	rdioCfg, err := d.resolveRdioConfig(payload)
	if err != nil {
		return stageErr("rdio", "load rdio config", err)
	}
	if err := applyRdioKeys(rdioCfg, localKeys, payload.feedOn()); err != nil {
		return stageErr("rdio", "inject api keys", err)
	}
	dbPath := filepath.Join(d.DataDir, "rdio", "rdio-scanner.db")
	if err := rdioctl.WriteConfigDB(dbPath, rdioCfg); err != nil {
		return stageErr("rdio", "write config db", err)
	}
	// rdio only reads its config at boot, so a restart is required for a CHANGED
	// config to take effect. Bounce rdio ONLY when the rdio config actually changed
	// since the last apply — a channel-only edit (very common while the operator
	// tunes the node) doesn't touch rdio config, and killing+restarting rdio on
	// every apply drops in-flight calls and thrashes it. The DB write above is
	// idempotent and runs every time, so rdio's DB stays correct regardless.
	if d.rdioConfigChanged(rdioCfg) && d.Supervisor != nil {
		if rerr := d.Supervisor.Restart("rdio"); rerr != nil {
			log.Printf("configapply: rdio config written but restart failed (config persisted; loads on next boot): %v", rerr)
		}
	}
	return nil
}

// rdioConfigChanged reports whether the rdio config differs from the one that last
// triggered a bounce, recording the new signature when it does. The signature is a
// sha256 over encoding/json's output — Go sorts map keys, so it's stable across
// applies — meaning a channel-only edit (identical rdio config) won't bounce rdio.
// Best-effort: a marshal error or a missing signature file returns true (bounce), so
// a genuine change is never silently skipped.
func (d Deps) rdioConfigChanged(rdioCfg map[string]any) bool {
	body, err := json.Marshal(rdioCfg)
	if err != nil {
		return true
	}
	sum := sha256.Sum256(body)
	sig := hex.EncodeToString(sum[:])
	sigPath := filepath.Join(d.DataDir, "rdio", "rdio-config.sig")
	if prev, rerr := os.ReadFile(sigPath); rerr == nil && strings.TrimSpace(string(prev)) == sig {
		return false
	}
	// Persist the new signature (the rdio dir already exists — WriteConfigDB opened
	// the DB there). Ignore write errors: worst case is an extra bounce next time.
	_ = os.WriteFile(sigPath, []byte(sig), 0o644)
	return true
}

// enrichRdioRowIDs copies the `_id` (rowid) of each existing rdio config entry
// onto the matching outgoing entry so rdio-scanner UPDATEs it instead of
// INSERTing a duplicate. Systems/groups/tags match on their `id`; apiKeys match
// on their `key`. Entries with no match keep no `_id` and are inserted as new.
func enrichRdioRowIDs(cfg, cur map[string]any) {
	byKey := func(list any, key string) map[string]any {
		out := map[string]any{}
		arr, ok := list.([]any)
		if !ok {
			return out
		}
		for _, e := range arr {
			m, ok := e.(map[string]any)
			if !ok {
				continue
			}
			if _, hasID := m["_id"]; !hasID {
				continue
			}
			if kv, ok := m[key]; ok && kv != nil {
				out[fmt.Sprintf("%v", kv)] = m["_id"]
			}
		}
		return out
	}
	stamp := func(section, key string) {
		rows := byKey(cur[section], key)
		if len(rows) == 0 {
			return
		}
		arr, ok := cfg[section].([]any)
		if !ok {
			return
		}
		for _, e := range arr {
			m, ok := e.(map[string]any)
			if !ok {
				continue
			}
			if kv, ok := m[key]; ok && kv != nil {
				if rid, ok := rows[fmt.Sprintf("%v", kv)]; ok {
					m["_id"] = rid
				}
			}
		}
	}
	stamp("systems", "id")
	stamp("apiKeys", "key")
	stamp("groups", "id")
	stamp("tags", "id")
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
func applyRdioKeys(cfg map[string]any, localKeys map[int]string, feed bool) error {
	aks, ok := cfg["apiKeys"].([]any)
	if !ok {
		return fmt.Errorf("rdio config has no apiKeys array")
	}
	// Only FILL empty keys from the locally-minted set — a non-empty key set by
	// the server is authoritative (it's identical to the sdrtrunk stream's key),
	// so overwriting it would break the stream↔rdio match. downstreamKey tracks a
	// valid non-empty apiKey to authenticate the relay POST.
	downstreamKey := ""
	for _, e := range aks {
		m, ok := e.(map[string]any)
		if !ok {
			continue
		}
		if cur, _ := m["key"].(string); cur == "" {
			if id, ok := apiKeySystemID(e); ok {
				if k, ok := localKeys[id]; ok {
					m["key"] = k
				}
			}
		}
		if downstreamKey == "" {
			if cur, _ := m["key"].(string); cur != "" {
				downstreamKey = cur
			}
		}
	}
	if downstreamKey == "" {
		downstreamKey = anyLocalKey(localKeys)
	}

	// Single downstream -> the agent's relay listener. Shape mirrors the preset
	// downstream object (rdio-scanner.json): _id / apiKey / disabled / order /
	// systems / url. "systems":"*" is rdio's wildcard for "all systems".
	// Feed off → downstream disabled: rdio keeps running + keeps its config but
	// stops uploading to the agent relay. The apiKey must be a VALID rdio apiKey.
	cfg["downstreams"] = []any{
		map[string]any{
			"_id":      1,
			"apiKey":   downstreamKey,
			"disabled": !feed,
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

// How long applyTuners waits for vce to report at least one tuner before
// giving up. ~30s total: vce's own headless startup allows the same for USB
// enumeration, so this must not expire first.
const (
	tunerWaitAttempts = 30
	tunerWaitInterval = time.Second
)

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
	// Wait (briefly) for vce to enumerate its USB tuners. On boot the agent
	// races that enumeration: vce needs several seconds after start to discover
	// tuners, and until it has, /tuners returns an EMPTY list — not an error. The
	// loop below would then simply find nothing to do and return silently,
	// leaving every tuner on whatever gain the hardware powered up with. That is
	// the "I have to apply SDR settings after every restart" case: the settings
	// were never sent, and nothing said so.
	//
	// Bounded and best-effort: a node genuinely running without an SDR must not
	// stall the config apply, so give up after the deadline and carry on.
	var live []sdrctl.Tuner
	var err error
	for attempt := 0; attempt < tunerWaitAttempts; attempt++ {
		live, err = d.SDR.Tuners()
		if err == nil && len(live) > 0 {
			break
		}
		if attempt < tunerWaitAttempts-1 {
			time.Sleep(tunerWaitInterval)
		}
	}
	if err != nil {
		log.Printf("configapply: tuner settings skipped — control server unreachable: %v", err)
		return
	}
	if len(live) == 0 {
		log.Printf("configapply: tuner settings skipped — no tuners reported after %s",
			time.Duration(tunerWaitAttempts)*tunerWaitInterval)
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
		// Sample rate: ONLY when it actually differs from what the tuner is
		// already running. SDR-Trunk tears down and rebuilds the polyphase
		// channelizer on any rate call — including one that sets the rate the
		// tuner already has — and that drops every traffic channel sourced from
		// it. Re-importing an unchanged config therefore used to churn the whole
		// node for no reason. Tolerance is 1 Hz: these are discrete hardware
		// rates, so anything closer than that is the same rate.
		if ts.SampleRate > 0 && math.Abs(lt.SampleRate-ts.SampleRate) > 1 {
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

// enforceDisabledChannels guarantees no channel whose config sets AutoStart=false
// is left processing after an apply. It is the authoritative backstop that makes
// "disable" independent of the running SDR-Trunk build: regardless of what that
// build's /playlist/reload does (older builds may not stop an already-running
// channel), or whether a fallback process restart fully took effect, the agent
// explicitly stops any disabled channel that is still — or comes back — running.
//
// Matched by channel name (the control server reports channel.getName(), which is
// exactly what the agent writes into the playlist). Bounded watch (~6s): a reload
// or restart brings channels up asynchronously, so a disabled channel can begin
// processing a beat after Apply returns; two consecutive clean passes end it early.
func (d Deps) enforceDisabledChannels(want []ChannelPlan) {
	if d.SDR == nil {
		return
	}
	disabled := make(map[string]bool, len(want))
	for _, ch := range want {
		// TRIMMED name so a whitespace mismatch with
		// SDR-Trunk's getName() can't blind this backstop.
		if n := strings.TrimSpace(ch.Name); n != "" && !ch.AutoStart {
			disabled[n] = true
		}
	}
	if len(disabled) == 0 {
		return
	}
	clean := 0
	for attempt := 0; attempt < 12 && clean < 2; attempt++ {
		channels, _, err := d.SDR.Channels()
		if err != nil {
			// Control server may be mid-restart; give it a moment and retry.
			time.Sleep(500 * time.Millisecond)
			continue
		}
		foundRunning := false
		for _, ch := range channels {
			if ch.Processing && disabled[strings.TrimSpace(ch.Name)] {
				foundRunning = true
				if err := d.SDR.StopChannel(ch.ID); err != nil {
					log.Printf("configapply: enforce-stop disabled channel %d (%s) failed: %v", ch.ID, ch.Name, err)
				} else {
					log.Printf("configapply: enforce-stopped disabled channel %q (auto-start off)", ch.Name)
				}
			}
		}
		if foundRunning {
			clean = 0
		} else {
			clean++
		}
		time.Sleep(500 * time.Millisecond)
	}
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
