// Package sdrctl is a client for the SDR-Trunk headless control server the
// agent launches alongside the sdrtrunk child process. It speaks REST over
// http://127.0.0.1:<P> for status/tuner/channel/playlist control and a
// WebSocket over ws://127.0.0.1:<P+1> for the live spectrum stream. Every
// request carries the per-boot bearer token the agent generated and passed to
// sdrtrunk via SDRTRUNK_CONTROL_TOKEN.
package sdrctl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const restTimeout = 4 * time.Second

// importTimeout bounds POST /config/import: a full-overwrite config import
// (re)starts channels inside sdrtrunk-vce, which can take far longer than the
// default REST timeout on a loaded node.
const importTimeout = 60 * time.Second

// Status mirrors GET /status.
type Status struct {
	Version    string  `json:"version"`
	UptimeMs   int64   `json:"uptimeMs"`
	Headless   bool    `json:"headless"`
	Tuners     int     `json:"tuners"`
	Channels   int     `json:"channels"`
	Processing int     `json:"processing"`
	CPULoad    float64 `json:"cpuLoad"`
	Cores      int     `json:"cores"`
	MemUsedMB  int     `json:"memUsedMB"`
	MemMaxMB   int     `json:"memMaxMB"`
	// Node readiness (0.6.7+): whether CPU calibration has run and the JMBE voice
	// codec is installed. Pointers so an older control server (absent fields)
	// reports null rather than a misleading false.
	Calibrated    *bool `json:"calibrated"`
	JmbeInstalled *bool `json:"jmbeInstalled"`
}

// Tuner mirrors one element of GET /tuners.
type Tuner struct {
	Index            int     `json:"index"`
	ID               string  `json:"id"`
	Name             string  `json:"name"`
	Type             string  `json:"type"`
	TunerClass       string  `json:"tunerClass"`
	Status           string  `json:"status"`
	Enabled          bool    `json:"enabled"`
	Available        bool    `json:"available"`
	Frequency        int64   `json:"frequency"`
	SampleRate       float64 `json:"sampleRate"`
	PPM              float64 `json:"ppm"`
	MeasuredPpmError float64 `json:"measuredPpmError"`
	// Gain is number|string|null depending on the tuner class, so it is decoded
	// loosely and forwarded verbatim.
	Gain    any     `json:"gain"`
	AutoPpm bool    `json:"autoPpm"`
	Error   *string `json:"error"`
	// Capabilities is the device's OWN description of what it accepts:
	// {sampleRates:[Hz…], gain:{mode, masterGain:[…], masterGainUnit, agc}}.
	// Forwarded verbatim so the staff panel can build each device's controls
	// from what the device reports instead of a hardcoded per-chip table.
	// It matters that this is authoritative: the R820T/R828D master gain is an
	// enum of RAW composite values (0, 9, 14, 26 … 495), not dB, and /gain snaps
	// whatever number it is sent to the nearest entry — so a dB figure silently
	// lands near the bottom of the range. masterGainUnit disambiguates
	// value/dB/index per chip.
	Capabilities map[string]any `json:"capabilities,omitempty"`
}

// Channel mirrors one element of the "channels" array of GET /channels.
type Channel struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	System      string  `json:"system"`
	Site        string  `json:"site"`
	Type        string  `json:"type"`
	Processing  bool    `json:"processing"`
	State       string  `json:"state"`
	Control     bool    `json:"control"`
	From        *string `json:"from"`
	FromAlias   *string `json:"fromAlias"`
	To          *string `json:"to"`
	ToAlias     *string `json:"toAlias"`
	TalkerAlias *string `json:"talkerAlias"`
	Timeslot    *int    `json:"timeslot"`
	Frequency   *int64  `json:"frequency"`
	// Live decode-health % (0-100) and signal level (dBFS) from vce's
	// ControlChannelQualityMonitor. Null unless this is a standard/control
	// channel with a fresh snapshot.
	SyncPercent *float64 `json:"syncPercent"`
	SignalDbfs  *float64 `json:"signalDbfs"`
}

// ActiveCall mirrors one element of the "activeCalls" array of GET /channels.
type ActiveCall struct {
	State       string   `json:"state"`
	Control     bool     `json:"control"`
	ChannelID   int      `json:"channelId"`
	ChannelName string   `json:"channelName"`
	From        *string  `json:"from"`
	FromAlias   *string  `json:"fromAlias"`
	To          *string  `json:"to"`
	Talkgroup   *string  `json:"talkgroup"`
	ToAlias     *string  `json:"toAlias"`
	TalkerAlias *string  `json:"talkerAlias"`
	Timeslot    *int     `json:"timeslot"`
	Frequency   *int64   `json:"frequency"`
	SyncPercent *float64 `json:"syncPercent"`
	SignalDbfs  *float64 `json:"signalDbfs"`
}

// Event mirrors one element of GET /events.
type Event struct {
	At         int64   `json:"at"`
	TimeStart  int64   `json:"timeStart"`
	TimeEnd    int64   `json:"timeEnd"`
	Type       string  `json:"type"`
	TypeLabel  string  `json:"typeLabel"`
	Protocol   string  `json:"protocol"`
	From       *string `json:"from"`
	FromAlias  *string `json:"fromAlias"`
	To         *string `json:"to"`
	ToAlias    *string `json:"toAlias"`
	DurationMs int64   `json:"durationMs"`
	Timeslot   *int    `json:"timeslot"`
	Details    *string `json:"details"`
	Channel    *string `json:"channel"`
}

// Client is the REST client for the SDR-Trunk control server.
type Client struct {
	baseURL string
	token   string
	hc      *http.Client
}

// New builds a control-server REST client for 127.0.0.1:port.
func New(port int, token string) *Client {
	return &Client{
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", port),
		token:   token,
		hc:      &http.Client{Timeout: restTimeout},
	}
}

// get issues an authenticated GET and decodes the JSON body into out.
func (c *Client) get(path string, out any) error {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("control GET %s: status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// okResp is the common {ok, error?} POST envelope.
type okResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
}

// post issues an authenticated POST with an optional JSON body and maps a
// non-ok response (HTTP error or {ok:false}) to a Go error carrying the
// server's message.
func (c *Client) post(path string, body any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Try to decode the {ok,error} envelope regardless of status code.
	var r okResp
	raw, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(raw, &r)

	if resp.StatusCode != http.StatusOK {
		if r.Error != "" {
			return fmt.Errorf("control POST %s: %s", path, r.Error)
		}
		return fmt.Errorf("control POST %s: status %d", path, resp.StatusCode)
	}
	if !r.OK {
		if r.Error != "" {
			return fmt.Errorf("%s", r.Error)
		}
		return fmt.Errorf("control POST %s: not ok", path)
	}
	return nil
}

// Status fetches GET /status.
func (c *Client) Status() (*Status, error) {
	var s Status
	if err := c.get("/status", &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Tuners fetches GET /tuners.
func (c *Client) Tuners() ([]Tuner, error) {
	var r struct {
		Tuners []Tuner `json:"tuners"`
	}
	if err := c.get("/tuners", &r); err != nil {
		return nil, err
	}
	return r.Tuners, nil
}

// Channels fetches GET /channels, which returns both the configured channels
// and the currently-active calls: {"channels":[...], "activeCalls":[...]}.
func (c *Client) Channels() ([]Channel, []ActiveCall, error) {
	var r struct {
		Channels    []Channel    `json:"channels"`
		ActiveCalls []ActiveCall `json:"activeCalls"`
	}
	if err := c.get("/channels", &r); err != nil {
		return nil, nil, err
	}
	return r.Channels, r.ActiveCalls, nil
}

// Events fetches GET /events?limit=N (newest last).
func (c *Client) Events(limit int) ([]Event, error) {
	var r struct {
		Events []Event `json:"events"`
	}
	if err := c.get(fmt.Sprintf("/events?limit=%d", limit), &r); err != nil {
		return nil, err
	}
	return r.Events, nil
}

// SetFrequency POSTs /tuners/{id}/frequency.
func (c *Client) SetFrequency(id string, hz int64) error {
	return c.post("/tuners/"+url.PathEscape(id)+"/frequency", map[string]any{"frequency": hz})
}

// SetPPM POSTs /tuners/{id}/ppm.
func (c *Client) SetPPM(id string, ppm float64) error {
	return c.post("/tuners/"+url.PathEscape(id)+"/ppm", map[string]any{"ppm": ppm})
}

// SetGain POSTs /tuners/{id}/gain with a single scalar gain value.
func (c *Client) SetGain(id string, gain int) error {
	return c.post("/tuners/"+url.PathEscape(id)+"/gain", map[string]any{"gain": gain})
}

// SetGainParams POSTs a device-specific gain object to /tuners/{id}/gain. The
// control server accepts a superset body ({gain, auto, gainMode, lnaGain,
// vgaGain, amp, lnaState, gainReduction, attenuation, lna}) and each device
// reads only the keys that apply to it, so the caller forwards whatever axes
// the tuner type exposes.
func (c *Client) SetGainParams(id string, params map[string]any) error {
	return c.post("/tuners/"+url.PathEscape(id)+"/gain", params)
}

// SetSampleRate POSTs /tuners/{id}/samplerate. Supported by the control server
// only for tuners with settable sample rates; a rejection is returned as an
// error for the caller to log (best-effort).
func (c *Client) SetSampleRate(id string, hz float64) error {
	return c.post("/tuners/"+url.PathEscape(id)+"/samplerate", map[string]any{"sampleRate": hz})
}

// SetAutoPPM POSTs /tuners/{id}/autoppm to toggle automatic PPM correction.
func (c *Client) SetAutoPPM(id string, enabled bool) error {
	return c.post("/tuners/"+url.PathEscape(id)+"/autoppm", map[string]any{"enabled": enabled})
}

// StartChannel POSTs /channels/{id}/start.
func (c *Client) StartChannel(id int) error {
	return c.post(fmt.Sprintf("/channels/%d/start", id), nil)
}

// StopChannel POSTs /channels/{id}/stop.
func (c *Client) StopChannel(id int) error {
	return c.post(fmt.Sprintf("/channels/%d/stop", id), nil)
}

// ReloadPlaylist POSTs /playlist/reload. Kept by sdrtrunk-vce as an alias of
// /config/reload for backward compatibility.
func (c *Client) ReloadPlaylist() error {
	return c.post("/playlist/reload", nil)
}

// ReloadConfig POSTs /config/reload: sdrtrunk-vce reloads its configuration
// from its SQLite database and restarts auto-start channels.
func (c *Client) ReloadConfig() error {
	return c.post("/config/reload", nil)
}

// ImportResult mirrors the POST /config/import response envelope:
// {ok:true, channels:N, aliases:N, streams:N} or {ok:false, error}.
type ImportResult struct {
	OK       bool   `json:"ok"`
	Channels int    `json:"channels"`
	Aliases  int    `json:"aliases"`
	Streams  int    `json:"streams"`
	Error    string `json:"error"`
}

// ImportConfig POSTs a vce ConfigurationState JSON document to /config/import
// (full-overwrite, idempotent). body must already be the serialized JSON. It
// uses a dedicated 60s timeout because the import (re)starts channels.
func (c *Client) ImportConfig(body []byte) (ImportResult, error) {
	var out ImportResult
	req, err := http.NewRequest(http.MethodPost, c.baseURL+"/config/import", bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	hc := &http.Client{Timeout: importTimeout}
	resp, err := hc.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = json.Unmarshal(raw, &out)

	if resp.StatusCode != http.StatusOK || !out.OK {
		if out.Error != "" {
			return out, fmt.Errorf("control POST /config/import: %s", out.Error)
		}
		return out, fmt.Errorf("control POST /config/import: status %d", resp.StatusCode)
	}
	return out, nil
}

// CallSite mirrors GET /activity/call-site: the P25 RFSS/site (and NAC) the
// control server observed for a talkgroup/source around a timestamp. Numeric
// fields are pointers because the server may return null for any of them.
type CallSite struct {
	Found  bool   `json:"found"`
	Rfss   *int   `json:"rfss"`
	Site   *int   `json:"site"`
	Nac    *int   `json:"nac"`
	Source string `json:"source"` // "event" | "channel" | "context"
}

// CallSite GETs /activity/call-site for the given call parameters. tsMs is the
// call start in epoch millis; windowMs is the match window around it.
func (c *Client) CallSite(tgid, src int, freqHz, tsMs int64, windowMs int) (CallSite, error) {
	var out CallSite
	path := fmt.Sprintf("/activity/call-site?tgid=%d&src=%d&freqHz=%d&tsMs=%d&windowMs=%d",
		tgid, src, freqHz, tsMs, windowMs)
	if err := c.get(path, &out); err != nil {
		return out, err
	}
	return out, nil
}

// ActivityEvent mirrors one element of GET /activity/events. Nullable numeric
// fields are pointers so a server null round-trips as null when the agent
// re-serializes the event for the backend.
type ActivityEvent struct {
	ID          int64   `json:"id"`
	AtMs        int64   `json:"atMs"`
	Action      string  `json:"action"`
	EventType   string  `json:"eventType"`
	Source      *int    `json:"source"`
	Target      *int    `json:"target"`
	FrequencyHz *int64  `json:"frequencyHz"`
	Timeslot    *int    `json:"timeslot"`
	Encrypted   bool    `json:"encrypted"`
	Rfss        *int    `json:"rfss"`
	Site        *int    `json:"site"`
	Nac         *int    `json:"nac"`
	Wacn        *int    `json:"wacn"`
	SystemID    *int    `json:"systemId"`
	ChannelName *string `json:"channelName"`
	// SystemName is the channel's configured P25 system name (e.g. "NSWPSN")
	// the operator's Data tab shows; SourceAlias is the over-the-air talker
	// alias last captured for the source radio. Both are null on older control
	// servers that don't emit them (backward compatible).
	SystemName  *string `json:"systemName"`
	SourceAlias *string `json:"sourceAlias"`
	// PatchMembers are the talkgroups patched into this call. A patched
	// transmission carries the PATCH GROUP as its target - a supergroup nobody
	// scans - so without these the real channels carrying the conversation are
	// unknowable downstream. Omitted when empty, which is the overwhelming
	// majority of calls, and absent entirely from control servers older than
	// the change that added it.
	PatchMembers []int `json:"patchMembers,omitempty"`
}

// ActivityEvents GETs /activity/events?sinceId=..&limit=..&kinds=calls: the
// decode-activity rows with id > sinceID (oldest first, up to limit). The
// second return is the response envelope's lastId; empty events with
// lastId == sinceID means nothing new, while lastId < sinceID signals the
// server's database was recreated and its ids restarted.
func (c *Client) ActivityEvents(sinceID int64, limit int) ([]ActivityEvent, int64, error) {
	var r struct {
		Events []ActivityEvent `json:"events"`
		LastID int64           `json:"lastId"`
		// Set when the control server's activity database could not be read.
		// Without it a locked or corrupt database looked exactly like a quiet
		// network — a valid 200 carrying no events — and shipping stopped
		// silently. Absent on control servers older than the change that
		// added it, which is simply the old behaviour.
		Error string `json:"error"`
	}
	path := fmt.Sprintf("/activity/events?sinceId=%d&limit=%d&kinds=calls", sinceID, limit)
	if err := c.get(path, &r); err != nil {
		return nil, 0, err
	}
	if r.Error != "" {
		return nil, sinceID, fmt.Errorf("control server activity read failed: %s", r.Error)
	}
	return r.Events, r.LastID, nil
}

// SiteSnapshots GETs /site/snapshots and returns the control server's "sites"
// array. Each element is forwarded to the backend verbatim, so the deep P25
// site metadata (control channel, channel plan, neighbors, bands, quality) is
// deliberately NOT modelled here — every object is kept as a raw JSON message
// and re-serialized unchanged, keeping the agent tolerant of vce contract
// additions. An older control server without the endpoint returns an error
// (non-200), which the caller treats as "nothing to ship this tick".
func (c *Client) SiteSnapshots() ([]json.RawMessage, error) {
	var r struct {
		Sites []json.RawMessage `json:"sites"`
	}
	if err := c.get("/site/snapshots", &r); err != nil {
		return nil, err
	}
	return r.Sites, nil
}

// SpectrumConn is a single shared WebSocket to the control server's spectrum
// endpoint (ws://127.0.0.1:P+1). It ref-counts active tuner streams: the
// connection opens on the first Start and closes when the last Stop leaves no
// active streams. Each received binary frame is handed to onBinary.
type SpectrumConn struct {
	wsURL    string
	token    string
	onBinary func([]byte)

	mu      sync.Mutex // guards conn + active
	conn    *websocket.Conn
	active  map[string]struct{}
	writeMu sync.Mutex // serializes writes (gorilla forbids concurrent writers)
}

type spectrumCtrl struct {
	T       string `json:"t"`
	TunerID string `json:"tunerId"`
	FPS     int    `json:"fps,omitempty"`
	Bins    int    `json:"bins,omitempty"`
}

// NewSpectrumConn builds a spectrum WS client. onBinary is invoked for every
// binary frame received (it must be safe to call from the read goroutine).
func NewSpectrumConn(port int, token string, onBinary func([]byte)) *SpectrumConn {
	return &SpectrumConn{
		wsURL:    fmt.Sprintf("ws://127.0.0.1:%d", port+1),
		token:    token,
		onBinary: onBinary,
	}
}

// Start ensures the WS is open, registers tunerId, and sends a spectrumStart
// control message. fps/bins default to 10/512 when non-positive.
func (s *SpectrumConn) Start(tunerID string, fps, bins int) error {
	if fps <= 0 {
		fps = 10
	}
	if bins <= 0 {
		bins = 512
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	opened := false
	if s.conn == nil {
		if err := s.dialLocked(); err != nil {
			return err
		}
		opened = true
	}
	if err := s.sendLocked(spectrumCtrl{T: "spectrumStart", TunerID: tunerID, FPS: fps, Bins: bins}); err != nil {
		// If we just opened purely for this tuner, tear it back down.
		if opened && len(s.active) == 0 {
			s.closeLocked()
		}
		return err
	}
	s.active[tunerID] = struct{}{}
	return nil
}

// Stop sends spectrumStop for tunerID and closes the WS when no streams remain.
func (s *SpectrumConn) Stop(tunerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil
	}
	err := s.sendLocked(spectrumCtrl{T: "spectrumStop", TunerID: tunerID})
	delete(s.active, tunerID)
	if len(s.active) == 0 {
		s.closeLocked()
	}
	return err
}

// ActiveCount reports how many tuner streams are currently active.
func (s *SpectrumConn) ActiveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}

// Close tears down the connection and clears active streams.
func (s *SpectrumConn) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked()
}

// dialLocked opens the WS and starts the read loop. Caller holds s.mu.
func (s *SpectrumConn) dialLocked() error {
	hdr := http.Header{"Authorization": {"Bearer " + s.token}}
	d := websocket.Dialer{HandshakeTimeout: restTimeout}
	// URL fallback with ?token= is accepted by the server, but the header is
	// the primary path.
	conn, _, err := d.Dial(s.wsURL, hdr)
	if err != nil {
		return err
	}
	s.conn = conn
	s.active = make(map[string]struct{})
	go s.readLoop(conn)
	return nil
}

// sendLocked writes a control message. Caller holds s.mu (which also gates
// conn), and this additionally takes writeMu to satisfy gorilla's single-writer
// rule against the read loop (which never writes) and any future writers.
func (s *SpectrumConn) sendLocked(msg spectrumCtrl) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(restTimeout))
	return s.conn.WriteJSON(msg)
}

// closeLocked closes and forgets the connection. Caller holds s.mu.
func (s *SpectrumConn) closeLocked() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.active = nil
}

// readLoop forwards binary frames to onBinary until the conn errors, then
// clears it if it is still the current connection.
func (s *SpectrumConn) readLoop(conn *websocket.Conn) {
	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			s.mu.Lock()
			if s.conn == conn {
				s.conn = nil
				s.active = nil
			}
			s.mu.Unlock()
			return
		}
		if mt == websocket.BinaryMessage && s.onBinary != nil {
			s.onBinary(data)
		}
	}
}
