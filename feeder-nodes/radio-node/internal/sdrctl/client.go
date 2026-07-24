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
	Error            *string `json:"error"`
}

// Channel mirrors one element of GET /channels.
type Channel struct {
	ID         int     `json:"id"`
	Name       string  `json:"name"`
	System     string  `json:"system"`
	Site       string  `json:"site"`
	Type       string  `json:"type"`
	Processing bool    `json:"processing"`
	State      string  `json:"state"`
	Frequency  *int64  `json:"frequency"`
	From       *string `json:"from"`
	To         *string `json:"to"`
}

// Event mirrors one element of GET /events.
type Event struct {
	At         int64   `json:"at"`
	Type       string  `json:"type"`
	Protocol   string  `json:"protocol"`
	From       *string `json:"from"`
	To         *string `json:"to"`
	DurationMs int64   `json:"durationMs"`
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

// Channels fetches GET /channels.
func (c *Client) Channels() ([]Channel, error) {
	var r struct {
		Channels []Channel `json:"channels"`
	}
	if err := c.get("/channels", &r); err != nil {
		return nil, err
	}
	return r.Channels, nil
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

// SetGain POSTs /tuners/{id}/gain.
func (c *Client) SetGain(id string, gain int) error {
	return c.post("/tuners/"+url.PathEscape(id)+"/gain", map[string]any{"gain": gain})
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

// ReloadPlaylist POSTs /playlist/reload.
func (c *Client) ReloadPlaylist() error {
	return c.post("/playlist/reload", nil)
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
