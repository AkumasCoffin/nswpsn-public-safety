// Package rdioctl is a minimal client for the local rdio-scanner admin HTTP API.
// It mirrors the behaviour of rdio-scanner/server/command.go: login exchanges
// the admin password for a session token, and every authenticated request sends
// that token in a RAW "Authorization" header (NOT "Bearer <token>"). The agent
// uses it to push the full config document (apiKeys + downstream) into the local
// rdio instance it supervises.
package rdioctl

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DefaultAdminPassword is rdio-scanner's built-in fallback admin password, used
// when the agent has not persisted a random one (data/rdio-admin.secret missing).
const DefaultAdminPassword = "rdio-scanner"

// adminSecretFile is the bootstrap-written file holding the local rdio admin
// password (written by the agent when it first launched local rdio in P2).
const adminSecretFile = "rdio-admin.secret"

const httpTimeout = 15 * time.Second

// putConfigTimeout is the ceiling for PUT /api/admin/config specifically. The full
// rdio config (all systems/talkgroups) is large and rdio-scanner parses it, writes
// sqlite, and reloads — which routinely exceeds the 15s general timeout on a modest
// node and made every config apply fail (dragging the whole apply down with it).
// http.Client.Timeout is a hard ceiling a per-request context can't extend, so this
// call needs its own longer-timeout client.
const putConfigTimeout = 120 * time.Second

// Client talks to one rdio-scanner instance's admin API.
type Client struct {
	baseURL string
	token   string
	hc      *http.Client
	hcLong  *http.Client // longer timeout, used only for the large config PUT
}

// New builds a client for the rdio admin API rooted at baseURL
// (e.g. "http://127.0.0.1:17391"). The trailing slash is normalised away.
func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		hc:      &http.Client{Timeout: httpTimeout},
		hcLong:  &http.Client{Timeout: putConfigTimeout},
	}
}

// ReadAdminPassword returns the local rdio admin password: the trimmed contents
// of <dataDir>/rdio-admin.secret if present, else rdio-scanner's default. The
// bool reports whether the persisted secret was found (false => using default).
func ReadAdminPassword(dataDir string) (string, bool) {
	b, err := os.ReadFile(filepath.Join(dataDir, adminSecretFile))
	if err != nil {
		return DefaultAdminPassword, false
	}
	pw := strings.TrimSpace(string(b))
	if pw == "" {
		return DefaultAdminPassword, false
	}
	return pw, true
}

// EnsureAdminPassword returns the local rdio admin password, generating and
// persisting a random one to <dataDir>/rdio-admin.secret if none exists yet.
// It is used when the agent launches a MANAGED rdio-scanner (config command
// empty) and must pass --admin_password: the same secret is later read back by
// ReadAdminPassword for the config-apply admin login, so both agree.
func EnsureAdminPassword(dataDir string) (string, error) {
	if pw, ok := ReadAdminPassword(dataDir); ok {
		return pw, nil
	}
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate rdio admin password: %w", err)
	}
	pw := hex.EncodeToString(b[:])
	path := filepath.Join(dataDir, adminSecretFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(pw), 0o600); err != nil {
		return "", fmt.Errorf("write rdio admin secret: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("persist rdio admin secret: %w", err)
	}
	return pw, nil
}

// Login POSTs /api/admin/login with the password and stores the returned token
// for subsequent authenticated calls.
func (c *Client) Login(password string) error {
	body, err := json.Marshal(map[string]any{"password": password})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+"/api/admin/login", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("rdio login: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("rdio login: status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var r struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("rdio login: decode response: %w", err)
	}
	if r.Token == "" {
		return fmt.Errorf("rdio login: no token in response")
	}
	c.token = r.Token
	return nil
}

// GetConfig GETs /api/admin/config and returns the bare config document. The
// server wraps it as {"config": {...}, ...}; this unwraps the "config" object.
func (c *Client) GetConfig() (map[string]any, error) {
	req, err := http.NewRequest(http.MethodGet, c.baseURL+"/api/admin/config", nil)
	if err != nil {
		return nil, err
	}
	c.auth(req)

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rdio get config: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rdio get config: status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var wrapper map[string]any
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return nil, fmt.Errorf("rdio get config: decode: %w", err)
	}
	if cfg, ok := wrapper["config"].(map[string]any); ok {
		return cfg, nil
	}
	// Some builds may return the bare document; fall back to that.
	return wrapper, nil
}

// PutConfig PUTs /api/admin/config with the full config document as the body
// (the server replaces its config section-by-section from this map).
func (c *Client) PutConfig(cfg map[string]any) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("rdio put config: marshal: %w", err)
	}
	req, err := http.NewRequest(http.MethodPut, c.baseURL+"/api/admin/config", bytes.NewReader(body))
	if err != nil {
		return err
	}
	c.auth(req)
	req.Header.Set("Content-Type", "application/json")

	// Use the longer-timeout client: a large config apply in rdio can exceed the
	// general 15s timeout, which would abort the whole config apply before the
	// SDR-Trunk playlist is even written.
	resp, err := c.hcLong.Do(req)
	if err != nil {
		return fmt.Errorf("rdio put config (%d bytes): %w", len(body), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("rdio put config: status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil
}

// auth attaches the RAW session token in the Authorization header. rdio-scanner
// expects the bare token here, NOT a "Bearer " prefix.
func (c *Client) auth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", c.token)
	}
}
