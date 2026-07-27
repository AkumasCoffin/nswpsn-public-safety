// Package agentcfg loads, defaults, and persists the pager node agent's YAML config.
package agentcfg

import (
	"crypto/rand"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ComponentCfg describes one supervised external process (a pager reader.sh).
type ComponentCfg struct {
	Enabled bool     `yaml:"enabled"`
	Command string   `yaml:"command"`
	Args    []string `yaml:"args"`
	WorkDir string   `yaml:"work_dir"`

	// Env holds extra environment variables applied on top of the agent's own
	// environment when launching the process. It is set programmatically, not
	// read from YAML.
	Env map[string]string `yaml:"-"`
}

// Config is the full agent configuration.
type Config struct {
	ServerURL string `yaml:"server_url"`
	WSURL     string `yaml:"ws_url"`
	NodeToken string `yaml:"node_token"`
	InstallID string `yaml:"install_id"`
	// Kind is the node type this agent runs as (radio/pager/adsb). Declared to
	// the backend in hello; defaults to pager (the only type this agent serves).
	Kind      string `yaml:"kind"`
	DataDir   string `yaml:"data_dir"`
	RelayAddr string `yaml:"relay_addr"`
}

const (
	defaultRelayAddr = "127.0.0.1:17390"
	defaultDataDir   = "./data"
)

// Load reads and parses the YAML config at path, applies defaults, generates and
// persists an install_id if missing, derives ws_url from server_url when empty,
// and ensures the data directories exist.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}

	changed := cfg.applyDefaults()

	// Generate + persist install_id on first run.
	if strings.TrimSpace(cfg.InstallID) == "" {
		id, err := newUUIDv4()
		if err != nil {
			return nil, fmt.Errorf("generate install_id: %w", err)
		}
		cfg.InstallID = id
		changed = true
	}

	if changed {
		if err := cfg.Save(path); err != nil {
			return nil, fmt.Errorf("persist config %q: %w", path, err)
		}
	}

	if err := cfg.ensureDirs(); err != nil {
		return nil, err
	}

	if err := cfg.validateSecurity(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// validateSecurity enforces transport-security invariants. server_url is the
// self-update integrity anchor: artifact sha256s come from a manifest fetched
// over it, so a plaintext channel lets a network MITM rewrite the manifest AND
// its sha256 together and serve a matching malicious binary → fleet-wide RCE.
// Require https except for a loopback dev server. The local relay is
// unauthenticated by design (the reader.sh curls can't auth), so it must bind
// loopback — a non-loopback bind is warned about loudly.
func (c *Config) validateSecurity() error {
	if s := strings.TrimSpace(c.ServerURL); s != "" {
		u, err := url.Parse(s)
		if err != nil {
			return fmt.Errorf("invalid server_url %q: %w", s, err)
		}
		if u.Scheme != "https" && !isLoopbackHost(u.Hostname()) {
			return fmt.Errorf(
				"server_url must be https (got %q) — refusing to run the self-updater over an unauthenticated channel",
				s,
			)
		}
	}
	if a := strings.TrimSpace(c.RelayAddr); a != "" {
		host, _, err := net.SplitHostPort(a)
		if err != nil {
			host = a
		}
		if !isLoopbackHost(host) {
			// The relay is UNAUTHENTICATED — a non-loopback bind lets any host on
			// the network POST forged pages under this node's identity. Refuse to
			// run rather than merely warn (mirrors the server_url https refusal).
			return fmt.Errorf(
				"relay_addr %q must be loopback (127.0.0.1 / ::1 / localhost) — the local relay is unauthenticated and must not be exposed off-host",
				a,
			)
		}
	}
	return nil
}

// isLoopbackHost reports whether h is localhost or a loopback IP.
func isLoopbackHost(h string) bool {
	h = strings.TrimSpace(h)
	if h == "localhost" {
		return true
	}
	if ip := net.ParseIP(h); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// applyDefaults fills empty fields with defaults and derives ws_url. It reports
// whether any value was changed (so the caller can decide to persist).
func (c *Config) applyDefaults() bool {
	changed := false

	if strings.TrimSpace(c.Kind) == "" {
		c.Kind = "pager"
		changed = true
	}
	if strings.TrimSpace(c.RelayAddr) == "" {
		c.RelayAddr = defaultRelayAddr
		changed = true
	}
	if strings.TrimSpace(c.DataDir) == "" {
		c.DataDir = defaultDataDir
		changed = true
	}
	if strings.TrimSpace(c.WSURL) == "" && strings.TrimSpace(c.ServerURL) != "" {
		c.WSURL = deriveWSURL(c.ServerURL)
		changed = true
	}
	return changed
}

// deriveWSURL maps an http(s) base URL to its ws(s) equivalent.
func deriveWSURL(serverURL string) string {
	s := strings.TrimSpace(serverURL)
	switch {
	case strings.HasPrefix(s, "https://"):
		return "wss://" + strings.TrimPrefix(s, "https://")
	case strings.HasPrefix(s, "http://"):
		return "ws://" + strings.TrimPrefix(s, "http://")
	default:
		return s
	}
}

// ensureDirs creates data_dir plus its queue/ and logs/ subdirectories.
func (c *Config) ensureDirs() error {
	for _, d := range []string{c.DataDir, c.QueueDir(), c.LogsDir()} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("create dir %q: %w", d, err)
		}
	}
	return nil
}

// QueueDir is the disk queue directory.
func (c *Config) QueueDir() string { return filepath.Join(c.DataDir, "queue") }

// LogsDir is the child-process log directory.
func (c *Config) LogsDir() string { return filepath.Join(c.DataDir, "logs") }

// AppliedConfigVersionPath is the file recording the config version last
// successfully applied, so the agent can report appliedConfigVersion at hello.
func (c *Config) AppliedConfigVersionPath() string {
	return filepath.Join(c.DataDir, "applied-config-version")
}

// AppliedConfigPath is the file recording the FULL config last successfully
// applied (frequency plan + toggles), so the agent can replay the exact same
// config on boot instead of a hardcoded default. Without this the agent would
// boot on the default frequency order (NSW RFS first) yet still report the
// persisted version, so the backend — seeing a matching version — sends no
// push and the node stays on the wrong frequency (e.g. after a Fire & Rescue
// primary was chosen). Persisting the whole config keeps boot honest.
func (c *Config) AppliedConfigPath() string {
	return filepath.Join(c.DataDir, "applied-config.json")
}

// Save round-trips the config back to YAML at path.
func (c *Config) Save(path string) error {
	b, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	tmp := path + ".tmp"
	// 0o600: this file holds node_token (a bearer credential). World-readable
	// perms would let any other local user read it and impersonate the node.
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("write temp config: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}

// newUUIDv4 returns a random RFC-4122 v4 UUID string using crypto/rand.
func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
