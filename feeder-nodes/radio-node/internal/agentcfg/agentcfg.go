// Package agentcfg loads, defaults, and persists the node agent's YAML config.
package agentcfg

import (
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ComponentCfg describes one supervised external process (SDR-Trunk or rdio).
type ComponentCfg struct {
	Enabled bool     `yaml:"enabled"`
	Command string   `yaml:"command"`
	Args    []string `yaml:"args"`
	WorkDir string   `yaml:"work_dir"`

	// Env holds extra environment variables applied on top of the agent's own
	// environment when launching the process (e.g. SDRTRUNK_CONTROL_TOKEN). It
	// is set programmatically, not read from YAML.
	Env map[string]string `yaml:"-"`
}

// Config is the full agent configuration.
type Config struct {
	ServerURL string `yaml:"server_url"`
	WSURL     string `yaml:"ws_url"`
	NodeToken string `yaml:"node_token"`
	InstallID string `yaml:"install_id"`
	DataDir   string `yaml:"data_dir"`
	RelayAddr string `yaml:"relay_addr"`

	// SDRTrunkControlPort is the REST port of the SDR-Trunk headless control
	// server the agent launches sdrtrunk with (WS spectrum is port+1).
	SDRTrunkControlPort int `yaml:"sdrtrunk_control_port"`

	// PresetsDir holds the base playlist/rdio presets the agent reads at runtime
	// when applying a pushed config (default ./presets). If missing, the agent
	// falls back to presets embedded in the binary.
	PresetsDir string `yaml:"presets_dir"`

	// SDRTrunkAppRoot is the SDR-Trunk application root the agent launches
	// sdrtrunk with (--app-root) AND writes the rendered playlist under
	// (<root>/playlist/default.xml). Both must agree. Default <data_dir>/sdrtrunk.
	SDRTrunkAppRoot string `yaml:"sdrtrunk_app_root"`

	SDRTrunk ComponentCfg `yaml:"sdrtrunk"`
	Rdio     ComponentCfg `yaml:"rdio"`
}

const (
	defaultRelayAddr   = "127.0.0.1:17390"
	defaultDataDir     = "./data"
	defaultControlPort = 17392
	defaultPresetsDir  = "./presets"
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
// unauthenticated by design (rdio downstreams can't auth), so it must bind
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
			log.Printf("WARNING: relay_addr %q is not loopback; the local relay is unauthenticated — any host that can reach it can inject calls under this node's identity", a)
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

	if strings.TrimSpace(c.RelayAddr) == "" {
		c.RelayAddr = defaultRelayAddr
		changed = true
	}
	if strings.TrimSpace(c.DataDir) == "" {
		c.DataDir = defaultDataDir
		changed = true
	}
	if c.SDRTrunkControlPort == 0 {
		c.SDRTrunkControlPort = defaultControlPort
		changed = true
	}
	if strings.TrimSpace(c.PresetsDir) == "" {
		c.PresetsDir = defaultPresetsDir
		changed = true
	}
	if strings.TrimSpace(c.SDRTrunkAppRoot) == "" {
		// Defaults under data_dir, which is itself defaulted just above.
		c.SDRTrunkAppRoot = filepath.Join(c.DataDir, "sdrtrunk")
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

// AppliedConfigPath is the file holding the full last-applied config payload
// (JSON). The agent re-renders the SDR-Trunk playlist from it at startup — BEFORE
// launching SDR-Trunk — so SDR-Trunk always boots from the current config instead
// of a stale last-session playlist.
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
