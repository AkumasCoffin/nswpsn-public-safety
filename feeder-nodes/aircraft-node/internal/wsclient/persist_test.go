package wsclient

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/agentcfg"
)

func tmpCfg(t *testing.T) *agentcfg.Config {
	t.Helper()
	return &agentcfg.Config{Kind: "adsb", DataDir: t.TempDir()}
}

func write(t *testing.T, cfg *agentcfg.Config, body string) {
	t.Helper()
	if err := os.WriteFile(cfg.AppliedConfigPath(), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The exact failure seen in production: a host switched from a pager node came
// up as ADS-B, unmarshalled the PAGER's applied-config (the structs share
// ConfigVersion / CaptureEnabled / FeedEnabled), and resumed it — reporting a
// config version belonging to another node while running with no antenna
// position. The backend then saw a node already in sync and pushed nothing.
func TestIgnoresAnotherKindsPersistedConfig(t *testing.T) {
	cfg := tmpCfg(t)
	write(t, cfg, `{
	  "kind": "pager",
	  "ConfigVersion": "66c318bcc36fcb5a0a30f13e2e866b9b21821126d0cf3032db4c13f32494203b",
	  "CaptureEnabled": true,
	  "FeedEnabled": true,
	  "Frequencies": [{"label":"QFES","mhz":148.6375}],
	  "Protocols": ["POCSAG512"]
	}`)

	got, ok := LoadPersistedConfig(cfg)
	if ok {
		t.Fatalf("a pager config must not be resumed by the adsb agent, got %+v", got)
	}
	if got.ConfigVersion != "" {
		t.Fatalf("version leaked from the other kind's file: %q", got.ConfigVersion)
	}
}

func TestResumesOwnPersistedConfig(t *testing.T) {
	cfg := tmpCfg(t)
	lat, lon := -33.8688, 151.2093
	c := &Client{cfg: cfg}
	if err := c.persistAppliedConfig(AdsbConfig{
		ConfigVersion: "abc123", CaptureEnabled: true, FeedEnabled: true,
		Gain: "auto", Lat: &lat, Lon: &lon,
	}); err != nil {
		t.Fatal(err)
	}

	got, ok := LoadPersistedConfig(cfg)
	if !ok {
		t.Fatal("this agent must resume the config it wrote itself")
	}
	if got.ConfigVersion != "abc123" || got.Gain != "auto" {
		t.Fatalf("round-trip lost fields: %+v", got)
	}
	if got.Lat == nil || *got.Lat != lat || got.Lon == nil || *got.Lon != lon {
		t.Fatalf("antenna position did not round-trip: %+v", got)
	}
}

func TestStampsTheKindOnDisk(t *testing.T) {
	cfg := tmpCfg(t)
	c := &Client{cfg: cfg}
	if err := c.persistAppliedConfig(AdsbConfig{ConfigVersion: "v1"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(cfg.AppliedConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"kind":"adsb"`) {
		t.Fatalf("the written file must record which kind wrote it:\n%s", string(b))
	}
}

// A file from an agent predating the stamp could have been written by either
// kind, so it is refused for the same reason. The cost is one forced re-push,
// which is immediate; resuming the wrong kind's config is not self-correcting.
func TestIgnoresUnstampedConfig(t *testing.T) {
	cfg := tmpCfg(t)
	write(t, cfg, `{"ConfigVersion":"older","CaptureEnabled":true,"FeedEnabled":true}`)
	if _, ok := LoadPersistedConfig(cfg); ok {
		t.Fatal("an unstamped config must not be resumed")
	}
}

func TestMissingAndCorruptFiles(t *testing.T) {
	cfg := tmpCfg(t)
	if _, ok := LoadPersistedConfig(cfg); ok {
		t.Fatal("no file means nothing to resume")
	}
	write(t, cfg, "{not json")
	if _, ok := LoadPersistedConfig(cfg); ok {
		t.Fatal("a corrupt file means nothing to resume")
	}
	// ...and neither case may leave a stray temp file behind.
	entries, _ := os.ReadDir(filepath.Dir(cfg.AppliedConfigPath()))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("left a temp file behind: %s", e.Name())
		}
	}
}
