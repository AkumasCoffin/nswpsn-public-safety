package agentcfg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadYAML(t *testing.T, extra string) (*Config, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	body := "server_url: https://example.test\n" +
		"data_dir: " + filepath.ToSlash(filepath.Join(dir, "data")) + "\n" +
		"presets_dir: " + filepath.ToSlash(filepath.Join(dir, "presets")) + "\n" + extra
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, path
}

func TestLocalListenDefaultsOnAndIsWrittenBack(t *testing.T) {
	// Existing installs have no local_listen_addr at all; they get the page.
	cfg, path := loadYAML(t, "")
	if cfg.LocalListenAddr != defaultLocalListen || !cfg.LocalListenEnabled() {
		t.Fatalf("want the default %q enabled, got %q", defaultLocalListen, cfg.LocalListenAddr)
	}
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), "local_listen_addr: 0.0.0.0:17380") {
		t.Fatalf("the default should be persisted so the owner can see and change it:\n%s", b)
	}
}

func TestLocalListenOffStaysOff(t *testing.T) {
	cfg, path := loadYAML(t, "local_listen_addr: \"off\"\n")
	if cfg.LocalListenEnabled() {
		t.Fatal(`"off" must disable the listener page`)
	}
	// And survives the next load, which rewrites the file when defaulting.
	again, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if again.LocalListenEnabled() {
		t.Fatal(`"off" must survive a reload`)
	}
}
