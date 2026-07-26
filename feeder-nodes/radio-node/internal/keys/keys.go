// Package keys manages the per-agency local rdio API keys the agent injects into
// both the local rdio-scanner config (apiKeys[].key) and the SDR-Trunk playlist
// (<stream> api_key). Each key is a UUIDv4 generated once per system id and
// persisted to data/keys.json so it stays stable across agent restarts and
// config pushes — regenerating them would silently break the SDR-Trunk ->
// local-rdio upload auth on every apply.
package keys

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// EnsureKeys loads data/keys.json (a { "<systemId>": "<uuid>" } map), generates a
// stable UUIDv4 for any systemId in systemIds that lacks one, persists the file
// atomically when it changed, and returns the full systemId -> key map. It is
// safe to call on every config apply: existing keys are never rotated.
func EnsureKeys(dataDir string, systemIds []int) (map[int]string, error) {
	path := filepath.Join(dataDir, "keys.json")

	stored, err := load(path)
	if err != nil {
		return nil, err
	}

	changed := false
	for _, id := range systemIds {
		if _, ok := stored[id]; ok {
			continue
		}
		k, gerr := newUUIDv4()
		if gerr != nil {
			return nil, fmt.Errorf("generate key for system %d: %w", id, gerr)
		}
		stored[id] = k
		changed = true
	}

	if changed {
		if err := save(path, stored); err != nil {
			return nil, err
		}
	}

	return stored, nil
}

// load reads keys.json into an int-keyed map. A missing file is not an error
// (fresh install) — it yields an empty map.
func load(path string) (map[int]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[int]string{}, nil
		}
		return nil, fmt.Errorf("read %q: %w", path, err)
	}

	raw := map[string]string{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("parse %q: %w", path, err)
	}

	out := make(map[int]string, len(raw))
	for k, v := range raw {
		id, err := strconv.Atoi(k)
		if err != nil {
			return nil, fmt.Errorf("parse %q: bad system id %q: %w", path, k, err)
		}
		out[id] = v
	}
	return out, nil
}

// save writes the map back as { "<systemId>": "<uuid>" } atomically (temp +
// rename) with deterministic key order for stable diffs.
func save(path string, m map[int]string) error {
	raw := make(map[string]string, len(m))
	ids := make([]int, 0, len(m))
	for id, v := range m {
		raw[strconv.Itoa(id)] = v
		ids = append(ids, id)
	}
	sort.Ints(ids)

	b, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal keys: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("write temp keys: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace keys file: %w", err)
	}
	return nil
}

// newUUIDv4 returns a random RFC-4122 v4 UUID using crypto/rand (no external
// dependency), matching the generator used elsewhere in the agent.
func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
