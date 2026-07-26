// Package update implements the pager node agent's self-update. It fetches a
// signed-by-sha256 manifest from the backend, downloads + verifies the new agent
// binary, and self-replaces the running agent (via a detached helper on Windows,
// an in-place rename + re-exec on Unix).
//
// The pager agent has NO managed external components (its readers are plain
// reader.sh scripts driving system-installed rtl_fm/multimon-ng/curl), so unlike
// the radio agent this only ever self-updates its own binary — the manifest's
// "agent" component.
//
// PLACEHOLDER-SAFE
// ----------------
// The live backend manifest currently ships PLACEHOLDER urls and EMPTY sha256
// values. Every entry point here treats an empty/placeholder url or empty sha256
// as "nothing to do": FetchManifest still succeeds and StageAgentUpdate returns
// ErrNothingToDo. No downloads, no errors, no crashes.
package update

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/version"
)

// ErrNothingToDo is the sentinel returned when a manifest entry has no usable
// artifact (empty/placeholder url or empty sha256) or is not newer than what is
// already running. Callers treat it as a graceful no-op, never a fault.
var ErrNothingToDo = errors.New("update: nothing to do")

// fetchTimeout bounds the manifest GET; the artifact download gets its own,
// generous, timeout since binaries can be large.
const (
	fetchTimeout    = 15 * time.Second
	downloadTimeout = 30 * time.Minute
)

// ComponentSpec is one artifact family in the manifest: a version plus per
// platform download urls and expected sha256 hex digests.
type ComponentSpec struct {
	Version string            `json:"version"`
	URLs    map[string]string `json:"urls"`
	SHA256  map[string]string `json:"sha256"`
}

// Manifest mirrors GET <server_url>/api/node-updates/manifest. The pager agent
// only consumes the "agent" component (self-update); the backend may still ship
// other component keys, which are simply ignored here.
type Manifest struct {
	Agent ComponentSpec `json:"agent"`
	// AutoUpdate is the server's global auto-update switch. A pointer so a
	// missing field (nil) is treated as enabled; false means AUTOMATIC passes
	// (startup / 6h) must NOT apply updates, though a MANUAL update command
	// still does.
	AutoUpdate *bool `json:"autoUpdate"`
}

// artifact returns the (url, sha256) for the current platform, plus ok=false
// when the entry is empty or a placeholder (so the caller skips gracefully).
func (s ComponentSpec) artifact() (url, sha string, ok bool) {
	key := platformKey()
	url = strings.TrimSpace(s.URLs[key])
	sha = strings.TrimSpace(s.SHA256[key])
	if isPlaceholderURL(url) || sha == "" {
		return "", "", false
	}
	return url, sha, true
}

// platformKey is the manifest map key for the running platform, e.g.
// "windows-amd64" / "linux-amd64".
func platformKey() string {
	return runtime.GOOS + "-" + runtime.GOARCH
}

// isPlaceholderURL reports whether url is empty or an obvious manifest
// placeholder (the backend ships these until real GitHub Releases exist).
func isPlaceholderURL(url string) bool {
	if url == "" {
		return true
	}
	low := strings.ToLower(url)
	for _, marker := range []string{"placeholder", "example.com", "replace_me", "replaceme", "todo"} {
		if strings.Contains(low, marker) {
			return true
		}
	}
	return false
}

// FetchManifest GETs the backend update manifest with the node token header.
func FetchManifest(serverURL, token string) (*Manifest, error) {
	url := strings.TrimSuffix(serverURL, "/") + "/api/node-updates/manifest"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Node-Token", token)
	req.Header.Set("User-Agent", version.UserAgent())

	hc := &http.Client{Timeout: fetchTimeout}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("fetch manifest: status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var m Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	return &m, nil
}

// downloadVerified streams url to destPath.tmp, computing sha256 as it writes,
// verifies the digest, and atomically renames into place. An empty/placeholder
// url or empty sha256 yields ErrNothingToDo (not an error). Not resumable: a
// partial/failed download leaves no destPath and should be retried whole.
func downloadVerified(url, sha256hex, destPath string) error {
	url = strings.TrimSpace(url)
	sha256hex = strings.TrimSpace(sha256hex)
	if isPlaceholderURL(url) || sha256hex == "" {
		return ErrNothingToDo
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", version.UserAgent())
	hc := &http.Client{Timeout: downloadTimeout}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: status %d", url, resp.StatusCode)
	}

	tmp := destPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, h), resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("download %s: %w", url, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	got := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(got, sha256hex) {
		_ = os.Remove(tmp)
		return fmt.Errorf("sha256 mismatch for %s: got %s want %s", url, got, sha256hex)
	}

	// Atomic replace. On Windows os.Rename fails if destPath exists, so remove
	// any stale artifact first (best-effort).
	_ = os.Remove(destPath)
	if err := os.Rename(tmp, destPath); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// ---- agent self-update ------------------------------------------------------

// StageAgentUpdate downloads + verifies a newer agent binary into
// nodeagent.pending next to the running exe and returns its path + the new
// version. It returns ErrNothingToDo when the manifest has no usable artifact or
// the advertised version is not newer than the running one, so callers can skip
// silently. The caller performs the actual swap (via SwapAndRestart) AFTER
// acknowledging, since a running exe cannot replace itself.
func StageAgentUpdate(spec ComponentSpec, dataDir string) (pendingPath, newVersion string, err error) {
	url, sha, ok := spec.artifact()
	newVersion = strings.TrimSpace(spec.Version)
	if !ok || newVersion == "" {
		return "", "", ErrNothingToDo
	}
	if !versionNewer(newVersion, version.Version) {
		return "", newVersion, ErrNothingToDo
	}

	exe, err := os.Executable()
	if err != nil {
		return "", newVersion, err
	}
	pendingPath = pendingBinaryPath(exe)

	if err := downloadVerified(url, sha, pendingPath); err != nil {
		return "", newVersion, err
	}
	return pendingPath, newVersion, nil
}

// pendingBinaryPath is the staged-download path beside the running exe.
func pendingBinaryPath(exe string) string {
	dir := filepath.Dir(exe)
	if runtime.GOOS == "windows" {
		return filepath.Join(dir, "nodeagent.pending.exe")
	}
	return filepath.Join(dir, "nodeagent.pending")
}

// SwapAndRestart replaces the running executable with the staged pending binary
// and restarts the agent. The mechanism is platform-specific (see
// swap_windows.go / swap_unix.go); on success it does not return (the process is
// re-exec'd or exits for the service manager / helper to relaunch).
func SwapAndRestart(pendingPath string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	// Resolve any symlinks so we replace the real file, not a link.
	if resolved, rerr := filepath.EvalSymlinks(exe); rerr == nil {
		exe = resolved
	}
	return swapAndRestart(exe, pendingPath)
}

// versionNewer reports whether candidate is a strictly newer semver than
// current, comparing dotted numeric components and ignoring any pre-release
// suffix (e.g. current "0.0.0-dev" -> [0 0 0]). Unparseable parts count as 0, so
// a well-formed manifest version always wins over the "-dev" default.
func versionNewer(candidate, current string) bool {
	c := parseVersion(candidate)
	cur := parseVersion(current)
	for i := 0; i < 3; i++ {
		if c[i] != cur[i] {
			return c[i] > cur[i]
		}
	}
	return false
}

// parseVersion extracts up to three leading numeric components from a version
// string, stripping a leading "v" and any "-suffix"/"+build" metadata.
func parseVersion(v string) [3]int {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "v")
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		v = v[:i]
	}
	var out [3]int
	parts := strings.Split(v, ".")
	for i := 0; i < 3 && i < len(parts); i++ {
		n, err := strconv.Atoi(strings.TrimSpace(parts[i]))
		if err == nil {
			out[i] = n
		}
	}
	return out
}
