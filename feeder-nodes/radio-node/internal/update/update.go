// Package update implements the node agent's self-update and managed-component
// install (Phase 4). It fetches a signed-by-sha256 manifest from the backend,
// downloads + verifies artifacts, lays managed components out under
// <data_dir>/components/<name>/<version>/ with a portable "current.txt" pointer,
// and self-replaces the running agent binary (via a detached helper on Windows,
// an in-place rename + re-exec on Unix).
//
// PLACEHOLDER-SAFE
// ----------------
// The live backend manifest currently ships PLACEHOLDER urls and EMPTY sha256
// values. Every entry point here treats an empty/placeholder url or empty sha256
// as "nothing to do": FetchManifest still succeeds, EnsureComponent resolves
// whatever (if anything) is already installed without downloading, and
// StageAgentUpdate returns ErrNothingToDo. No downloads, no errors, no crashes.
package update

import (
	"archive/zip"
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

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/version"
)

// ErrNothingToDo is the sentinel returned when a manifest entry has no usable
// artifact (empty/placeholder url or empty sha256) or is not newer than what is
// already installed/running. Callers treat it as a graceful no-op, never a fault.
var ErrNothingToDo = errors.New("update: nothing to do")

// fetchTimeout bounds the manifest GET; artifact downloads get their own,
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

// Manifest mirrors GET <server_url>/api/node-updates/manifest.
type Manifest struct {
	Agent    ComponentSpec `json:"agent"`
	SDRTrunk ComponentSpec `json:"sdrtrunk"`
	Rdio     ComponentSpec `json:"rdio"`
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

// ---- managed components -----------------------------------------------------

// Installed describes a resolved managed component the supervisor can launch.
type Installed struct {
	Name     string // "sdrtrunk" | "rdio"
	Version  string // active version dir name
	Dir      string // <data_dir>/components/<name>/<version>
	ExecPath string // primary executable to launch (java for sdrtrunk, the binary for rdio)
}

// componentsRoot is <data_dir>/components.
func componentsRoot(dataDir string) string { return filepath.Join(dataDir, "components") }

// componentDir is <data_dir>/components/<name>.
func componentDir(dataDir, name string) string { return filepath.Join(componentsRoot(dataDir), name) }

// pointerPath is the "current" pointer file for a component.
//
// POINTER CHOICE: a small text file (current.txt) holding the active version
// directory name — NOT a symlink/junction. Symlink creation on Windows requires
// either Developer Mode or the SeCreateSymbolicLink privilege and silently fails
// for unprivileged services; directory junctions need mklink/DeviceIoControl and
// can't span across the update boundary cleanly. A plain pointer file is fully
// portable, atomic to swap (write temp + rename), and readable without any OS
// privilege, so it's used on every platform for consistency.
func pointerPath(dataDir, name string) string {
	return filepath.Join(componentDir(dataDir, name), "current.txt")
}

// readCurrentVersion returns the active version dir name from the pointer file,
// or "" if none is installed.
func readCurrentVersion(dataDir, name string) string {
	b, err := os.ReadFile(pointerPath(dataDir, name))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// writeCurrentVersion atomically points "current" at ver.
func writeCurrentVersion(dataDir, name, ver string) error {
	p := pointerPath(dataDir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, []byte(ver), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

// execName returns the platform binary name for a component's primary executable.
func execName(name string) string {
	switch name {
	case "sdrtrunk":
		// jlink runtime java launcher.
		if runtime.GOOS == "windows" {
			return "java.exe"
		}
		return "java"
	case "rdio":
		if runtime.GOOS == "windows" {
			return "rdio-scanner.exe"
		}
		return "rdio-scanner"
	default:
		if runtime.GOOS == "windows" {
			return name + ".exe"
		}
		return name
	}
}

// execPathFor resolves a component's primary executable inside its version dir.
// sdrtrunk (a jlink runtime zip) launches via <dir>/bin/java; rdio is the single
// binary sitting directly in <dir>.
func execPathFor(name, versionDir string) string {
	if name == "sdrtrunk" {
		return filepath.Join(versionDir, "bin", execName(name))
	}
	return filepath.Join(versionDir, execName(name))
}

// isZipArtifact reports whether a component ships as a zip (extracted on
// install) rather than a single binary.
func isZipArtifact(name string) bool { return name == "sdrtrunk" }

// resolveInstalled returns the currently-active install for name, or nil if
// nothing is installed or the pointed-at exec is missing. It never touches the
// network, so it works offline / with a placeholder manifest.
func resolveInstalled(name, dataDir string) *Installed {
	ver := readCurrentVersion(dataDir, name)
	if ver == "" {
		return nil
	}
	dir := filepath.Join(componentDir(dataDir, name), ver)
	exec := execPathFor(name, dir)
	if _, err := os.Stat(exec); err != nil {
		return nil
	}
	return &Installed{Name: name, Version: ver, Dir: dir, ExecPath: exec}
}

// EnsureComponent makes sure the manifest's version of a managed component is
// installed, then returns the resolved install (or nil if none is available).
//
// Behaviour:
//   - If spec has a valid url+sha for this platform and that version is not yet
//     installed, it downloads + verifies + lays it out under the version dir and
//     flips the "current" pointer.
//   - With a placeholder/empty spec (the live manifest today) it installs
//     nothing and simply resolves whatever is already installed — returning nil
//     when nothing is, so the supervisor keeps that component skipped.
//
// It is best-effort: a download/extract failure is returned as an error for the
// caller to log, but callers treat a nil-with-nil-error as "not installed".
func EnsureComponent(name string, spec ComponentSpec, dataDir string) (*Installed, error) {
	url, sha, ok := spec.artifact()
	ver := strings.TrimSpace(spec.Version)

	// No usable artifact (placeholder manifest) or already the active version:
	// just resolve what's on disk.
	if !ok || ver == "" || readCurrentVersion(dataDir, name) == ver {
		return resolveInstalled(name, dataDir), nil
	}

	versionDir := filepath.Join(componentDir(dataDir, name), ver)
	if err := installArtifact(name, url, sha, versionDir); err != nil {
		if errors.Is(err, ErrNothingToDo) {
			return resolveInstalled(name, dataDir), nil
		}
		return resolveInstalled(name, dataDir), fmt.Errorf("install %s %s: %w", name, ver, err)
	}

	if err := writeCurrentVersion(dataDir, name, ver); err != nil {
		return resolveInstalled(name, dataDir), fmt.Errorf("point %s at %s: %w", name, ver, err)
	}
	return resolveInstalled(name, dataDir), nil
}

// installArtifact downloads the component artifact and lays it out under
// versionDir. A zip artifact (sdrtrunk jlink runtime) is extracted; a single
// binary (rdio) is placed as the component's exec name.
func installArtifact(name, url, sha, versionDir string) error {
	if err := os.MkdirAll(versionDir, 0o755); err != nil {
		return err
	}

	if isZipArtifact(name) {
		zipPath := filepath.Join(versionDir, "artifact.zip")
		if err := downloadVerified(url, sha, zipPath); err != nil {
			return err
		}
		defer os.Remove(zipPath)
		if err := unzip(zipPath, versionDir); err != nil {
			return fmt.Errorf("extract: %w", err)
		}
		return nil
	}

	// Single-binary component.
	dest := execPathFor(name, versionDir)
	return downloadVerified(url, sha, dest)
}

// unzip extracts src into destDir, guarding against zip-slip and preserving the
// executable bit on the jlink bin/ entries.
func unzip(src, destDir string) error {
	zr, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer zr.Close()

	destAbs, err := filepath.Abs(destDir)
	if err != nil {
		return err
	}

	for _, f := range zr.File {
		target := filepath.Join(destDir, f.Name) //nolint:gosec // guarded below
		targetAbs, err := filepath.Abs(target)
		if err != nil {
			return err
		}
		// zip-slip guard: the resolved path must stay under destDir.
		if targetAbs != destAbs && !strings.HasPrefix(targetAbs, destAbs+string(os.PathSeparator)) {
			return fmt.Errorf("zip entry escapes dest: %q", f.Name)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}

		mode := f.Mode()
		if mode == 0 {
			mode = 0o644
		}
		if err := writeZipEntry(f, target, mode); err != nil {
			return err
		}
	}
	return nil
}

func writeZipEntry(f *zip.File, target string, mode os.FileMode) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, rc); err != nil { //nolint:gosec // trusted, sha256-verified archive
		_ = out.Close()
		return err
	}
	return out.Close()
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
