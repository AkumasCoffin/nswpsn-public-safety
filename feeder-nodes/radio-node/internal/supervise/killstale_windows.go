//go:build windows

package supervise

import (
	"os/exec"
	"strings"
)

// KillStale best-effort terminates leftover component processes by image name.
// On Windows the agent self-update goes through a detached helper + service
// restart (which tears down the process tree), so orphans are far less likely
// than on Unix's re-exec path; this is a backstop. It matches SDR-Trunk's
// launcher/JVM heuristically via `taskkill` image-name filters.
func KillStale(matches []string) int {
	killed := 0
	for _, m := range matches {
		if m == "" || !strings.Contains(strings.ToLower(m), "sdr") {
			continue
		}
		// Kill java hosting SDR-Trunk and any sdr-trunk launcher; /F force, /T tree.
		_ = exec.Command("taskkill", "/F", "/T", "/FI", "WINDOWTITLE eq sdr-trunk*").Run()
		_ = exec.Command("taskkill", "/F", "/T", "/IM", "sdr-trunk.exe").Run()
		killed++
		break
	}
	return killed
}
