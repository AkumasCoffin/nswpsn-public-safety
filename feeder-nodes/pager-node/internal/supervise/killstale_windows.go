//go:build windows

package supervise

import (
	"os/exec"
	"strings"
)

// KillStale best-effort terminates leftover component processes matching any of
// the given terms. On Windows the agent self-update goes through a detached
// helper + service restart (which tears down the process tree), so orphans are
// far less likely than on Unix's re-exec path; this is a backstop. Each match
// string is used generically as both a taskkill image-name filter and a window
// title filter, so it works for arbitrary process names (rtl_fm, multimon-ng, …)
// rather than a single hard-coded term.
func KillStale(matches []string) int {
	killed := 0
	for _, m := range matches {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		// Force-kill by image name and by window title matching the term (tree kill).
		_ = exec.Command("taskkill", "/F", "/T", "/IM", m).Run()
		_ = exec.Command("taskkill", "/F", "/T", "/FI", "WINDOWTITLE eq "+m+"*").Run()
		killed++
	}
	return killed
}
