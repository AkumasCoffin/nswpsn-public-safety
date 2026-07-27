//go:build !windows

package supervise

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// KillStale finds processes whose /proc cmdline contains any of the match
// substrings (excluding this process + its own group) and SIGKILLs their WHOLE
// process group. It's used on agent startup to reap a reader pipeline left
// running by a previous agent that exited via a re-exec self-update (syscall.Exec
// keeps the PID/cgroup, so the children are NOT torn down); an orphaned rtl_fm
// keeps the dongle busy so the fresh reader can't open it.
//
// IMPORTANT: match strings must be SPECIFIC to this agent's own processes — pass
// the reader-scripts directory path, NOT a generic binary name like "rtl_fm",
// which would also kill an unrelated SDR tool (e.g. the operator's own Pagermon,
// which is literally rtl_fm | multimon-ng) running on the same box. We kill the
// matched process's GROUP so the reader's rtl_fm/multimon/curl children (which
// don't carry the match string in their own cmdline) die too. Best-effort.
func KillStale(matches []string) int {
	self := os.Getpid()
	selfpg, _ := syscall.Getpgid(self)
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	killed := 0
	killedPG := make(map[int]bool)
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		cmdline := strings.ReplaceAll(string(b), "\x00", " ")
		for _, m := range matches {
			if m == "" || !strings.Contains(cmdline, m) {
				continue
			}
			pg, err := syscall.Getpgid(pid)
			// Never target pid 1, our own process, or our own group.
			if err != nil || pg <= 1 || pg == self || pg == selfpg {
				_ = syscall.Kill(pid, syscall.SIGKILL)
				killed++
				break
			}
			if killedPG[pg] {
				break
			}
			killedPG[pg] = true
			_ = syscall.Kill(-pg, syscall.SIGKILL) // kill the whole group
			killed++
			break
		}
	}
	return killed
}
