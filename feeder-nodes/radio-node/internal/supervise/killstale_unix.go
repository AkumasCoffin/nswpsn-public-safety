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
// substrings (excluding this process) and SIGKILLs them. It's used on agent
// startup to reap a component — chiefly an SDR-Trunk JVM — left running by a
// previous agent that exited via a re-exec self-update (syscall.Exec keeps the
// PID/cgroup, so the child is NOT torn down). An orphaned JVM keeps holding its
// control port, so the freshly launched instance fails to bind. Best-effort.
func KillStale(matches []string) int {
	self := os.Getpid()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	killed := 0
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
			if m != "" && strings.Contains(cmdline, m) {
				_ = syscall.Kill(pid, syscall.SIGKILL)
				killed++
				break
			}
		}
	}
	return killed
}
