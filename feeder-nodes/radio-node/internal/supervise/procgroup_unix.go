//go:build !windows

package supervise

import (
	"os/exec"
	"syscall"
)

// setProcessGroup puts the child in its OWN process group so the whole tree
// (e.g. SDR-Trunk's launch script + the java process it spawns) can be signalled
// together. Without this, killing the launcher leaves the JVM orphaned — it keeps
// holding its ports (e.g. the control port 17392), so the relaunched instance
// fails with "Address already in use".
func setProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// killProcessGroup SIGKILLs the child's entire process group. With Setpgid the
// child's PID equals its PGID, so the negative pid targets the whole group
// (launcher + JVM + any grandchildren), leaving no orphan behind.
func killProcessGroup(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
