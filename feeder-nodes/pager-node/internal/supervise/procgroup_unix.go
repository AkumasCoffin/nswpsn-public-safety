//go:build !windows

package supervise

import (
	"os/exec"
	"syscall"
)

// setProcessGroup puts the child in its OWN process group so the whole tree
// (e.g. the reader.sh bash launcher + the rtl_fm/multimon-ng/curl processes it
// pipes together) can be signalled as a unit. Without this, killing the launcher
// leaves rtl_fm orphaned holding the SDR device open, so the relaunched reader
// fails to acquire it.
func setProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// killProcessGroup SIGKILLs the child's entire process group. With Setpgid the
// child's PID equals its PGID, so the negative pid targets the whole group
// (launcher + pipeline children), leaving no orphan behind.
func killProcessGroup(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
