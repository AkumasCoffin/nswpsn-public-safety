//go:build windows

package supervise

import (
	"os/exec"
	"strconv"
	"syscall"
)

const createNewProcessGroup = 0x00000200 // CREATE_NEW_PROCESS_GROUP

// setProcessGroup starts the child in a new process group so it (and any
// processes it spawns) can be terminated as a unit.
func setProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= createNewProcessGroup
}

// killProcessGroup terminates the child and its descendants. `taskkill /T` walks
// the process tree by PID, so no child is left orphaned on restart.
func killProcessGroup(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(cmd.Process.Pid)).Run()
}
