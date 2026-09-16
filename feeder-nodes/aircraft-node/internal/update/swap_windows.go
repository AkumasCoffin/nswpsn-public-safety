//go:build windows

package update

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

// swapAndRestart replaces a running Windows executable, which cannot overwrite
// itself while the process holds the image locked. It writes a tiny detached
// batch helper that spins until the exe is unlocked (i.e. this process exits),
// moves the pending binary over it, and restarts the service — falling back to
// relaunching the exe directly when the service isn't registered (foreground
// dev runs). This process then exits so the helper's move can succeed.
//
// NOTE: the restart path can only be end-to-end verified on a real machine with
// the nswpsn-node service installed (or a real staged download). Compile-tested
// here; the batch logic is best-effort and documented for the operator.
func swapAndRestart(exePath, pendingPath string) error {
	if _, err := os.Stat(pendingPath); err != nil {
		return fmt.Errorf("pending binary missing: %w", err)
	}

	helper := filepath.Join(filepath.Dir(exePath), "nodeagent-update.bat")
	script := fmt.Sprintf(`@echo off
setlocal
set "TARGET=%s"
set "PENDING=%s"
:retry
move /Y "%%PENDING%%" "%%TARGET%%" >nul 2>&1
if errorlevel 1 (
  ping -n 2 127.0.0.1 >nul
  goto retry
)
sc start nswpsn-node >nul 2>&1
if errorlevel 1 (
  start "" "%%TARGET%%" run
)
del "%%~f0" >nul 2>&1
`, exePath, pendingPath)

	if err := os.WriteFile(helper, []byte(script), 0o644); err != nil {
		return fmt.Errorf("write update helper: %w", err)
	}

	// Launch the helper fully detached: cmd /c start "" /b <bat>. It outlives us.
	cmd := exec.Command("cmd", "/c", "start", "", "/b", helper)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("launch update helper: %w", err)
	}

	log.Printf("update: staged agent swap; exiting so helper can replace %s", exePath)
	os.Exit(0)
	return nil // unreachable
}
