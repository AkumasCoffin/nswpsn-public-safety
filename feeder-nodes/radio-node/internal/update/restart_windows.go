//go:build windows

package update

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

// RestartAgent ends this process so that it comes back up. It is the one place
// the deliberate agent-restart paths go through — staff rebootAgent, a
// component update that needs main() to re-resolve its launch path, and the
// fatal component error — so that "exit and get relaunched" means the same
// thing on every platform.
//
// On Windows a bare exit is NOT that. The service is installed with recovery
// actions (see newService in cmd/nodeagent), but a recovery action is the
// SCM's answer to a service that failed, not a dependable answer to one whose
// process left on purpose — and a node that does not come back is dark until
// somebody logs in and starts it: meanwhile its orphaned rdio keeps POSTing to
// a relay that is gone, so every reception in between is lost.
//
// So the restart is made explicit, through the same detached-helper pattern
// swapAndRestart already uses: a small batch file outlives this process, waits
// for the SCM to notice it is gone, and puts the service back. With no service
// registered — a foreground dev run — it relaunches the executable instead,
// and that branch is chosen by asking whether the service exists rather than
// by letting `sc start` fail, so a transient start failure can never leave a
// second agent running alongside the service.
//
// NOTE: like swapAndRestart, this can only be verified end to end on a real
// Windows host with the nswpsn-node service installed. Every enrolled node
// runs Linux today, so the path is latent.
func RestartAgent(reason string, exitCode int) {
	if err := spawnRestartHelper(); err != nil {
		log.Printf("update: restart helper failed (%v); exiting and relying on service recovery", err)
	}
	log.Printf("update: %s — exiting for restart", reason)
	os.Exit(exitCode)
}

// spawnRestartHelper writes and detaches the batch helper that restarts the
// service once this process is gone.
func spawnRestartHelper() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	// Resolve any symlink so the fallback relaunch runs the real file.
	if resolved, rerr := filepath.EvalSymlinks(exe); rerr == nil {
		exe = resolved
	}

	// `ping` is the sleep available to a bare .bat: the SCM needs a moment to
	// see this process end before it will accept a start, and the `sc stop`
	// makes that transition explicit rather than waiting on the detection.
	helper := filepath.Join(filepath.Dir(exe), "nodeagent-restart.bat")
	script := fmt.Sprintf(`@echo off
setlocal
set "TARGET=%s"
set "SVC=%s"
sc query "%%SVC%%" >nul 2>&1
if errorlevel 1 (
  ping -n 3 127.0.0.1 >nul
  start "" "%%TARGET%%" run
  goto done
)
ping -n 3 127.0.0.1 >nul
sc stop "%%SVC%%" >nul 2>&1
ping -n 4 127.0.0.1 >nul
sc start "%%SVC%%" >nul 2>&1
if errorlevel 1 (
  ping -n 6 127.0.0.1 >nul
  sc start "%%SVC%%" >nul 2>&1
)
:done
del "%%~f0" >nul 2>&1
`, exe, serviceName)

	if err := os.WriteFile(helper, []byte(script), 0o644); err != nil {
		return fmt.Errorf("write restart helper: %w", err)
	}
	// Fully detached (cmd /c start "" /b <bat>), so it outlives this process.
	cmd := exec.Command("cmd", "/c", "start", "", "/b", helper)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("launch restart helper: %w", err)
	}
	return nil
}
