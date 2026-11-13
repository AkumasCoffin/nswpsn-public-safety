//go:build !windows

package update

import (
	"fmt"
	"log"
	"os"
	"syscall"
)

// swapAndRestart replaces the running agent on Unix. Unlike Windows, a running
// binary's file can be renamed over while executing (the kernel holds the open
// inode), so we atomically move the pending binary into place and then re-exec
// the new image with the same args/env. Re-exec keeps the PID, so a systemd unit
// stays satisfied; on success syscall.Exec never returns.
//
// NOTE: only compile-tested on this Windows host; the rename+re-exec path must
// be verified on a real Linux service host with a genuine staged download.
func swapAndRestart(exePath, pendingPath string) error {
	if _, err := os.Stat(pendingPath); err != nil {
		return fmt.Errorf("pending binary missing: %w", err)
	}
	if err := os.Chmod(pendingPath, 0o755); err != nil {
		return fmt.Errorf("chmod pending: %w", err)
	}
	// Keep the binary being replaced as <exe>.prev. The rename below would
	// otherwise be the last anyone saw of it, and a new build that cannot
	// start leaves a host with nothing to go back to: with this, recovery is
	// `mv nodeagent.prev nodeagent`. A hard link, so it costs no copy; if it
	// fails (another filesystem, no permission) the update still goes ahead.
	prev := exePath + ".prev"
	_ = os.Remove(prev)
	if err := os.Link(exePath, prev); err != nil {
		log.Printf("update: could not keep the previous binary as %s: %v", prev, err)
	}
	if err := os.Rename(pendingPath, exePath); err != nil {
		return fmt.Errorf("replace exe: %w", err)
	}

	log.Printf("update: replaced %s; re-exec'ing new agent image", exePath)
	if err := syscall.Exec(exePath, os.Args, os.Environ()); err != nil {
		// Re-exec failed: fall back to a clean exit for the service manager to
		// relaunch the (already-replaced) binary.
		log.Printf("update: re-exec failed (%v); exiting for service manager restart", err)
		os.Exit(0)
	}
	return nil // unreachable on success
}
