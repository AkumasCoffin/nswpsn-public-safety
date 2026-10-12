//go:build !windows

package update

import (
	"log"
	"os"
)

// RestartAgent ends this process so that it comes back up. It is the one place
// the deliberate agent-restart paths go through — staff rebootAgent, a
// component update that needs main() to re-resolve its launch path, and the
// fatal component error — so that "exit and get relaunched" means the same
// thing on every platform.
//
// On Unix a plain exit is all it takes: the unit the installer writes carries
// Restart=always (installers/linux/nodeagent.service), and kardianos's own
// generated unit defaults to the same, so the service manager brings us back
// within RestartSec. exitCode is preserved because a failure exit should still
// read as a failure in the journal.
func RestartAgent(reason string, exitCode int) {
	log.Printf("update: %s — exiting for the service manager to relaunch", reason)
	os.Exit(exitCode)
}
