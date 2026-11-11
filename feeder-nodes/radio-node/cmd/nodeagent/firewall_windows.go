//go:build windows

package main

import (
	"log"
	"os/exec"
)

// localListenRule is the Windows Defender Firewall rule that lets the owner's
// own network reach the listener page (internal/localweb). Without it the
// service binds the port but every inbound connection is dropped.
const localListenRule = "NSWPSN Node Listen"

// ensureLocalListenFirewall makes sure the inbound rule exists for port. Run
// at every start rather than only from the installer, because nodes
// self-update and never re-run it. remoteip=localsubnet keeps it to the
// machine's own network whatever profile Windows has filed that network
// under (home networks are often left as "Public"). Best effort: a failure is
// logged and the agent carries on.
func ensureLocalListenFirewall(port string) {
	verb := "add"
	if exec.Command("netsh", "advfirewall", "firewall", "show", "rule", "name="+localListenRule).Run() == nil {
		verb = "set" // already there: rewrite it, in case the port changed
	}
	args := []string{"advfirewall", "firewall", verb, "rule", "name=" + localListenRule}
	if verb == "set" {
		args = append(args, "new")
	}
	args = append(args, "dir=in", "action=allow", "protocol=TCP", "localport="+port,
		"remoteip=localsubnet", "profile=any", "enable=yes")
	if out, err := exec.Command("netsh", args...).CombinedOutput(); err != nil {
		log.Printf("localweb: could not %s firewall rule %q (%v): %s", verb, localListenRule, err, out)
	}
}

// removeLocalListenFirewall deletes the rule on uninstall. A missing rule is
// not an error worth reporting.
func removeLocalListenFirewall() {
	_ = exec.Command("netsh", "advfirewall", "firewall", "delete", "rule", "name="+localListenRule).Run()
}
