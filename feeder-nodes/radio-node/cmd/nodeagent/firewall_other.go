//go:build !windows

package main

// The host's firewall (ufw, firewalld, nftables) is left to its owner on
// Linux; docs/components/radio-node.md says which port to open.
func ensureLocalListenFirewall(string) {}

func removeLocalListenFirewall() {}
