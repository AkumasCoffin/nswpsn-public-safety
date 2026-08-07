// Package version exposes the agent's build version and a formatted string
// including the Go runtime OS/arch. Version is overridable at build time via
// -ldflags "-X .../internal/version.Version=x.y.z".
package version

import (
	"fmt"
	"runtime"
)

// Version is the semantic version of this agent build. Override with:
//
//	go build -ldflags "-X github.com/AkumasCoffin/nswpsn-node/radio-node/internal/version.Version=1.2.3"
var Version = "0.2.0-dev"

// String returns a human-readable version line including OS/arch and Go version.
func String() string {
	return fmt.Sprintf("radio-node %s (%s/%s, %s)", Version, runtime.GOOS, runtime.GOARCH, runtime.Version())
}

// UserAgent is the User-Agent the agent sends on every request to the server.
// A distinct, non-default UA (Go's default "Go-http-client/1.1" trips
// Cloudflare Bot Fight Mode) so it's identifiable and can be allow-listed.
func UserAgent() string {
	return fmt.Sprintf("NSWPSN-NodeAgent/%s (%s; %s)", Version, runtime.GOOS, runtime.GOARCH)
}
