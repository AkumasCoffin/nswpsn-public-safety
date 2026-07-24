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
var Version = "0.0.0-dev"

// String returns a human-readable version line including OS/arch and Go version.
func String() string {
	return fmt.Sprintf("radio-node %s (%s/%s, %s)", Version, runtime.GOOS, runtime.GOARCH, runtime.Version())
}
