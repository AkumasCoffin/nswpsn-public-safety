// Package presets bundles the base SDR-Trunk playlist and rdio-scanner config
// documents into the agent binary via go:embed. These are the SAME no-secret
// presets the backend serves (apiKeys[].key / api_key attributes are empty);
// they act purely as a fallback so the agent can render a playlist even when the
// on-disk presets_dir is missing. The canonical, backend-populated copies live
// in the repo's top-level feeder-nodes/radio-node/presets/ directory and are the
// preferred source at runtime — see internal/configapply for the load order.
package presets

import _ "embed"

// DefaultPlaylistXML is the embedded SDR-Trunk PlaylistV2 template (default.xml).
//
//go:embed default.xml
var DefaultPlaylistXML []byte

// RdioConfigJSON is the embedded rdio-scanner config document (rdio-scanner.json).
//
//go:embed rdio-scanner.json
var RdioConfigJSON []byte
