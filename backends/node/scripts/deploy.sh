#!/usr/bin/env bash
# One-shot deploy: pull, build, restart pm2.
#
# Use this instead of `pm2 restart api-node` when you want to be sure
# dist/ is rebuilt. The ecosystem.config.js setup auto-builds via the
# prestart hook, but if you're running pm2 directly on dist/index.js
# (e.g. created with `pm2 start dist/index.js`) you NEED the explicit
# build before restart.
#
# Usage (from anywhere):
#   bash /var/www/nswpsn/backends/node/scripts/deploy.sh
#
# Or via npm:
#   cd /var/www/nswpsn/backends/node && npm run deploy

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODE_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$NODE_DIR/../.." && pwd)"

echo "[deploy] repo: $REPO_ROOT"
echo "[deploy] node: $NODE_DIR"

cd "$REPO_ROOT"
# `npm install` (below) rewrites package-lock.json every deploy, leaving the
# working tree dirty and making the NEXT `git pull --ff-only` abort with
# "local changes would be overwritten". Discard that auto-generated drift
# before pulling — the committed lockfile is authoritative and npm install
# re-resolves it anyway. Scoped to the lockfile so real edits are untouched.
git checkout -- backends/node/package-lock.json 2>/dev/null || true
echo "[deploy] git pull…"
git pull --ff-only

cd "$NODE_DIR"
# Install any newly-added deps from package.json so the build can find
# them. Includes dev deps because tsc + @types are dev-scoped and the
# prestart hook runs `tsc`. `npm ci` would be stricter but bails out on
# any node_modules drift, which is too brittle for a one-shot deploy.
echo "[deploy] npm install…"
npm install --no-audit --no-fund

# Playwright pins a specific Chromium revision per release. When the playwright
# package is bumped (e.g. a dependabot update), the OLD browser stays on disk,
# so chromium.launch() fails and the headless-browser workers go not-ready —
# /api/marinetraffic/vessels then returns 503 (and centralwatch breaks too).
# Reinstall the matching Chromium on every deploy; idempotent + fast when it's
# already current. Non-fatal so a browser hiccup doesn't abort the whole deploy.
echo "[deploy] playwright install chromium…"
npx playwright install chromium || echo "[deploy] WARNING: playwright chromium install failed — marinetraffic/centralwatch browser may not start"

# The Wire's video pipeline (watermark burn-in + bitrate normalisation + EXIF
# strip + poster) shells out to the ffmpeg binary that `ffmpeg-static` unpacks
# during npm install. Surface a broken/missing install HERE rather than at the
# first video upload — without it, videos are silently served exactly as
# uploaded. Non-fatal: everything else still works.
FFMPEG_BIN="$(node -e 'try{const m=require("ffmpeg-static");process.stdout.write(String((typeof m==="string"?m:m&&m.default)||""))}catch(e){}' 2>/dev/null || true)"
if [ -n "$FFMPEG_BIN" ] && [ -x "$FFMPEG_BIN" ]; then
  echo "[deploy] ffmpeg: $("$FFMPEG_BIN" -version 2>/dev/null | head -1)"
else
  echo "[deploy] WARNING: ffmpeg-static unusable — Wire videos will be served un-transcoded (no watermark, no bitrate normalisation)"
fi

echo "[deploy] npm run build…"
npm run build

# Build the node-agent binaries into the webroot downloads dir so volunteers
# fetch the latest agent (see NODE_DOWNLOADS_BASE). Needs Go >= 1.26 — snap
# installs it to /snap/bin. Non-fatal: if Go is missing the rest of the deploy
# still runs (existing binaries just aren't refreshed). The big SDR-Trunk
# runtime + rdio binary are placed in the same dir once, by hand.
AGENT_SRC="$REPO_ROOT/feeder-nodes/radio-node"
PAGER_SRC="$REPO_ROOT/feeder-nodes/pager-node"
DOWNLOADS_DIR="$REPO_ROOT/downloads"
EXPECTED_AGENTS="nodeagent-linux-amd64 nodeagent-windows-amd64.exe nodeagent-linux-arm64"
# Pager agent: Linux only (rtl_fm | multimon-ng stack), amd64 + arm64 (Pi).
PAGER_AGENTS="nodeagent-pager-linux-amd64 nodeagent-pager-linux-arm64"
export PATH="$PATH:/snap/bin"
if command -v go >/dev/null 2>&1 && [ -d "$AGENT_SRC" ]; then
  mkdir -p "$DOWNLOADS_DIR"
  # The built agent's version is stamped (below) to match the manifest's
  # agent.version, so self-update compares EQUAL and doesn't loop. To push a
  # new agent to running nodes: bump `agent.version` in node-versions.json.
  AGENT_VERSION="$(node -e 'process.stdout.write(String(require(process.argv[1]).agent.version||"0.0.0"))' "$NODE_DIR/assets/node-versions.json" 2>/dev/null || echo 0.0.0)"

  # Skip the rebuild if the already-published binary is already this version —
  # only the native linux-amd64 build can be run here to read its version.
  BUILT_VERSION=""
  if [ -x "$DOWNLOADS_DIR/nodeagent-linux-amd64" ]; then
    BUILT_VERSION="$("$DOWNLOADS_DIR/nodeagent-linux-amd64" version 2>/dev/null | sed -nE 's/^radio-node ([^ ]+).*/\1/p')"
  fi
  if [ -n "$AGENT_VERSION" ] && [ "$BUILT_VERSION" = "$AGENT_VERSION" ]; then
    echo "[deploy] node-agent v$AGENT_VERSION already built — skipping rebuild."
  else
    LDFLAGS="-s -w -X github.com/AkumasCoffin/nswpsn-node/radio-node/internal/version.Version=$AGENT_VERSION"
    echo "[deploy] building node-agent v$AGENT_VERSION (was '${BUILT_VERSION:-none}'; $(go version | awk '{print $3}'))…"
    (
      cd "$AGENT_SRC"
      GOOS=linux   GOARCH=amd64 go build -ldflags "$LDFLAGS" -o "$DOWNLOADS_DIR/nodeagent-linux-amd64"       ./cmd/nodeagent
      GOOS=windows GOARCH=amd64 go build -ldflags "$LDFLAGS" -o "$DOWNLOADS_DIR/nodeagent-windows-amd64.exe" ./cmd/nodeagent
      GOOS=linux   GOARCH=arm64 go build -ldflags "$LDFLAGS" -o "$DOWNLOADS_DIR/nodeagent-linux-arm64"       ./cmd/nodeagent
    )
    # Publish a sha256 sidecar next to each binary so install.sh can verify the
    # download against the published publisher hash (not just TLS).
    for b in $EXPECTED_AGENTS; do
      if [ -f "$DOWNLOADS_DIR/$b" ]; then
        ( cd "$DOWNLOADS_DIR" && sha256sum "$b" | awk '{print $1}' > "$b.sha256" )
      fi
    done
    echo "[deploy] node-agent binaries + sha256 sidecars updated in $DOWNLOADS_DIR"
  fi

  # ---- Pager node agent (separate module + binary, Linux amd64/arm64) --------
  if [ -d "$PAGER_SRC" ]; then
    PAGER_VERSION="$(node -e 'process.stdout.write(String((require(process.argv[1])["pager-agent"]||{}).version||"0.0.0"))' "$NODE_DIR/assets/node-versions.json" 2>/dev/null || echo 0.0.0)"
    PAGER_BUILT=""
    if [ -x "$DOWNLOADS_DIR/nodeagent-pager-linux-amd64" ]; then
      PAGER_BUILT="$("$DOWNLOADS_DIR/nodeagent-pager-linux-amd64" version 2>/dev/null | sed -nE 's/^pager-node ([^ ]+).*/\1/p')"
    fi
    if [ -n "$PAGER_VERSION" ] && [ "$PAGER_BUILT" = "$PAGER_VERSION" ]; then
      echo "[deploy] pager-agent v$PAGER_VERSION already built — skipping rebuild."
    else
      PLDFLAGS="-s -w -X github.com/AkumasCoffin/nswpsn-node/pager-node/internal/version.Version=$PAGER_VERSION"
      echo "[deploy] building pager-agent v$PAGER_VERSION (was '${PAGER_BUILT:-none}')…"
      (
        cd "$PAGER_SRC"
        GOOS=linux GOARCH=amd64 go build -ldflags "$PLDFLAGS" -o "$DOWNLOADS_DIR/nodeagent-pager-linux-amd64" ./cmd/nodeagent
        GOOS=linux GOARCH=arm64 go build -ldflags "$PLDFLAGS" -o "$DOWNLOADS_DIR/nodeagent-pager-linux-arm64" ./cmd/nodeagent
      )
      for b in $PAGER_AGENTS; do
        if [ -f "$DOWNLOADS_DIR/$b" ]; then
          ( cd "$DOWNLOADS_DIR" && sha256sum "$b" | awk '{print $1}' > "$b.sha256" )
        fi
      done
      echo "[deploy] pager-agent binaries + sha256 sidecars updated in $DOWNLOADS_DIR"
    fi
  fi

  # Remove any stale agent binaries no longer in the built set (e.g. an arch we
  # stopped shipping) so downloads/ never serves an orphaned old build. A
  # `<name>.sha256` sidecar is kept iff its base binary is still expected.
  for f in "$DOWNLOADS_DIR"/nodeagent-*; do
    [ -e "$f" ] || continue
    base="$(basename "$f")"
    check="${base%.sha256}"
    case " $EXPECTED_AGENTS $PAGER_AGENTS " in
      *" $check "*) ;;
      *) echo "[deploy] removing stale $base"; rm -f "$f" ;;
    esac
  done
else
  echo "[deploy] WARNING: 'go' not found (or agent source missing) — skipping node-agent build."
  echo "[deploy]          install Go 1.26+ with: sudo snap install go --classic"
fi

# Apply any pending DB migrations BEFORE the restart — new code often
# depends on new columns (e.g. incidents.units), and the migration
# runner is idempotent so this is a no-op when everything is applied.
echo "[deploy] npm run migrate…"
npm run migrate

echo "[deploy] pm2 restart api-node…"
# Try `api-node` by name first; if that fails, try id 6 (current
# process id on the host as of writing). Either resolves to the same
# process; this just future-proofs against rename.
pm2 restart api-node 2>/dev/null || pm2 restart 6

echo "[deploy] done — recent logs:"
sleep 2
pm2 logs api-node --lines 20 --nostream || pm2 logs 6 --lines 20 --nostream
