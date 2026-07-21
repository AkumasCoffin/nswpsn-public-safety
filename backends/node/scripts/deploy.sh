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
echo "[deploy] npm run build…"
npm run build

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
