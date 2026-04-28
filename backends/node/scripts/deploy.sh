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
echo "[deploy] git pull…"
git pull --ff-only

cd "$NODE_DIR"
echo "[deploy] npm run build…"
npm run build

echo "[deploy] pm2 restart api-node…"
# Try `api-node` by name first; if that fails, try id 6 (current
# process id on the host as of writing). Either resolves to the same
# process; this just future-proofs against rename.
pm2 restart api-node 2>/dev/null || pm2 restart 6

echo "[deploy] done — recent logs:"
sleep 2
pm2 logs api-node --lines 20 --nostream || pm2 logs 6 --lines 20 --nostream
