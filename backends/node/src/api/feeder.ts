/**
 * Volunteer-facing feeder endpoints (radio_contributor only).
 *
 * A contributor uses feeder.html to see their nodes, reveal their (short)
 * token prefix, and download an installer that carries their long-lived
 * feeder token. Auth is a strict Supabase JWT plus the radio_contributor
 * role — NOT the public API key.
 *
 *   GET /api/feeder/me
 *   GET /api/feeder/download/linux
 *   GET /api/feeder/download/windows
 */
import { Hono } from 'hono';
import type { MiddlewareHandler } from 'hono';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { requireSupabaseJwt } from '../services/auth/supabaseJwt.js';
import { hasRole } from '../services/auth/roles.js';
import {
  feederTokensConfigured,
  mintFeederToken,
} from '../services/auth/nodeToken.js';
import { listNodesForUser } from '../services/nodes/registry.js';
import { hub } from '../services/nodes/hub.js';

export const feederRouter = new Hono();

// The URL the installed node agent connects back to. Prefer the configured
// public base (dev/staging overrides) and fall back to production.
const SERVER_URL = config.PUBLIC_BASE_URL ?? 'https://api.forcequit.xyz';

// Where the canonical OS installers are published (SITE repo GitHub Releases).
// The bootstrap scripts below embed the caller's token + server URL, then
// fetch + run the real installer from here. `latest` tracks the newest release.
const INSTALLER_BASE =
  'https://github.com/AkumasCoffin/nswpsn-node/releases/latest/download';

/** Safely embed a value inside a bash single-quoted string. */
function shSingleQuote(v: string): string {
  return `'${v.replace(/'/g, `'\\''`)}'`;
}

/** Safely embed a value inside a PowerShell single-quoted string. */
function psSingleQuote(v: string): string {
  return `'${v.replace(/'/g, `''`)}'`;
}

/**
 * Gate on radio_contributor. Runs AFTER requireSupabaseJwt has set userId.
 */
const requireRadioContributor: MiddlewareHandler = async (c, next) => {
  const userId = c.get('userId');
  if (!userId || !(await hasRole(userId, ['radio_contributor']))) {
    return c.json({ error: 'not a radio contributor' }, 403);
  }
  await next();
};

feederRouter.use('/api/feeder/*', requireSupabaseJwt, requireRadioContributor);

// ---------------------------------------------------------------------------
// GET /api/feeder/me
// ---------------------------------------------------------------------------
feederRouter.get('/api/feeder/me', async (c) => {
  if (!feederTokensConfigured()) {
    return c.json({ error: 'feeder not configured' }, 503);
  }
  const userId = c.get('userId') as string;
  try {
    const { prefix } = await mintFeederToken(userId);
    const nodes = (await listNodesForUser(userId)).map((n) => ({
      id: n.id,
      installId: n.install_id,
      name: n.name,
      enabled: n.enabled,
      online: hub.isOnline(n.id),
      lastSeenAt: n.last_seen_at,
      agentVersion: n.agent_version,
    }));
    return c.json({ role: true, tokenPrefix: prefix, nodes });
  } catch (err) {
    log.error({ err, userId }, 'Error building feeder me');
    return c.json({ error: 'Failed to load feeder info' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/feeder/download/linux
// ---------------------------------------------------------------------------
feederRouter.get('/api/feeder/download/linux', async (c) => {
  if (!feederTokensConfigured()) {
    return c.json({ error: 'feeder not configured' }, 503);
  }
  const userId = c.get('userId') as string;
  try {
    const { token } = await mintFeederToken(userId);
    // Bootstrap: bake the caller's token + server in, then fetch + run the
    // canonical installer from the release. The real install.sh (built by the
    // installers agent) reads NODE_TOKEN + SERVER_URL from the environment.
    const script =
      `#!/usr/bin/env bash\n` +
      `# NSW PSN radio feeder node installer (bootstrap).\n` +
      `# Downloads and runs the current release installer with your node token\n` +
      `# and server URL baked in. Re-run any time to update.\n` +
      `set -euo pipefail\n` +
      `\n` +
      `export NODE_TOKEN=${shSingleQuote(token)}\n` +
      `export SERVER_URL=${shSingleQuote(SERVER_URL)}\n` +
      `INSTALLER_URL=${shSingleQuote(`${INSTALLER_BASE}/install.sh`)}\n` +
      `\n` +
      `echo "Fetching NSW PSN node installer..."\n` +
      `tmp="$(mktemp)"\n` +
      `if command -v curl >/dev/null 2>&1; then\n` +
      `  curl -fsSL "$INSTALLER_URL" -o "$tmp"\n` +
      `elif command -v wget >/dev/null 2>&1; then\n` +
      `  wget -qO "$tmp" "$INSTALLER_URL"\n` +
      `else\n` +
      `  echo "error: need curl or wget" >&2; exit 1\n` +
      `fi\n` +
      `chmod +x "$tmp"\n` +
      `NODE_TOKEN="$NODE_TOKEN" SERVER_URL="$SERVER_URL" bash "$tmp"\n` +
      `rm -f "$tmp"\n`;
    return c.body(script, 200, {
      'Content-Type': 'text/x-shellscript',
      'Content-Disposition': 'attachment; filename="install-nswpsn-node.sh"',
    });
  } catch (err) {
    log.error({ err, userId }, 'Error building linux installer');
    return c.json({ error: 'Failed to build installer' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/feeder/download/windows
// ---------------------------------------------------------------------------
feederRouter.get('/api/feeder/download/windows', async (c) => {
  if (!feederTokensConfigured()) {
    return c.json({ error: 'feeder not configured' }, 503);
  }
  const userId = c.get('userId') as string;
  try {
    const { token } = await mintFeederToken(userId);
    // PowerShell bootstrap: bake token + server in, download + launch the
    // release Windows installer. Cleaner cross-origin than a token-in-filename
    // .exe, and the PS1 carries the token in its body (not the URL/filename).
    const script =
      `# NSW PSN radio feeder node installer (bootstrap).\r\n` +
      `# Run in PowerShell:  powershell -ExecutionPolicy Bypass -File install-nswpsn-node.ps1\r\n` +
      `# Re-run any time to update.\r\n` +
      `$ErrorActionPreference = 'Stop'\r\n` +
      `$NodeToken = ${psSingleQuote(token)}\r\n` +
      `$ServerUrl = ${psSingleQuote(SERVER_URL)}\r\n` +
      `$InstallerUrl = ${psSingleQuote(`${INSTALLER_BASE}/install.ps1`)}\r\n` +
      `\r\n` +
      `$env:NODE_TOKEN = $NodeToken\r\n` +
      `$env:SERVER_URL = $ServerUrl\r\n` +
      `\r\n` +
      `Write-Host 'Fetching NSW PSN node installer...'\r\n` +
      `$tmp = Join-Path $env:TEMP 'install-nswpsn-node.inner.ps1'\r\n` +
      `Invoke-WebRequest -Uri $InstallerUrl -OutFile $tmp -UseBasicParsing\r\n` +
      `& powershell -ExecutionPolicy Bypass -File $tmp\r\n` +
      `Remove-Item $tmp -Force -ErrorAction SilentlyContinue\r\n`;
    return c.body(script, 200, {
      'Content-Type': 'text/plain; charset=utf-8',
      'Content-Disposition': 'attachment; filename="install-nswpsn-node.ps1"',
    });
  } catch (err) {
    log.error({ err, userId }, 'Error building windows installer');
    return c.json({ error: 'Failed to build installer' }, 500);
  }
});
