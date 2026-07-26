/**
 * Volunteer-facing feeder endpoints (radio_contributor only).
 *
 * The backend generates ONE self-contained installer per OS with the caller's
 * feeder token baked in (JWT-gated). That installer downloads the agent binary
 * — and, on first run, the agent downloads its components (SDR-Trunk runtime,
 * rdio) — from static files served off the site webroot
 * (NODE_DOWNLOADS_BASE, e.g. https://nswpsn.forcequit.xyz/downloads). The
 * binaries aren't secret; the token that authorizes a node lives in the
 * generated agent.yaml, which is what's protected.
 *
 *   GET /api/feeder/me                 (browser, JWT)
 *   GET /api/feeder/download/linux     (browser, JWT) → install-nswpsn-node.sh
 *   GET /api/feeder/download/windows   (browser, JWT) → install-nswpsn-node.ps1
 */
import { Hono } from 'hono';
import type { MiddlewareHandler } from 'hono';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { z } from 'zod';
import { requireSupabaseJwt } from '../services/auth/supabaseJwt.js';
import { hasRole } from '../services/auth/roles.js';
import { mintNodeToken, resolveNodeToken, _clearNodeTokenCache } from '../services/auth/nodeToken.js';
import {
  listNodesForUser,
  createNode,
  getNode,
  rotateNodeToken,
  deleteNode,
  countNodesForUser,
  MAX_NODES_PER_USER,
  isNodeKind,
  autoNodeName,
  type NodeRow,
} from '../services/nodes/registry.js';
import { getUsername } from './users.js';
import { hub } from '../services/nodes/hub.js';

export const feederRouter = new Hono();

// The URL the installed node agent connects back to (API/WebSocket).
const SERVER_URL = config.PUBLIC_BASE_URL ?? 'https://api.forcequit.xyz';

// Where the agent binary + components are served as static files (site
// webroot). The install scripts fetch the agent from here; the agent fetches
// its components (per node-versions.json) from here too.
const DOWNLOADS_BASE = config.NODE_DOWNLOADS_BASE.replace(/\/$/, '');

/** Safely embed a value inside a bash single-quoted string. */
function shSingleQuote(v: string): string {
  return `'${v.replace(/'/g, `'\\''`)}'`;
}

/** Safely embed a value inside a PowerShell single-quoted string. */
function psSingleQuote(v: string): string {
  return `'${v.replace(/'/g, `''`)}'`;
}

const requireRadioContributor: MiddlewareHandler = async (c, next) => {
  const userId = c.get('userId');
  if (!userId || !(await hasRole(userId, ['radio_contributor']))) {
    return c.json({ error: 'not a radio contributor' }, 403);
  }
  await next();
};

feederRouter.use('/api/feeder/*', requireSupabaseJwt, requireRadioContributor);

/** The volunteer-facing view of one of their nodes (name/type/key-prefix +
 *  live activity). No secrets — only the token PREFIX, never the token/hash. */
function feederNodeView(n: NodeRow) {
  const online = hub.isOnline(n.id);
  const live = hub.liveStatus(n.id);
  const st = live.status;
  const sdrUp = !!(
    st &&
    (st.components?.['sdrtrunk'] ||
      (Array.isArray(st.channels) && st.channels.length > 0) ||
      (Array.isArray(st.tuners) && st.tuners.length > 0))
  );
  const decoding =
    online &&
    sdrUp &&
    Array.isArray(st?.channels) &&
    st!.channels.some((c) => {
      const ch = c as { processing?: boolean; state?: string };
      return (
        ch.processing === true ||
        ['CONTROL', 'CALL', 'ACTIVE', 'DATA'].includes(String(ch.state ?? '').toUpperCase())
      );
    });
  const callsLast10m = hub.uploadsInWindow(n.id);
  return {
    id: n.id,
    kind: n.kind,
    installId: n.install_id,
    name: n.name,
    enabled: n.enabled,
    feedEnabled: n.feed_enabled,
    tokenPrefix: n.token_prefix,
    online,
    lastSeenAt: n.last_seen_at,
    agentVersion: n.agent_version,
    sdrtrunkVersion: n.sdrtrunk_version,
    rdioVersion: n.rdio_version,
    sdrUp,
    decoding: !!decoding,
    uploading: callsLast10m > 0,
    callsLast10m,
    queueDepth: typeof st?.queueDepth === 'number' ? st.queueDepth : null,
    calibrated: st?.calibrated ?? null,
    jmbeInstalled: st?.jmbeInstalled ?? null,
  };
}

// ---------------------------------------------------------------------------
// GET /api/feeder/me — the caller's nodes (no token minting).
// ---------------------------------------------------------------------------
feederRouter.get('/api/feeder/me', async (c) => {
  const userId = c.get('userId') as string;
  try {
    const nodes = (await listNodesForUser(userId)).map(feederNodeView);
    return c.json({ role: true, nodes });
  } catch (err) {
    log.error({ err, userId }, 'Error building feeder me');
    return c.json({ error: 'Failed to load feeder info' }, 500);
  }
});

// ---------------------------------------------------------------------------
// POST /api/feeder/nodes — the contributor creates their own node (name + type).
// Mints the node's own token, returned ONCE (bake into the installer now).
// ---------------------------------------------------------------------------
const CreateNodeSchema = z.object({
  // Optional — nodes are auto-named {kind}-{user}-{uuid} when no name is given.
  name: z.string().max(120).optional(),
  kind: z.string().refine(isNodeKind, 'invalid node kind'),
});
feederRouter.post('/api/feeder/nodes', async (c) => {
  const userId = c.get('userId') as string;
  try {
    const parsed = CreateNodeSchema.safeParse(await c.req.json().catch(() => ({})));
    if (!parsed.success) {
      return c.json({ error: 'invalid body', details: parsed.error.issues }, 400);
    }
    if ((await countNodesForUser(userId)) >= MAX_NODES_PER_USER) {
      return c.json({ error: 'node limit reached' }, 429);
    }
    const name = parsed.data.name?.trim() || autoNodeName(parsed.data.kind, await getUsername(userId));
    const { token, tokenHash, tokenPrefix } = mintNodeToken();
    const node = await createNode(userId, name, parsed.data.kind, tokenHash, tokenPrefix);
    if (!node) return c.json({ error: 'registry unavailable' }, 503);
    c.header('Cache-Control', 'no-store');
    return c.json({ node: feederNodeView(node), token });
  } catch (err) {
    log.error({ err, userId }, 'Error creating feeder node');
    return c.json({ error: 'Failed to create node' }, 500);
  }
});

// ---- Linux: one self-contained installer, token baked in --------------------
function linuxInstaller(token: string, kind: string): string {
  const T = shSingleQuote(token);
  const S = shSingleQuote(SERVER_URL);
  const D = shSingleQuote(DOWNLOADS_BASE);
  const K = shSingleQuote(kind);
  return [
    `#!/usr/bin/env bash`,
    `# NSW PSN feeder node installer. Your node token is baked in below.`,
    `# Run:  sudo bash install-nswpsn-node.sh    (re-run any time to update)`,
    `set -euo pipefail`,
    `NODE_TOKEN=${T}`,
    `SERVER_URL=${S}`,
    `DOWNLOADS=${D}`,
    `NODE_KIND=${K}`,
    ``,
    `if [ "$(id -u)" -ne 0 ]; then exec sudo -E NODE_TOKEN="$NODE_TOKEN" SERVER_URL="$SERVER_URL" DOWNLOADS="$DOWNLOADS" NODE_KIND="$NODE_KIND" bash "$0" "$@"; fi`,
    `case "$(uname -m)" in`,
    `  x86_64) ARCH=amd64;;`,
    `  aarch64|arm64) ARCH=arm64;;`,
    `  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1;;`,
    `esac`,
    ``,
    `install -d /opt/nswpsn-node /etc/nswpsn-node /var/lib/nswpsn-node`,
    `id nswpsn-node >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin -G plugdev nswpsn-node || useradd -r -s /usr/sbin/nologin nswpsn-node`,
    ``,
    `# Stop any running service first: on a re-run the binary is in use, and`,
    `# writing straight to it would fail with ETXTBSY (can't truncate a running`,
    `# exe). Harmless when nothing is installed yet.`,
    `systemctl stop nswpsn-node 2>/dev/null || true`,
    `echo "Downloading node agent..."`,
    `AGENT_URL="\${DOWNLOADS%/}/nodeagent-linux-\${ARCH}"`,
    `TMP_BIN="$(mktemp)"`,
    `if command -v curl >/dev/null 2>&1; then`,
    `  curl -fsSL "$AGENT_URL" -o "$TMP_BIN"`,
    `elif command -v wget >/dev/null 2>&1; then`,
    `  wget -qO "$TMP_BIN" "$AGENT_URL"`,
    `else`,
    `  echo "error: this installer needs 'curl' or 'wget'. Install one and re-run, e.g.: sudo apt install -y curl" >&2`,
    `  exit 1`,
    `fi`,
    `[ -s "$TMP_BIN" ] || { echo "error: downloaded agent is empty (check network / URL)" >&2; exit 1; }`,
    `# install(1) unlink+creates the target, so it replaces a running exe`,
    `# atomically without ETXTBSY — the running process keeps its old inode.`,
    `install -m 0755 "$TMP_BIN" /opt/nswpsn-node/nodeagent`,
    `rm -f "$TMP_BIN"`,
    `# The service user owns its install dir so self-update can swap the binary`,
    `# in place (rename over the running exe needs write on the dir, not the file).`,
    `chown -R nswpsn-node:nswpsn-node /opt/nswpsn-node`,
    ``,
    `# Preserve an existing install_id across re-runs.`,
    `INSTALL_ID=""`,
    `if [ -f /etc/nswpsn-node/agent.yaml ]; then`,
    `  INSTALL_ID="$(sed -n 's/^install_id:[[:space:]]*"\\{0,1\\}\\([^"]*\\)"\\{0,1\\}/\\1/p' /etc/nswpsn-node/agent.yaml | head -n1)"`,
    `fi`,
    `cat > /etc/nswpsn-node/agent.yaml <<YAML`,
    `server_url: "\${SERVER_URL}"`,
    `node_token: "\${NODE_TOKEN}"`,
    `install_id: "\${INSTALL_ID}"`,
    `kind: "\${NODE_KIND}"`,
    `data_dir: "/var/lib/nswpsn-node"`,
    `# SDR-Trunk + rdio are core to a feeder node and always run — the agent`,
    `# downloads + launches them from the release manifest on first run. No opt-in.`,
    `YAML`,
    `# The agent writes install_id back into agent.yaml on first run (atomic`,
    `# temp+rename), so the service user needs write on the config DIR, not just`,
    `# the file. Own the whole dir; keep the token file group-private.`,
    `chown -R nswpsn-node:nswpsn-node /etc/nswpsn-node`,
    `chmod 0750 /etc/nswpsn-node && chmod 0640 /etc/nswpsn-node/agent.yaml`,
    `chown -R nswpsn-node:nswpsn-node /var/lib/nswpsn-node`,
    ``,
    `cat > /etc/udev/rules.d/99-nswpsn-sdr.rules <<'RULES'`,
    `# RTL-SDR and common SDRs — grant plugdev access`,
    `SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2838", GROUP="plugdev", MODE="0660"`,
    `SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2832", GROUP="plugdev", MODE="0660"`,
    `SUBSYSTEM=="usb", ATTRS{idVendor}=="1d50", GROUP="plugdev", MODE="0660"`,
    `SUBSYSTEM=="usb", ATTRS{idVendor}=="03eb", GROUP="plugdev", MODE="0660"`,
    `RULES`,
    `udevadm control --reload-rules 2>/dev/null || true; udevadm trigger 2>/dev/null || true`,
    ``,
    `cat > /etc/systemd/system/nswpsn-node.service <<UNIT`,
    `[Unit]`,
    `Description=NSW PSN radio feeder node agent`,
    `After=network-online.target`,
    `Wants=network-online.target`,
    `[Service]`,
    `Type=simple`,
    `User=nswpsn-node`,
    `SupplementaryGroups=plugdev`,
    `ExecStart=/opt/nswpsn-node/nodeagent run --config /etc/nswpsn-node/agent.yaml`,
    `Restart=always`,
    `RestartSec=5`,
    `NoNewPrivileges=true`,
    `ProtectSystem=strict`,
    `# /opt/nswpsn-node must be writable so the agent can self-update its own`,
    `# binary in place (stage nodeagent.pending + swap) under ProtectSystem=strict.`,
    `ReadWritePaths=/opt/nswpsn-node /var/lib/nswpsn-node /etc/nswpsn-node`,
    `ProtectHome=true`,
    `PrivateTmp=true`,
    `[Install]`,
    `WantedBy=multi-user.target`,
    `UNIT`,
    `systemctl daemon-reload`,
    `systemctl enable --now nswpsn-node.service`,
    `echo "Done. The node is installed and starting. Check: systemctl status nswpsn-node"`,
    ``,
  ].join('\n');
}

// ---- Windows: one self-contained PowerShell installer, token baked in -------
function windowsInstaller(token: string, kind: string): string {
  const T = psSingleQuote(token);
  const S = psSingleQuote(SERVER_URL);
  const D = psSingleQuote(DOWNLOADS_BASE);
  const K = psSingleQuote(kind);
  return [
    `# NSW PSN radio feeder node installer. Your node token is baked in below.`,
    `# Right-click this file -> Run with PowerShell (it will elevate).`,
    `# Re-run any time to update.`,
    `$ErrorActionPreference = 'Stop'`,
    `$NodeToken = ${T}`,
    `$ServerUrl = (${S}).TrimEnd('/')`,
    `$Downloads = (${D}).TrimEnd('/')`,
    `$NodeKind = ${K}`,
    ``,
    `# Elevate if not admin.`,
    `$id = [Security.Principal.WindowsIdentity]::GetCurrent()`,
    `$p = New-Object Security.Principal.WindowsPrincipal($id)`,
    `if (-not $p.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)) {`,
    `  Start-Process powershell -Verb RunAs -ArgumentList @('-ExecutionPolicy','Bypass','-File',$PSCommandPath); exit`,
    `}`,
    ``,
    `$dir  = Join-Path $env:ProgramFiles 'NSWPSN Node'`,
    `$data = Join-Path $env:ProgramData 'NSWPSN Node'`,
    `New-Item -ItemType Directory -Force -Path $dir, $data | Out-Null`,
    ``,
    `Write-Host 'Downloading node agent...'`,
    `Invoke-WebRequest -Uri "$Downloads/nodeagent-windows-amd64.exe" -OutFile (Join-Path $dir 'nodeagent.exe') -UseBasicParsing`,
    ``,
    `# Preserve an existing install_id across re-runs.`,
    `$cfgPath = Join-Path $data 'agent.yaml'`,
    `$installId = ''`,
    `if (Test-Path $cfgPath) {`,
    `  $m = Select-String -Path $cfgPath -Pattern '^install_id:\\s*"?([^"]*)"?' | Select-Object -First 1`,
    `  if ($m) { $installId = $m.Matches[0].Groups[1].Value }`,
    `}`,
    `$dataFwd = $data -replace '\\\\','/'`,
    `$yaml = @"`,
    `server_url: "$ServerUrl"`,
    `node_token: "$NodeToken"`,
    `install_id: "$installId"`,
    `kind: "$NodeKind"`,
    `data_dir: "$dataFwd/data"`,
    `"@`,
    `Set-Content -Path $cfgPath -Value $yaml -Encoding UTF8`,
    ``,
    `& (Join-Path $dir 'nodeagent.exe') install --config $cfgPath`,
    `Write-Host 'Installed. The NSWPSN Node service is registered and starting.'`,
    ``,
  ].join('\r\n');
}

/** Confirm a node belongs to the calling contributor. Returns the node or null. */
async function ownedNode(c: import('hono').Context): Promise<NodeRow | null> {
  const userId = c.get('userId') as string;
  const id = c.req.param('id');
  if (!id) return null;
  const node = await getNode(id);
  return node && node.user_id === userId ? node : null;
}

// ---------------------------------------------------------------------------
// POST /api/feeder/nodes/:id/rotate-token — the owner re-issues their node's
// token (e.g. to re-download the installer, since the plaintext isn't stored).
// Returns the new token ONCE; the old one stops working.
// ---------------------------------------------------------------------------
feederRouter.post('/api/feeder/nodes/:id/rotate-token', async (c) => {
  const node = await ownedNode(c);
  if (!node) return c.json({ error: 'not your node' }, 404);
  try {
    const { token, tokenHash, tokenPrefix } = mintNodeToken();
    await rotateNodeToken(node.id, tokenHash, tokenPrefix);
    _clearNodeTokenCache();
    hub.forceDisconnectAgent(node.id, 'token rotated');
    c.header('Cache-Control', 'no-store');
    return c.json({ token });
  } catch (err) {
    log.error({ err, id: node.id }, 'Error rotating feeder node token');
    return c.json({ error: 'Failed to rotate token' }, 500);
  }
});

// ---------------------------------------------------------------------------
// DELETE /api/feeder/nodes/:id — the owner removes (hard-revokes) their node.
// ---------------------------------------------------------------------------
feederRouter.delete('/api/feeder/nodes/:id', async (c) => {
  const node = await ownedNode(c);
  if (!node) return c.json({ error: 'not your node' }, 404);
  try {
    hub.forceDisconnectAgent(node.id);
    await deleteNode(node.id);
    _clearNodeTokenCache();
    return c.json({ ok: true });
  } catch (err) {
    log.error({ err, id: node.id }, 'Error deleting feeder node');
    return c.json({ error: 'Failed to delete node' }, 500);
  }
});

// Installer download is POST with the node token in the BODY (never a URL/query,
// so it can't leak via logs/referrer). The token is not re-derivable, so the
// caller passes the plaintext it got at create/rotate time. We resolve it to the
// node (which also confirms ownership + role) and bake token + kind in.
const DownloadSchema = z.object({ token: z.string().min(1) });

async function serveInstaller(c: import('hono').Context, os: 'linux' | 'windows') {
  const userId = c.get('userId') as string;
  const parsed = DownloadSchema.safeParse(await c.req.json().catch(() => ({})));
  if (!parsed.success) return c.json({ error: 'token required' }, 400);
  const token = parsed.data.token;
  const resolved = await resolveNodeToken(token);
  if (!resolved.ok) return c.json({ error: 'invalid node token' }, 401);
  if (resolved.userId !== userId) return c.json({ error: 'not your node' }, 403);
  const body = os === 'linux' ? linuxInstaller(token, resolved.kind) : windowsInstaller(token, resolved.kind);
  return c.body(body, 200, {
    'Content-Type': os === 'linux' ? 'text/x-shellscript' : 'text/plain; charset=utf-8',
    'Content-Disposition': `attachment; filename="install-nswpsn-node.${os === 'linux' ? 'sh' : 'ps1'}"`,
    'Cache-Control': 'no-store',
  });
}

feederRouter.post('/api/feeder/download/linux', async (c) => {
  try {
    return await serveInstaller(c, 'linux');
  } catch (err) {
    log.error({ err }, 'Error building linux installer');
    return c.json({ error: 'Failed to build installer' }, 500);
  }
});

feederRouter.post('/api/feeder/download/windows', async (c) => {
  try {
    return await serveInstaller(c, 'windows');
  } catch (err) {
    log.error({ err }, 'Error building windows installer');
    return c.json({ error: 'Failed to build installer' }, 500);
  }
});
