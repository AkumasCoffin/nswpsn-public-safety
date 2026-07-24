/**
 * Volunteer-facing feeder endpoints (radio_contributor only) + the
 * token-gated node-agent binary download.
 *
 * The backend is the single source of a volunteer's install: it generates
 * ONE self-contained installer per OS with the caller's feeder token baked in
 * (no bootstrap, no second download of an installer), and it serves the agent
 * binary itself. The generated installer downloads the agent from this backend
 * and registers the service. Only the big SDR-Trunk runtime is fetched from a
 * release/CDN later, by the agent, on first run.
 *
 *   GET /api/feeder/me                 (browser, JWT)
 *   GET /api/feeder/download/linux     (browser, JWT) → install-nswpsn-node.sh
 *   GET /api/feeder/download/windows   (browser, JWT) → install-nswpsn-node.ps1
 *   GET /api/node-agent/:platform      (installer, feeder token) → raw binary
 */
import { Hono } from 'hono';
import type { MiddlewareHandler } from 'hono';
import { createReadStream, existsSync, statSync } from 'node:fs';
import { join, isAbsolute, resolve } from 'node:path';
import { Readable } from 'node:stream';
import { config } from '../config.js';
import { log } from '../lib/log.js';
import { requireSupabaseJwt } from '../services/auth/supabaseJwt.js';
import { hasRole } from '../services/auth/roles.js';
import {
  feederTokensConfigured,
  mintFeederToken,
  resolveFeederToken,
} from '../services/auth/nodeToken.js';
import { listNodesForUser } from '../services/nodes/registry.js';
import { hub } from '../services/nodes/hub.js';

export const feederRouter = new Hono();
export const nodeAgentRouter = new Hono();

// The URL the installed node agent connects back to (and downloads its binary
// from). Prefer the configured public base; fall back to production.
const SERVER_URL = config.PUBLIC_BASE_URL ?? 'https://api.forcequit.xyz';

// Supported agent platforms → the binary filename in NODE_AGENT_DIST_DIR.
const AGENT_BINARIES: Record<string, string> = {
  'windows-amd64': 'nodeagent-windows-amd64.exe',
  'linux-amd64': 'nodeagent-linux-amd64',
  'linux-arm64': 'nodeagent-linux-arm64',
};

/** Safely embed a value inside a bash single-quoted string. */
function shSingleQuote(v: string): string {
  return `'${v.replace(/'/g, `'\\''`)}'`;
}

/** Safely embed a value inside a PowerShell single-quoted string. */
function psSingleQuote(v: string): string {
  return `'${v.replace(/'/g, `''`)}'`;
}

// ---------------------------------------------------------------------------
// feeder (browser, JWT + radio_contributor)
// ---------------------------------------------------------------------------
const requireRadioContributor: MiddlewareHandler = async (c, next) => {
  const userId = c.get('userId');
  if (!userId || !(await hasRole(userId, ['radio_contributor']))) {
    return c.json({ error: 'not a radio contributor' }, 403);
  }
  await next();
};

feederRouter.use('/api/feeder/*', requireSupabaseJwt, requireRadioContributor);

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

// ---- Linux: one self-contained installer, token baked in --------------------
function linuxInstaller(token: string): string {
  const T = shSingleQuote(token);
  const S = shSingleQuote(SERVER_URL);
  // The agent binary is pulled from THIS backend (token-gated). systemd unit +
  // udev rules are written inline so the single downloaded file is everything.
  return [
    `#!/usr/bin/env bash`,
    `# NSW PSN radio feeder node installer. Your node token is baked in below.`,
    `# Run:  sudo bash install-nswpsn-node.sh    (re-run any time to update)`,
    `set -euo pipefail`,
    `NODE_TOKEN=${T}`,
    `SERVER_URL=${S}`,
    ``,
    `if [ "$(id -u)" -ne 0 ]; then exec sudo -E NODE_TOKEN="$NODE_TOKEN" SERVER_URL="$SERVER_URL" bash "$0" "$@"; fi`,
    `case "$(uname -m)" in`,
    `  x86_64) ARCH=amd64;;`,
    `  aarch64|arm64) ARCH=arm64;;`,
    `  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1;;`,
    `esac`,
    ``,
    `install -d /opt/nswpsn-node /etc/nswpsn-node /var/lib/nswpsn-node`,
    `id nswpsn-node >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin -G plugdev nswpsn-node || useradd -r -s /usr/sbin/nologin nswpsn-node`,
    ``,
    `echo "Downloading node agent..."`,
    `curl -fsSL -H "X-Node-Token: \${NODE_TOKEN}" "\${SERVER_URL%/}/api/node-agent/linux-\${ARCH}" -o /opt/nswpsn-node/nodeagent`,
    `chmod +x /opt/nswpsn-node/nodeagent`,
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
    `data_dir: "/var/lib/nswpsn-node"`,
    `YAML`,
    `chown root:nswpsn-node /etc/nswpsn-node/agent.yaml && chmod 0640 /etc/nswpsn-node/agent.yaml`,
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
    `ReadWritePaths=/var/lib/nswpsn-node /etc/nswpsn-node`,
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
function windowsInstaller(token: string): string {
  const T = psSingleQuote(token);
  const S = psSingleQuote(SERVER_URL);
  return [
    `# NSW PSN radio feeder node installer. Your node token is baked in below.`,
    `# Right-click this file -> Run with PowerShell (it will elevate).`,
    `# Re-run any time to update.`,
    `$ErrorActionPreference = 'Stop'`,
    `$NodeToken = ${T}`,
    `$ServerUrl = (${S}).TrimEnd('/')`,
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
    `Invoke-WebRequest -Uri "$ServerUrl/api/node-agent/windows-amd64" -Headers @{ 'X-Node-Token' = $NodeToken } -OutFile (Join-Path $dir 'nodeagent.exe') -UseBasicParsing`,
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
    `data_dir: "$dataFwd/data"`,
    `"@`,
    `Set-Content -Path $cfgPath -Value $yaml -Encoding UTF8`,
    ``,
    `& (Join-Path $dir 'nodeagent.exe') install --config $cfgPath`,
    `Write-Host 'Installed. The NSWPSN Node service is registered and starting.'`,
    ``,
  ].join('\r\n');
}

feederRouter.get('/api/feeder/download/linux', async (c) => {
  if (!feederTokensConfigured()) return c.json({ error: 'feeder not configured' }, 503);
  const userId = c.get('userId') as string;
  try {
    const { token } = await mintFeederToken(userId);
    return c.body(linuxInstaller(token), 200, {
      'Content-Type': 'text/x-shellscript',
      'Content-Disposition': 'attachment; filename="install-nswpsn-node.sh"',
      // Per-user token inside — never let a CDN/proxy cache and cross-serve it.
      'Cache-Control': 'no-store',
    });
  } catch (err) {
    log.error({ err, userId }, 'Error building linux installer');
    return c.json({ error: 'Failed to build installer' }, 500);
  }
});

feederRouter.get('/api/feeder/download/windows', async (c) => {
  if (!feederTokensConfigured()) return c.json({ error: 'feeder not configured' }, 503);
  const userId = c.get('userId') as string;
  try {
    const { token } = await mintFeederToken(userId);
    return c.body(windowsInstaller(token), 200, {
      'Content-Type': 'text/plain; charset=utf-8',
      'Content-Disposition': 'attachment; filename="install-nswpsn-node.ps1"',
      // Per-user token inside — never let a CDN/proxy cache and cross-serve it.
      'Cache-Control': 'no-store',
    });
  } catch (err) {
    log.error({ err, userId }, 'Error building windows installer');
    return c.json({ error: 'Failed to build installer' }, 500);
  }
});

// ---------------------------------------------------------------------------
// node-agent binary download (hit by the installer script, NOT the browser).
// Auth is the feeder token (the script has it) — this path is exempt from the
// public API-key gate. The binary itself isn't secret; the token in the
// written config is what authorizes the node.
// ---------------------------------------------------------------------------
nodeAgentRouter.get('/api/node-agent/:platform', async (c) => {
  const platform = c.req.param('platform');
  const filename = AGENT_BINARIES[platform];
  if (!filename) return c.json({ error: 'unknown platform' }, 404);

  const token =
    c.req.query('token') ?? c.req.header('X-Node-Token') ?? '';
  const resolved = await resolveFeederToken(token);
  if (!resolved.ok) {
    return c.json({ error: 'unauthorized' }, resolved.reason === 'no_role' ? 403 : 401);
  }

  const base = isAbsolute(config.NODE_AGENT_DIST_DIR)
    ? config.NODE_AGENT_DIST_DIR
    : resolve(process.cwd(), config.NODE_AGENT_DIST_DIR);
  const path = join(base, filename);
  if (!existsSync(path)) {
    log.warn({ platform, path }, 'node-agent binary not present on server');
    return c.json({ error: 'agent binary not published for this platform yet' }, 404);
  }
  const size = statSync(path).size;
  const stream = Readable.toWeb(createReadStream(path)) as unknown as ReadableStream;
  return c.body(stream, 200, {
    'Content-Type': 'application/octet-stream',
    'Content-Length': String(size),
    'Content-Disposition': `attachment; filename="${filename}"`,
    'Cache-Control': 'no-store',
  });
});
