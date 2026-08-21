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
  setNodeLocation,
  countNodesForUser,
  MAX_NODES_PER_USER,
  isNodeKind,
  autoNodeName,
  type NodeRow,
} from '../services/nodes/registry.js';
import { getUsername } from './users.js';
import { hub } from '../services/nodes/hub.js';
import { getZoneGroups, isValidZone } from '../services/nodes/rfsZones.js';
import { getPool } from '../db/pool.js';
import { feederRadioStats } from './node-data.js';

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

// The feeder API is open to EITHER contributor role; the specific role a node
// KIND needs is checked per-action (see roleForKind + the create handler).
const requireContributor: MiddlewareHandler = async (c, next) => {
  const userId = c.get('userId');
  if (!userId || !(await hasRole(userId, ['feeder:radio', 'feeder:pager']))) {
    return c.json({ error: 'not a feeder contributor' }, 403);
  }
  await next();
};

feederRouter.use('/api/feeder/*', requireSupabaseJwt, requireContributor);

/** The contributor role that gates a given node kind: pager nodes need
 *  feeder:pager; radio (and adsb, until it has its own role) need feeder:radio.
 *  Mirrored in resolveNodeToken so the ongoing gate matches. */
export function roleForKind(kind: string): 'feeder:radio' | 'feeder:pager' {
  return kind === 'pager' ? 'feeder:pager' : 'feeder:radio';
}

/** The volunteer-facing view of one of their nodes (name/type/key-prefix +
 *  live activity). No secrets — only the token PREFIX, never the token/hash. */
function feederNodeView(n: NodeRow) {
  const online = hub.isOnline(n.id);
  const live = hub.liveStatus(n.id);
  const st = live.status;
  const callsLast10m = hub.uploadsInWindow(n.id);
  const isPager = n.kind === 'pager';

  // Pager nodes have no SDR-Trunk channels/tuners: their "up/decoding" signal is
  // how many POCSAG readers are running (reported as components "reader:<label>").
  const readersUp = st?.components
    ? Object.entries(st.components).filter(
        ([k, v]) => k.startsWith('reader') && String(v).toLowerCase().includes('run'),
      ).length
    : 0;

  const sdrUp = isPager
    ? readersUp > 0
    : !!(
        st &&
        (st.components?.['sdrtrunk'] ||
          (Array.isArray(st.channels) && st.channels.length > 0) ||
          (Array.isArray(st.tuners) && st.tuners.length > 0))
      );

  const decoding = isPager
    ? online && readersUp > 0
    : online &&
      sdrUp &&
      Array.isArray(st?.channels) &&
      st!.channels.some((c) => {
        const ch = c as { processing?: boolean; state?: string };
        return (
          ch.processing === true ||
          ['CONTROL', 'CALL', 'ACTIVE', 'DATA'].includes(String(ch.state ?? '').toUpperCase())
        );
      });
  return {
    id: n.id,
    kind: n.kind,
    installId: n.install_id,
    name: n.name,
    enabled: n.enabled,
    feedEnabled: n.feed_enabled,
    tokenPrefix: n.token_prefix,
    lat: n.lat,
    lon: n.lon,
    zone: n.zone,
    online,
    lastSeenAt: n.last_seen_at,
    agentVersion: n.agent_version,
    sdrtrunkVersion: n.sdrtrunk_version,
    rdioVersion: n.rdio_version,
    sdrUp,
    decoding: !!decoding,
    uploading: callsLast10m > 0,
    callsLast10m,
    // Pager alias for callsLast10m (messages relayed to Pagermon in 10 min) +
    // running-reader count, for the pager card.
    messagesLast10m: callsLast10m,
    readersUp: isPager ? readersUp : null,
    queueDepth: typeof st?.queueDepth === 'number' ? st.queueDepth : null,
    // Calls accepted from the node's local rdio that it could not persist —
    // lost, since rdio does not retry a downstream. queueDepth stays 0 for
    // them, so without this the owner's card shows a node shedding every call
    // as perfectly healthy. Null on agents older than 0.2.20.
    uploadsDropped: typeof st?.uploadsDropped === 'number' ? st.uploadsDropped : null,
    calibrated: st?.calibrated ?? null,
    jmbeInstalled: st?.jmbeInstalled ?? null,
    // Self-update in progress — the feeder card shows "updating" (not offline)
    // across the agent's swap/re-exec disconnect.
    updating: hub.isUpdating(n.id),
    // Pager: which frequency readers are decoding right now (RFS / FRNSW / both),
    // so the owner can see coverage on their own card.
    pagerDecoding: isPager ? hub.pagerDecoding(n.id) : null,
  };
}

// ---------------------------------------------------------------------------
// GET /api/feeder/me — the caller's nodes (no token minting).
// ---------------------------------------------------------------------------
feederRouter.get('/api/feeder/me', async (c) => {
  const userId = c.get('userId') as string;
  try {
    const nodes = (await listNodesForUser(userId)).map(feederNodeView);
    // Which node kinds this user may create, by role — so the UI can offer only
    // what they're allowed (backend still enforces it on create).
    const [radio, pager] = await Promise.all([
      hasRole(userId, ['feeder:radio']),
      hasRole(userId, ['feeder:pager']),
    ]);
    return c.json({ role: true, nodes, canCreate: { radio, pager } });
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
  // Nodes are always auto-named {kind}-{user}-{uuid}. A coarse RFS `zone` is
  // REQUIRED at creation; the exact antenna pin (lat/lon) stays optional and is
  // set/updated separately via PUT .../location.
  kind: z.string().refine(isNodeKind, 'invalid node kind'),
  zone: z.string().min(1).refine(isValidZone, 'unknown zone'),
});
feederRouter.post('/api/feeder/nodes', async (c) => {
  const userId = c.get('userId') as string;
  try {
    const parsed = CreateNodeSchema.safeParse(await c.req.json().catch(() => ({})));
    if (!parsed.success) {
      return c.json({ error: 'invalid body', details: parsed.error.issues }, 400);
    }
    // Creating a node of a given kind is locked to that kind's contributor role:
    // radio (PSN) → radio_contributor, pager → pager_contributor.
    const needed = roleForKind(parsed.data.kind);
    if (!(await hasRole(userId, [needed]))) {
      return c.json({ error: `creating a ${parsed.data.kind} node requires the ${needed} role` }, 403);
    }
    if ((await countNodesForUser(userId)) >= MAX_NODES_PER_USER) {
      return c.json({ error: 'node limit reached' }, 429);
    }
    const name = autoNodeName(parsed.data.kind, await getUsername(userId));
    const { token, tokenHash, tokenPrefix } = mintNodeToken();
    const node = await createNode(userId, name, parsed.data.kind, tokenHash, tokenPrefix, parsed.data.zone);
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
  if (kind === 'pager') return pagerLinuxInstaller(token);
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

// ---- Linux PAGER node installer (rtl_fm | multimon-ng POCSAG → Pagermon) -----
// Linux/amd64+arm64 only. Installs rtl-sdr + multimon-ng, the pager agent binary
// (served as nodeagent-pager-linux-<arch>), udev rules + the RTL kernel-module
// blacklist, and a systemd unit. The agent auto-detects RTL dongles, assigns
// distinct EEPROM serials ONLY when they collide, and runs one POCSAG reader per
// frequency (1 SDR → NSW RFS; 2 → also Fire & Rescue NSW).
function pagerLinuxInstaller(token: string): string {
  const T = shSingleQuote(token);
  const S = shSingleQuote(SERVER_URL);
  const D = shSingleQuote(DOWNLOADS_BASE);
  return [
    `#!/usr/bin/env bash`,
    `# NSW PSN PAGER feeder node installer. Your node token is baked in below.`,
    `# Run:  sudo bash install-nswpsn-node.sh    (re-run any time to update)`,
    `set -euo pipefail`,
    `NODE_TOKEN=${T}`,
    `SERVER_URL=${S}`,
    `DOWNLOADS=${D}`,
    `NODE_KIND='pager'`,
    ``,
    `if [ "$(id -u)" -ne 0 ]; then exec sudo -E NODE_TOKEN="$NODE_TOKEN" SERVER_URL="$SERVER_URL" DOWNLOADS="$DOWNLOADS" bash "$0" "$@"; fi`,
    `case "$(uname -m)" in`,
    `  x86_64) ARCH=amd64;;`,
    `  aarch64|arm64) ARCH=arm64;;`,
    `  *) echo "unsupported architecture: $(uname -m) (pager nodes are amd64/arm64)" >&2; exit 1;;`,
    `esac`,
    ``,
    `# Decode toolchain: rtl-sdr (rtl_fm, rtl_test, rtl_eeprom) + multimon-ng.`,
    `echo "Installing rtl-sdr + multimon-ng..."`,
    `if command -v apt-get >/dev/null 2>&1; then`,
    `  export DEBIAN_FRONTEND=noninteractive`,
    `  apt-get update -qq || true`,
    `  apt-get install -y --no-install-recommends rtl-sdr multimon-ng curl`,
    `else`,
    `  echo "warning: no apt-get — install 'rtl-sdr' and 'multimon-ng' manually, then re-run." >&2`,
    `fi`,
    ``,
    `install -d /opt/nswpsn-node /etc/nswpsn-node /var/lib/nswpsn-node`,
    `id nswpsn-node >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin -G plugdev nswpsn-node || useradd -r -s /usr/sbin/nologin nswpsn-node`,
    ``,
    `systemctl stop nswpsn-node 2>/dev/null || true`,
    `echo "Downloading pager node agent..."`,
    `AGENT_URL="\${DOWNLOADS%/}/nodeagent-pager-linux-\${ARCH}"`,
    `TMP_BIN="$(mktemp)"`,
    `if command -v curl >/dev/null 2>&1; then`,
    `  curl -fsSL "$AGENT_URL" -o "$TMP_BIN"`,
    `elif command -v wget >/dev/null 2>&1; then`,
    `  wget -qO "$TMP_BIN" "$AGENT_URL"`,
    `else`,
    `  echo "error: this installer needs 'curl' or 'wget'." >&2; exit 1`,
    `fi`,
    `[ -s "$TMP_BIN" ] || { echo "error: downloaded agent is empty (check network / URL)" >&2; exit 1; }`,
    `install -m 0755 "$TMP_BIN" /opt/nswpsn-node/nodeagent`,
    `rm -f "$TMP_BIN"`,
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
    `kind: "pager"`,
    `data_dir: "/var/lib/nswpsn-node"`,
    `# Loopback relay the local readers POST decoded POCSAG lines to; the agent`,
    `# parses + relays them to the backend, which forwards to central Pagermon.`,
    `relay_addr: "127.0.0.1:17390"`,
    `# Frequencies + POCSAG rates come from the backend config push; the agent`,
    `# auto-detects SDRs and runs one reader per frequency (1 SDR = NSW RFS only).`,
    `YAML`,
    `chown -R nswpsn-node:nswpsn-node /etc/nswpsn-node`,
    `chmod 0750 /etc/nswpsn-node && chmod 0640 /etc/nswpsn-node/agent.yaml`,
    `chown -R nswpsn-node:nswpsn-node /var/lib/nswpsn-node`,
    ``,
    `# RTL-SDR USB access for the plugdev group (rtl_fm/rtl_eeprom run as the`,
    `# service user). uaccess also grants the active seat.`,
    `cat > /etc/udev/rules.d/99-nswpsn-sdr.rules <<'RULES'`,
    `SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2838", GROUP="plugdev", MODE="0660", TAG+="uaccess"`,
    `SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2832", GROUP="plugdev", MODE="0660", TAG+="uaccess"`,
    `RULES`,
    `udevadm control --reload-rules 2>/dev/null || true; udevadm trigger 2>/dev/null || true`,
    ``,
    `# The DVB-T kernel driver claims RTL dongles and blocks rtl_fm — blacklist it.`,
    `cat > /etc/modprobe.d/blacklist-nswpsn-rtl.conf <<'BL'`,
    `blacklist dvb_usb_rtl28xxu`,
    `blacklist rtl2832`,
    `blacklist rtl2830`,
    `BL`,
    `modprobe -r dvb_usb_rtl28xxu 2>/dev/null || true`,
    ``,
    `cat > /etc/systemd/system/nswpsn-node.service <<UNIT`,
    `[Unit]`,
    `Description=NSW PSN pager feeder node agent`,
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
    `ReadWritePaths=/opt/nswpsn-node /var/lib/nswpsn-node /etc/nswpsn-node`,
    `ProtectHome=true`,
    `PrivateTmp=true`,
    `[Install]`,
    `WantedBy=multi-user.target`,
    `UNIT`,
    `systemctl daemon-reload`,
    `systemctl enable --now nswpsn-node.service`,
    `echo "Done. The pager node is installed and starting. Check: systemctl status nswpsn-node"`,
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
// GET /api/feeder/nodes/:id/stats?window=24h|7d|30d — a LIGHT per-node summary
// (calls / receptions + top talkgroup / unit / site + a short recent-activity
// feed) so a contributor can see what their OWN radio node is hearing, without
// the staff Data page's full drill-downs. Owner-scoped (radio_contributor), NOT
// gated on any staff/team role.
// ---------------------------------------------------------------------------
feederRouter.get('/api/feeder/nodes/:id/stats', async (c) => {
  const node = await ownedNode(c);
  if (!node) return c.json({ error: 'not your node' }, 404);
  // Radio-only concepts (talkgroups/units/sites). Pager nodes have their own
  // Messages view instead.
  if (node.kind !== 'radio') return c.json({ error: 'stats are radio-only' }, 400);
  try {
    const pool = await getPool();
    if (!pool) return c.json({ error: 'database unavailable' }, 503);
    const url = new URL(c.req.url);
    const wRaw = (url.searchParams.get('window') ?? '24h').toLowerCase();
    const window = (['24h', '7d', '30d'] as const).find((w) => w === wRaw) ?? '24h';
    const stats = await feederRadioStats(pool, node.id, window);
    return c.json(stats);
  } catch (err) {
    log.error({ err, id: node.id }, 'Error building feeder node stats');
    return c.json({ error: 'Failed to load stats' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/feeder/zones — the RFS zone list (grouped by Area Command) that backs
// the location picker's required zone dropdown. Reference data; any logged-in
// user (feeder or staff) may read it.
// ---------------------------------------------------------------------------
feederRouter.get('/api/feeder/zones', (c) => {
  c.header('Cache-Control', 'public, max-age=3600');
  return c.json({ groups: getZoneGroups() });
});

// ---------------------------------------------------------------------------
// PUT /api/feeder/nodes/:id/location — set the node's location: its REQUIRED RFS
// `zone` (coarse area) plus the OPTIONAL exact antenna pin (lat/lon for coverage
// + channel tuning). Pass null lat/lon for "zone only".
// ---------------------------------------------------------------------------
const LocationSchema = z.object({
  zone: z.string().min(1).refine(isValidZone, 'unknown zone'),
  lat: z.number().min(-90).max(90).nullable(),
  lon: z.number().min(-180).max(180).nullable(),
});
feederRouter.put('/api/feeder/nodes/:id/location', async (c) => {
  const node = await ownedNode(c);
  if (!node) return c.json({ error: 'not your node' }, 404);
  const parsed = LocationSchema.safeParse(await c.req.json().catch(() => ({})));
  if (!parsed.success) return c.json({ error: 'invalid location' }, 400);
  try {
    const updated = await setNodeLocation(node.id, parsed.data);
    return c.json({ node: updated ? feederNodeView(updated) : null });
  } catch (err) {
    log.error({ err, id: node.id }, 'Error setting node location');
    return c.json({ error: 'Failed to set location' }, 500);
  }
});

// ---------------------------------------------------------------------------
// GET /api/feeder/nodes/:id/messages — the owner views their pager node's recent
// decoded pages (the same rolling buffer staff see in the drawer). Owner-gated;
// pager nodes only (radio nodes have no decoded-message buffer).
// ---------------------------------------------------------------------------
feederRouter.get('/api/feeder/nodes/:id/messages', async (c) => {
  const node = await ownedNode(c);
  if (!node) return c.json({ error: 'not your node' }, 404);
  if (node.kind !== 'pager') return c.json({ messages: [] });
  c.header('Cache-Control', 'no-store');
  return c.json({ messages: hub.recentPagerMessages(node.id) });
});

// ---------------------------------------------------------------------------
// DELETE /api/feeder/nodes/:id — the owner removes (hard-revokes) their node.
// ---------------------------------------------------------------------------
feederRouter.delete('/api/feeder/nodes/:id', async (c) => {
  const node = await ownedNode(c);
  if (!node) return c.json({ error: 'not your node' }, 404);
  try {
    hub.forceDisconnectAgent(node.id);
    hub.clearNode(node.id);
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
  // Pager nodes are Linux-only (rtl_fm | multimon-ng | reader.sh is a bash stack).
  if (resolved.kind === 'pager' && os === 'windows') {
    return c.json({ error: 'pager nodes are Linux only' }, 400);
  }
  const body = os === 'linux' ? linuxInstaller(token, resolved.kind) : windowsInstaller(token, resolved.kind);
  const ext = os === 'linux' ? 'sh' : 'ps1';
  return c.body(body, 200, {
    'Content-Type': os === 'linux' ? 'text/x-shellscript' : 'text/plain; charset=utf-8',
    // Name the file by node kind so a radio vs pager installer is identifiable.
    'Content-Disposition': `attachment; filename="install-nswpsn-${resolved.kind}-node.${ext}"`,
    'Access-Control-Expose-Headers': 'Content-Disposition',
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
