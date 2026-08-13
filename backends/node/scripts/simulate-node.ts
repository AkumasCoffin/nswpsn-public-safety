/**
 * Fake feeder-node agent — drives the real /api/node-ws/agent WebSocket so
 * the backend registry, hub, and staff Nodes tab can be exercised without a
 * real SDR / Go agent.
 *
 * It mints (or reuses) a real feeder token for a given Supabase user id via
 * the same code the server uses, optionally grants that user the
 * radio_contributor role, then connects and behaves like an agent: sends
 * hello + periodic status + occasional events, answers cmd with cmdResult,
 * and streams a few fake spectrum frames on spectrumStart.
 *
 * Run (from backends/node):
 *   npx tsx --env-file-if-exists=../.env scripts/simulate-node.ts --user <uid> --grant-role
 *
 * Options:
 *   --user <id>     Supabase user id to own this node        (required)
 *   --url <ws>      agent WS base URL      (default ws://127.0.0.1:3000)
 *   --install <id>  install id             (default sim-<random>)
 *   --grant-role    INSERT radio_contributor for --user first (dev only)
 *   --once          send one status then exit (smoke test)
 *
 * Requires DATABASE_URL + FEEDER_TOKEN_SECRET in ../.env (same as the server).
 */
import { randomUUID } from 'node:crypto';
import WebSocket from 'ws';
import { mintFeederToken } from '../src/services/auth/nodeToken.js';
import { getPool, closePool } from '../src/db/pool.js';
import { invalidateUserRolesCache } from '../src/services/auth/roles.js';

function arg(name: string, fallback?: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  if (i >= 0 && i + 1 < process.argv.length) return process.argv[i + 1];
  return fallback;
}
function flag(name: string): boolean {
  return process.argv.includes(`--${name}`);
}

const userId = arg('user');
if (!userId) {
  console.error('ERROR: --user <supabaseUserId> is required');
  process.exit(1);
}
const wsBase = (arg('url', 'ws://127.0.0.1:3000') as string).replace(/\/$/, '');
const installId = arg('install', `sim-${randomUUID().slice(0, 8)}`) as string;

function fakeStatus() {
  return {
    tuners: [
      { id: 0, model: 'RTL2838 (sim)', serial: '00000001', frequency: 412_987_500,
        sampleRate: 2_400_000, gain: 28, ppm: 0, state: 'ENABLED' },
    ],
    channels: [
      { name: 'NSW PSN — control', state: 'CONTROL', frequency: 412_987_500,
        from: null, to: null, decodeRate: 100, errorRate: 0.1 },
    ],
    components: { sdrtrunk: 'running', rdio: 'running' },
    queueDepth: 0,
    cpuPct: 12 + Math.floor(Math.random() * 6),
    memMB: 480,
    diskFreeMB: 51_200,
    configVersion: null,
  };
}

function spectrumFrame(tunerId = 0): Buffer {
  const bins = 512;
  const header = Buffer.alloc(1 + 1 + 8 + 4 + 2);
  let o = 0;
  header.writeUInt8(0x01, o); o += 1;
  header.writeUInt8(tunerId, o); o += 1;
  header.writeBigUInt64LE(412_987_500n, o); o += 8;
  header.writeUInt32LE(2_400_000, o); o += 4;
  header.writeUInt16LE(bins, o); o += 2;
  const body = Buffer.alloc(bins);
  for (let i = 0; i < bins; i++) {
    // Noise floor ~ -110 dB with a bump in the middle (int8 dB values).
    const mid = Math.abs(i - bins / 2) < 8 ? 40 : 0;
    body.writeInt8(Math.max(-128, Math.min(127, -110 + mid + Math.floor(Math.random() * 5))), i);
  }
  return Buffer.concat([header, body]);
}

async function main() {
  if (flag('grant-role')) {
    const pool = await getPool();
    if (!pool) { console.error('DATABASE_URL not set — cannot --grant-role'); process.exit(1); }
    await pool.query(
      `INSERT INTO user_roles (user_id, role, granted_by)
       VALUES ($1, 'feeder:radio', 'simulate-node')
       ON CONFLICT (user_id, role) DO NOTHING`,
      [userId],
    );
    invalidateUserRolesCache(userId);
    console.log(`granted radio_contributor to ${userId}`);
  }

  const { token, prefix } = await mintFeederToken(userId);
  console.log(`node token minted (prefix ${prefix}), install=${installId}`);

  const ws = new WebSocket(`${wsBase}/api/node-ws/agent`, {
    headers: { 'X-Node-Token': token, 'X-Node-Install': installId },
  });

  let statusTimer: ReturnType<typeof setInterval> | undefined;

  ws.on('open', () => {
    console.log('WS open → sending hello');
    ws.send(JSON.stringify({
      t: 'hello',
      data: {
        protocolVersion: 1, agentVersion: '0.0.1-sim', sdrtrunkVersion: 'sim',
        rdioVersion: 'sim', os: process.platform, arch: process.arch,
        hostname: `sim-host-${installId}`, appliedConfigVersion: null,
      },
    }));
  });

  ws.on('message', (raw: Buffer, isBinary: boolean) => {
    if (isBinary) return;
    let msg: { t?: string; id?: string; data?: any };
    try { msg = JSON.parse(raw.toString('utf8')); } catch { return; }
    console.log('← recv', msg.t, msg.data ? JSON.stringify(msg.data) : '');

    if (msg.t === 'helloAck') {
      ws.send(JSON.stringify({ t: 'status', data: fakeStatus() }));
      if (flag('once')) { setTimeout(() => { ws.close(); }, 300); return; }
      statusTimer = setInterval(() => {
        ws.send(JSON.stringify({ t: 'status', data: fakeStatus() }));
      }, 5000);
      // Emit a demo event shortly after connecting.
      setTimeout(() => ws.send(JSON.stringify({
        t: 'event', data: { kind: 'callStart', talkgroup: 'RFS Fireground 1', at: Date.now() },
      })), 1500);
    }

    if (msg.t === 'cmd' && msg.id) {
      console.log('  handling cmd:', msg.data?.action);
      ws.send(JSON.stringify({ t: 'cmdResult', id: msg.id, data: { ok: true, message: `sim did ${msg.data?.action}` } }));
    }

    if (msg.t === 'spectrumStart') {
      console.log('  spectrum start → streaming 20 frames');
      let n = 0;
      const spec = setInterval(() => {
        if (n++ >= 20 || ws.readyState !== ws.OPEN) { clearInterval(spec); return; }
        ws.send(spectrumFrame(), { binary: true });
      }, 100);
    }

    if (msg.t === 'disabled') {
      console.log('  server says node disabled — closing');
      ws.close();
    }
  });

  ws.on('close', (code, reason) => {
    if (statusTimer) clearInterval(statusTimer);
    console.log(`WS closed code=${code} reason=${reason.toString()}`);
    void closePool().then(() => process.exit(0));
  });
  ws.on('error', (err) => console.error('WS error:', (err as Error).message));

  process.on('SIGINT', () => { ws.close(); });
}

void main().catch((err) => { console.error(err); process.exit(1); });
