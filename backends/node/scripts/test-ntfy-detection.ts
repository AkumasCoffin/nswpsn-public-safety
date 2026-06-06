/**
 * Preview the rdio → ntfy incident detector against REAL recent
 * transcripts, using the exact same detection + formatting code the live
 * loop runs (imported from src/services/rdioIncidentAlerts.ts) — so what
 * you see here is what subscribers would get.
 *
 * Dry-run by default (prints only). Pass --send to actually publish.
 * Bypasses the cooldown table entirely, so it never touches live state.
 *
 * Run:
 *   npx tsx --env-file-if-exists=../.env scripts/test-ntfy-detection.ts [opts]
 *   # or: npm run ntfy-test -- [opts]
 *
 * Options:
 *   --window N      look-back window in minutes      (default 15)
 *   --threshold N   min calls per talkgroup to show  (default 2)
 *   --talkgroup N   only this talkgroup id
 *   --limit N       max candidates to show           (default 5)
 *   --send          actually publish to ntfy (else dry-run print)
 *   --topic NAME    publish to this topic instead of NTFY_TOPIC
 *                   (use a private test topic so you don't spam subs)
 *
 * The --window/--threshold default LOW so candidates surface even in a
 * quiet period — they're for previewing formatting, not the live
 * thresholds (which come from your .env).
 */
import { config } from '../src/config.js';
import { getRdioPool, closeRdioPool, isRdioConfigured } from '../src/services/rdio.js';
import {
  detectBursts,
  buildNotification,
  analyzeKeywords,
  incidentKeywords,
} from '../src/services/rdioIncidentAlerts.js';
import { publishToNtfy } from '../src/services/rdioIncidentAlerts.js';

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : undefined;
}
function flag(name: string): boolean {
  return process.argv.includes(`--${name}`);
}
function num(name: string, def: number): number {
  const v = arg(name);
  const n = v !== undefined ? Number.parseInt(v, 10) : NaN;
  return Number.isFinite(n) ? n : def;
}

const RULE = '═'.repeat(72);

async function main(): Promise<void> {
  if (!isRdioConfigured()) {
    console.error('✗ RDIO_DATABASE_URL is not set — nothing to read.');
    process.exit(1);
  }
  const windowMin = num('window', 15);
  const threshold = num('threshold', 2);
  const limit = num('limit', 5);
  const talkgroup = arg('talkgroup') ? Number.parseInt(arg('talkgroup')!, 10) : null;
  const send = flag('send');
  const topic = arg('topic') ?? config.NTFY_TOPIC;

  console.log(RULE);
  console.log('rdio → ntfy detection preview');
  console.log(
    `  window=${windowMin}min  threshold=${threshold}  ` +
      `talkgroup=${talkgroup ?? 'any'}  limit=${limit}`,
  );
  console.log(`  live require_keyword=${config.RDIO_ALERT_REQUIRE_KEYWORD}`);
  const kw = incidentKeywords();
  console.log(
    `  keyword list (.env): ${kw.length ? kw.join(', ') : '(none — set RDIO_ALERT_KEYWORDS)'}`,
  );
  console.log(`  mode: ${send ? `SEND → topic "${topic}"` : 'DRY RUN (no push)'}`);
  console.log(RULE);

  const rdio = await getRdioPool();
  if (!rdio) {
    console.error('✗ could not open rdio pool');
    process.exit(1);
  }

  const bursts = await detectBursts(rdio, { windowMin, threshold, talkgroup });
  if (bursts.length === 0) {
    console.log(
      `\nNo talkgroup had ≥ ${threshold} transcribed calls in the last ` +
        `${windowMin} min. Try a larger --window or lower --threshold.`,
    );
    await closeRdioPool();
    return;
  }

  console.log(`\nFound ${bursts.length} bursting talkgroup(s); showing up to ${limit}.\n`);

  const shown = bursts.slice(0, limit);
  for (const b of shown) {
    const notif = await buildNotification(b);
    const { matched } = analyzeKeywords(b.allText);
    const wouldFireLive = !config.RDIO_ALERT_REQUIRE_KEYWORD || matched.length > 0;

    console.log(RULE);
    console.log(`system=${b.system}  talkgroup=${b.talkgroup}  calls=${b.n}`);
    console.log(`matched keywords: ${matched.length ? matched.join(', ') : '(none)'}`);
    console.log(`priority: ${notif.priority}`);
    console.log(
      `would fire live (require_keyword=${config.RDIO_ALERT_REQUIRE_KEYWORD}): ` +
        `${wouldFireLive ? 'YES' : 'NO — no keyword in window'}`,
    );
    console.log('\n──── PUSH TITLE ────');
    console.log(notif.title);
    console.log('\n──── PUSH BODY ────');
    console.log(notif.body);

    if (send) {
      const ok = await publishToNtfy(notif, topic);
      console.log(`\n→ published to "${topic}": ${ok ? 'OK ✓' : 'FAILED ✗'}`);
    }
    console.log('');
  }

  if (!send) {
    console.log(RULE);
    console.log('Dry run — nothing sent. Re-run with --send (optionally');
    console.log('--topic <test-topic>) to push these to your phone.');
  }
  console.log(RULE);

  await closeRdioPool();
}

main().catch((err) => {
  console.error('test-ntfy-detection failed:', err);
  process.exit(1);
});
