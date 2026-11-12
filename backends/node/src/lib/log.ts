/**
 * Structured logger used everywhere in the Node backend.
 *
 * Output style mirrors python's Log class: one short line per event,
 * `[HH:MM:SS] message {key=val …}`. Multi-line pretty-printed JSON
 * (pino-pretty default) was burying real signal under structure noise.
 *
 * Production: still raw ndjson so log shippers (journald/Loki) parse
 * cleanly. Dev / pm2-as-dev: pretty single-line.
 *
 * Usage:
 *   import { log } from './lib/log.js';
 *   log.info({ source: 'rfs', count: 42 }, 'rfs refreshed');
 *
 * The `svc` tag isn't shown in single-line dev output (it's the same
 * value on every line — noise) but stays in the production ndjson.
 */
import pino from 'pino';
import pretty from 'pino-pretty';
import { config } from '../config.js';

/**
 * One colour per request-log client tag (server.ts requestLogger), so a busy
 * tail separates into streams by eye.
 */
const CLIENT_COLOUR: Record<string, string> = {
  browser: '\x1b[94m', // bright blue: people
  'discord-bot': '\x1b[95m', // magenta: the bot's pollers
  node: '\x1b[92m', // green: feeder agents
  rdio: '\x1b[38;5;208m', // orange: rdio's call/transcript pushes
  // Bold white, NOT cyan: the message body is cyan, so a cyan tag vanished
  // into its own line — and this is the most frequent tag.
  whisper: '\x1b[1;97m', // bold white: transcription traffic
  'whisper-node': '\x1b[93m', // yellow: the PC watcher
  other: '\x1b[90m', // grey: unidentified
};
// Back to the message colour (cyan) rather than a full reset, so the auth
// tag after it keeps the line's colour.
const MSG_COLOUR = '\x1b[22;36m';

/** Same set pino-pretty removes from messages: anything that can rewrite the
 *  terminal, keeping tabs and newlines. */
// eslint-disable-next-line no-control-regex
const UNSAFE = /[\u0000-\u0008\u000B-\u001F\u007F-\u009F]/g;

/**
 * The message, as pino-pretty would print it, with the client tag coloured.
 *
 * A function rather than a string template because pino-pretty strips control
 * characters from messages — which is right for the text (a logged URL must
 * not be able to repaint the terminal) but also stripped the ESC out of colour
 * codes written into the message, which is how `pm2 logs` came to show
 * literal "[93mwhisper-node[0m". The text is stripped here the same way, and
 * only then is the colour added.
 */
function formatMessage(rec: Record<string, unknown>, key: string): string {
  const msg = String(rec[key] ?? '').replace(UNSAFE, '');
  const client = rec['client'];
  if (typeof client !== 'string' || !(client in CLIENT_COLOUR)) return msg;
  const tag = `[${client}]`;
  const i = msg.lastIndexOf(tag);
  if (i < 0) return msg;
  return msg.slice(0, i) + `[${CLIENT_COLOUR[client]}${client}${MSG_COLOUR}]` + msg.slice(i + tag.length);
}

/**
 * Pretty-print only in dev. In production we emit raw ndjson so a log shipper
 * can do its job without parsing ANSI.
 *
 * In-process rather than as a worker transport: a transport's options cross a
 * thread boundary and cannot carry the formatMessage function. Colour stays
 * forced on, which is what makes it show through `pm2 logs`.
 *
 * Synchronous writes. The worker transport flushed itself on exit; an
 * in-process async stream does not, and the crash handlers in index.ts log
 * fatal and call process.exit() on the next line — the one line that
 * matters most would be the one lost. At this log volume a sync write costs
 * a few microseconds a line.
 */
const stream = config.NODE_ENV === 'production'
  ? undefined
  : pretty({
      colorize: true,
      sync: true,
      translateTime: 'HH:MM:ss',
      // svc/pid/hostname are constant per-process noise; client is already
      // the coloured tag in the message.
      ignore: 'pid,hostname,svc,client',
      // Collapse the structured fields onto the same line as the
      // message — without this every log entry was 5+ lines of
      // pretty-printed JSON which buried real signal under noise.
      //   before: 6 lines of INFO: slow request {svc, method, path...}
      //   after:  [09:01:56] INFO: slow request (method="GET" ...)
      singleLine: true,
      messageFormat: (rec, key) => formatMessage(rec as Record<string, unknown>, key),
    });

const options: pino.LoggerOptions = {
  level: config.LOG_LEVEL,
  base: {
    // Tag every log line with the service name so when this lives next
    // to the Python backend in the same log stream, they're easy to
    // disambiguate. (Hidden in the pretty-print via `ignore` above.)
    svc: 'nswpsn-api-node',
  },
};

export const log = stream ? pino(options, stream) : pino(options);
