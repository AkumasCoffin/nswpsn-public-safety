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
import { config } from '../config.js';

export const log = pino({
  level: config.LOG_LEVEL,
  // Pretty-print only in dev. In production we emit raw ndjson so a
  // log shipper can do its job without parsing ANSI.
  transport:
    config.NODE_ENV === 'production'
      ? undefined
      : {
          target: 'pino-pretty',
          options: {
            colorize: true,
            translateTime: 'HH:MM:ss',
            // svc/pid/hostname are constant per-process noise.
            ignore: 'pid,hostname,svc',
            // Collapse the structured fields onto the same line as the
            // message — without this every log entry was 5+ lines of
            // pretty-printed JSON which buried real signal under noise.
            //   before: 6 lines of INFO: slow request {svc, method, path...}
            //   after:  [09:01:56] INFO: slow request (method="GET" ...)
            singleLine: true,
          },
        },
  base: {
    // Tag every log line with the service name so when this lives next
    // to the Python backend in the same log stream, they're easy to
    // disambiguate. (Hidden in the pretty-print via `ignore` above.)
    svc: 'nswpsn-api-node',
  },
});
