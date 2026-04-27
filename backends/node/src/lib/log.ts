/**
 * Structured logger used everywhere in the Node backend.
 *
 * Replaces the Python `Log.startup() / Log.error() / Log.cleanup()` etc.
 * helpers. Pretty-prints in dev so the output is greppable; emits ndjson
 * in production so it's pipeable to journald/Loki/etc.
 *
 * Usage:
 *   import { log } from './lib/log.js';
 *   log.info({ source: 'rfs', count: 42 }, 'fetched incidents');
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
            ignore: 'pid,hostname',
          },
        },
  base: {
    // Tag every log line with the service name so when this lives next
    // to the Python backend in the same log stream, they're easy to
    // disambiguate.
    svc: 'nswpsn-api-node',
  },
});
