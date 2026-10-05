# nswpsn-api-node

The AusAware backend. **Entry point: `src/index.ts`.**

TypeScript on Node, HTTP by Hono, PostgreSQL by `pg`, environment validated by
zod, tests by Vitest. `package.json` declares `engines.node >= 20`; CI builds on
Node 20.

This is the **only** backend. The Python service it replaced
(`backends/external_api_proxy.py`) was deleted; the migration is finished. Some
source comments still cite line numbers in that file as the reference the
TypeScript was ported against — those are historical notes about behaviour that
was matched, not a live dependency.

**The full component doc is
[`docs/components/backend.md`](../../docs/components/backend.md)** — route groups,
middleware order, the source registry with every cadence, authentication layers,
and the deploy. This file is the short version.

Before changing anything here, read [`AGENTS.md`](../../AGENTS.md).

## Quickstart

```bash
cd backends/node
npm install
npm run dev                                 # tsx watch on src/index.ts
curl http://localhost:3000/api/health | jq
curl http://localhost:3000/api/config | jq
```

```bash
npm run typecheck    # tsc --noEmit
npm test             # vitest run
```

**Both must pass before you commit a code change.** For a documentation-only
change, do not run the suite.

## Scripts

| Script | Does |
|---|---|
| `npm run dev` | watch mode |
| `npm run build` | `tsc`, then copy migrations into `dist/` |
| `npm start` | run the built `dist/index.js` (`prestart` builds first) |
| `npm run typecheck` | `tsc --noEmit` |
| `npm test` | `vitest run` |
| `npm run migrate` | apply pending migrations |
| `npm run simulate-node` | drive the real `/api/node-ws/agent` path without a receiver |
| `npm run capture-fixtures` | pull golden JSON into `test/fixtures/contract/` |
| `npm run ntfy-test` | exercise the rdio burst → ntfy detector |
| `npm run deploy` | **the owner's deploy. Never run this** |

Every `tsx`/`node` script loads `../.env` when present, so one `backends/.env`
drives everything.

## Environment

`src/config.ts` is the **only** place `process.env` is read; everything else
imports `config`. zod parses once at startup, prints a flat list of problems, and
exits if anything required is missing.

Almost every variable is optional, and the pattern is consistent: an unset
variable turns its feature off rather than breaking the server — the route answers
503 with a clear "not configured" body, or the source registers and returns an
empty snapshot. `PORT` defaults to 3000.

`../env.sample` is the annotated template.

**`../ecosystem.config.js` is stale and unused.** Nothing on the box runs from it.
Never cite it for a port, an environment variable, or a timeout.

## Layout

```
src/index.ts          entry point — preflight(), serve(), shutdown hooks
src/server.ts         createApp() — the Hono instance, middleware, routes
src/config.ts         zod-validated env
src/api/*.ts          57 route modules, one per endpoint group
src/sources/*.ts      one module per polled upstream
src/services/         pollers, node hub, whisper router, LLM, auth, …
src/store/live.ts     LiveStore — current state, in memory
src/store/archive.ts  ArchiveWriter — history, batched into PostgreSQL
src/db/migrations/    119 numbered .sql files
src/lib/              logger, timezone, masks, relay-error formatting
assets/               node-versions.json (the agent manifest), rfs-zones.json
scripts/deploy.sh     the deploy. The OWNER runs this
test/unit/            Vitest, no database
```

`createApp()` is separate from `src/index.ts` so a test can spin up the app
without binding a port — `createApp().fetch(req)` returns a `Response`.

## Database

PostgreSQL, 119 numbered migrations in `src/db/migrations/`, applied by
`src/db/migrate.ts` at boot and by `npm run migrate` before the restart on deploy.
The runner is idempotent. New schema is a new numbered file; never edit an applied
one.

Three separate pools: the main one (`DATABASE_URL`, read/write), the
rdio-scanner's (`RDIO_DATABASE_URL`, **read-only — never add a write path**), and
the Discord bot's (`BOT_DATA_DATABASE_URL`, read, for the dashboard).

## Deploy

`scripts/deploy.sh`, run **by the owner** on the host. Never by you. Three things
it does that you have to work with:

1. **It discards `package-lock.json` before pulling**, so an `npm install` run on
   the server is erased at the next deploy. Commit dependency changes. Do not
   remove that line — without it, `npm install`'s lockfile rewrite makes the next
   `git pull --ff-only` abort.
2. **It skips the Go agent rebuild** when the built binary already reports the
   version in `assets/node-versions.json`. A feeder-node change does not reach any
   node until that version is bumped.
3. **It passes `--kill-timeout 30000`** on the pm2 restart. A transcription
   legitimately runs for seconds and a SIGKILL through one loses that reception's
   transcript for good.

Your work finishes at the commit on `dev-beta`. **Never deploy. Never restart
anything.**

## Vocabulary

Ingested radio traffic is a **reception**, never a "call" — except when quoting
rdio-scanner's own names, where the endpoint really is `call-upload` and the table
really is `calls`.

## See also

- [`AGENTS.md`](../../AGENTS.md) — the operating rules.
- [`docs/components/backend.md`](../../docs/components/backend.md) — the full doc.
- [`docs/architecture.md`](../../docs/architecture.md) — the whole system.
