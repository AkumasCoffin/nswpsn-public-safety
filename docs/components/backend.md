# Backend — `backends/node`

**Entry point: `backends/node/src/index.ts`.**

The only backend. TypeScript on Node, HTTP by Hono, PostgreSQL by `pg`,
environment validated by zod, tests by Vitest. `package.json` declares
`engines.node >= 20`; CI builds on Node 20.

**There is no Python/Flask backend.** `backends/external_api_proxy.py` was
deleted months ago. Some code comments still cite line numbers in it as the
reference the TypeScript was ported against — those are historical notes about
behaviour that was matched, not a live dependency.

Read [`../../OPERATING-RULES.md`](../../OPERATING-RULES.md) before changing anything here.

---

## Layout

```
backends/node/
  src/index.ts          entry point — preflight(), serve(), shutdown hooks
  src/server.ts         createApp() — the Hono instance, middleware, routes
  src/config.ts         zod-validated env. The ONLY process.env reader
  src/api/*.ts          57 route modules, one per endpoint group
  src/sources/*.ts      one module per polled upstream
  src/services/         pollers, node hub, whisper router, LLM, auth, …
  src/store/live.ts     LiveStore — current state, in memory
  src/store/archive.ts  ArchiveWriter — history, batched into PostgreSQL
  src/db/pool.ts        pg pool wrapper
  src/db/migrate.ts     migration runner
  src/db/migrations/    119 numbered .sql files
  src/lib/              logger, timezone, masks, relay-error formatting
  assets/               node-versions.json (agent manifest), rfs-zones.json
  scripts/deploy.sh     the deploy. The OWNER runs this. You do not
  test/unit/            Vitest, no database
```

## Running it

```bash
cd backends/node
npm install
npm run dev        # tsx watch on src/index.ts, reloads on save
curl http://localhost:3000/api/health
```

| Script | Does |
|---|---|
| `npm run dev` | watch mode |
| `npm run typecheck` | `tsc --noEmit` |
| `npm test` | `vitest run` |
| `npm run build` | `tsc` then copy migrations into `dist/` |
| `npm run migrate` | apply migrations |
| `npm run simulate-node` | drive the node ingest path without a real receiver |
| `npm run deploy` | **the owner's deploy. Never run this** |

All the `tsx`/`node` scripts load `../.env` when present, so one `backends/.env`
drives everything.

## Boot sequence

`preflight()` in `src/index.ts` runs before the port is bound:

1. Hydrate LiveStore from `STATE_DIR/*.json`.
2. Run migrations. A failure is logged but boot continues so `/api/health` stays
   observable.
3. Register every source (`src/sources/registerAll.ts`,
   `src/sources/registerPower.ts`). Nothing fetches yet.
4. Start the persist loop, ensure this month's and next month's archive
   partitions, start the archive flush loop.
5. Arm the background loops: activity mode, filter cache, stats archiver, hourly
   cleanup, node-event pruner, node hourly rollup, ADS-B daily flush, node
   uptime flush, whisper health probe, Wire video processor, Wire purge, memory
   watch.
6. `prewarmAll(30_000)` — every source's first poll in parallel, 30s ceiling, so
   the first request after a restart is not served an empty store.
7. `startPolling()`.
8. `createApp()`, `serve()` on `config.PORT`, `attachNodeWebSockets(server)`.

Then a background perf-index build 5s later, on its own connection with
`statement_timeout=0`, idempotent.

Most steps are deliberately best-effort and individually logged: a missing
`DATABASE_URL` or an empty `STATE_DIR` must not stop the server coming up.

Shutdown on `SIGTERM`/`SIGINT` drains in-flight requests, stops the loops in
dependency order (pollers before writers), flushes LiveStore and ArchiveWriter,
and closes all three pools — main, rdio, bot-data. The ADS-B and uptime flushes
are **awaited** so a partial minute of counters is written rather than discarded.

## The two stores

**`src/store/live.ts` — LiveStore.** Current snapshot per source, in memory.
Every live `/api/*` endpoint reads from here and **never from PostgreSQL**.
Persisted per source to `STATE_DIR/<source>.json` with temp-file-plus-rename, so
the file on disk is always complete. Each snapshot is opaque to the store — it
is whatever shape the source decided, only requirement being JSON-serialisable.

The separation is the whole design. The previous architecture kept live state and
history in the same tables with UPDATE-driven liveness columns, and the resulting
write contention caused repeated cascading failures.

**`src/store/archive.ts` — ArchiveWriter.** Batched inserts into four
month-partitioned tables (`ArchiveTable`, `archive.ts:32`):

`archive_traffic` · `archive_rfs` · `archive_power` · `archive_misc`

`src/services/cleanup.ts` creates partitions hourly and drops expired ones.
`src/services/dataHistoryQuery.ts` maps a source name to the minimum set of
tables a query needs; unknown sources fall back to `archive_misc`.

## The source registry

`src/services/sourceRegistry.ts`. One entry per polled upstream:

```ts
registerSource({
  name: 'rfs_incidents',   // LiveStore key + log identifier
  family: 'rfs',           // traffic | rfs | power | misc → archive table
  intervalMs: 60_000,      // poll cadence, used 24/7
  fetch: async () => { … }, // returns the snapshot; throws on failure
  archiveSource: 'rfs',    // optional: the value written to archive rows
  archiveItems: (…) => […], // optional: custom fan-out to rows
})
```

`archiveSource` exists because a LiveStore key and a historical archive `source`
value sometimes have to differ — the key is `rfs_incidents` but archive rows must
be tagged `rfs` to match data backfilled before the rewrite. Without the
override, `/api/data/history?source=rfs` returns nothing.

`archiveItems` is for a snapshot that is neither a GeoJSON `FeatureCollection`
nor a flat array. The pager source returns `{ messages, count }`, so it supplies
one — otherwise the whole snapshot would land as a single "Unknown" wrapper row
instead of one row per message.

`src/services/poller.ts` walks the registry and arms one `setInterval` per
source. Every source polls on that one interval, round the clock: the old
active/idle split keyed to a page-activity heartbeat was removed, because the
Discord bot consumes continuously whether or not anyone has the site open.
Failures increment a counter, get backoff (`src/sources/shared/backoff.ts`), and
feed `src/services/sourceHealth.ts`.

### The 36 registered sources

38 sources are declared; two of them do not register by default, so a running
backend holds **36**.

| Source | Family | Interval |
|---|---|---|
| `adsb_aircraft` | misc | 8s |
| `act_ambulance` | misc | 60s |
| `bom_warnings` | misc | 60s |
| `endeavour_current` | power | 60s |
| `pager` | misc | 60s |
| `qld_cameras`, `qld_flood_cameras` | traffic | 60s |
| `rfs_incidents` | rfs | 60s |
| `traffic_cameras` | traffic | 60s |
| `traffic_incidents` | traffic | 60s |
| `user_incidents` | misc | 60s |
| `ausgrid`, `ausgrid_stats` | power | — **not registered** |
| `qld_fire`, `qld_warning` | rfs | 2m |
| `sa_cfs`, `sa_mfs` | rfs | 2m |
| `vic_emergency` | rfs | 2m |
| `wa_incident`, `wa_warning` | rfs | 2m |
| `essential_current`, `essential_future` | power | 3m |
| `aviation_cameras` | misc | 5m |
| `endeavour_planned`, `endeavour_maintenance` | power | 5m |
| `nt_fire` | rfs | 5m |
| `traffic_roadwork`, `traffic_flood`, `traffic_fire`, `traffic_majorevent`, `traffic_alpine`, `traffic_lga` | traffic | 5m |
| `traffic_works` | traffic | 5m |
| `weather_radar` | misc | 5m |
| `beachwatch`, `beachsafe` | misc | 10m |
| `firms_hotspots` | misc | 15m |
| `weather_current` | misc | 30m |

**Ausgrid is off.** `register()` in `src/sources/ausgrid.ts:262` returns
immediately unless `AUSGRID_DISABLED` is explicitly set to `false`, and the
default is `'true'` — the upstream `webapi/*` endpoints have returned 404 for
months, and the default keeps the dead poll from firing even on a fresh deploy.
So `ausgrid` and `ausgrid_stats` are the two declared-but-unregistered sources,
and there is no `ausgrid` key in `/api/status`. Setting
`AUSGRID_DISABLED=false` brings them back at 2m each.

The seven LiveTraffic hazard kinds come from one table in
`src/sources/traffic.ts:428`. `traffic_lga` is council-submitted local-road
records — a genuinely separate reporting stream with verified zero overlap
against the others on id, coordinate and street-plus-suburb.

`traffic_works` is **not** an eighth hazard kind and not a duplicate of
`traffic_roadwork`. It is a separate upstream feed: `fetchTrafficWorks`
(`src/sources/traffic.ts:502`) reads the LiveTraffic web feed rather than the
`HAZARD_BASE/<endpoint>.json` the seven hazard kinds share, groups records by
`apiSource`, restores groups that vanish between polls, and archives each
record under the type its category implies instead of into a single bucket.
`src/services/sourceHealth.ts:45` labels it "LiveTraffic — works & ACT".

### Not in the registry

- **Public transport** — `src/sources/tfnsw.ts` + `src/api/transport.ts`. AnyTrip
  for metadata, official TfNSW GTFS-Realtime for authoritative positions and
  service alerts. Positions off by default (`TFNSW_POSITIONS_DISABLED=true`).
  Deliberately **not** in the cacheable-path list — that middleware ignores
  query strings, so a CDN would cross-serve one viewport's vehicles to every
  viewport.
- **MarineTraffic**, **Central Watch** — each parks a Playwright Chromium tab
  (`src/services/marinetrafficBrowser.ts`, `centralwatchBrowser.ts`). Kill
  switches: `MARINETRAFFIC_DISABLED`, `CENTRALWATCH_DISABLED`. A host without
  Chromium still boots and serves last-good JSON.
- **News RSS** — `src/sources/news.ts`.
- **What3Words** (`src/api/w3w.ts`), **boundaries**, **agency reference data**.

## Routes

57 modules in `src/api/`, composed by `createApp()` in `src/server.ts`. Each
defines its own `/api/...` paths and is mounted at `/`, so handler URLs are
exactly the paths they declare.

Order matters in one place: `nodeUpdatesRouter` is mounted **before**
`nodesRouter` so its exact `/api/nodes/versions` matches before
`/api/nodes/:id` treats `"versions"` as an id.

Roughly grouped:

| Group | Modules |
|---|---|
| Core | `health`, `config`, `status`, `heartbeat`, `system`, `stats` |
| Sources | `rfs`, `bom`, `traffic`, `beach`, `weather`, `pager`, `firms`, `adsb`, `act-ambulance`, `qld-fire`, `qld-cameras`, `nt-fire`, `vic-emergency`, `wa-emergency`, `sa-fire`, `aviation`, `news`, `centralwatch`, `marinetraffic`, `transport`, `boundaries`, `agency` |
| Power | `endeavour`, `ausgrid`, `essential` |
| History | `data-history` |
| Users & content | `incidents`, `editor`, `users`, `profiles`, `tickets`, `referrals`, `fleet`, `wire`, `wireComments` |
| Staff | `staffNotices`, `staffNotify`, `node-data` |
| Feeder nodes | `nodes`, `node-updates`, `node-enrol`, `node-ingest`, `node-ws`, `node-data`, `feeder`, `scanner-ingest`, `radio-public` |
| Radio | `radio-public`, `transcripts`, `summaries`, `whisper` |
| Discord | `dashboard` |
| Utility | `w3w` |

`GET /` returns an endpoint catalogue. One entry in it is stale:
`police-heatmap` (`src/server.ts:453`) — Waze was removed and no such route
exists. See the architecture doc.

### Middleware, in order

1. **Request logger** — a replacement for Hono's. Quiet on 2xx/3xx for
   high-volume paths (`/api/heartbeat`, `/api/config`, `/api/health`,
   `/api/status`, `/api/check-editor/*`); `info` for anything slower than 500 ms,
   for 4xx, and for all 2xx outside production; `warn` for 5xx; silent for
   OPTIONS. Every line is tagged with the caller it identified — `browser`,
   `discord-bot`, `node`, `rdio`, `whisper`, `whisper-node`, `other` — and
   colour-coded outside production.

   Path beats header for `rdio` and `whisper`: nothing else posts to those
   routes, and the callers that do cannot set custom headers at all.

2. **Compression** — brotli/gzip. `/api/data/history` payloads compress about
   10×.

3. **Short public cache** — a fixed list of cheap GETs gets
   `public, max-age=30, stale-while-revalidate=300`. Auth-sensitive and
   per-user paths are excluded.

4. **CORS** — an explicit allowlist of exact first-party origins, extensible
   without a code change via `CORS_ALLOWED_ORIGINS`. Deliberately narrow:
   `/api/config` returns the API key in its body, and `credentials: true` is
   required for the dashboard's session cookie — a wildcard, or a
   `*.forcequit.xyz` regex, would let a compromised sibling subdomain make
   credentialed reads with a logged-in user's cookie.

5. **`optionalSupabaseJwt`** — identify the user before any gate, so a privileged
   route can role-check the real person.

6. **`requireApiKey`** — the global `NSWPSN_API_KEY` gate. Short-circuits for
   OPTIONS, a list of public endpoints, anything outside `/api`, and any request
   already authenticated as a Supabase user.

### Authentication

| Caller | Credential | Where |
|---|---|---|
| Browser / public | `NSWPSN_API_KEY`, handed out by `/api/config` | `services/auth/apiKey.ts` |
| Logged-in user | Supabase JWT (HS256, `SUPABASE_JWT_SECRET`) | `services/auth/supabaseJwt.ts` |
| Staff | role check on top of the JWT | `services/auth/roles.ts` |
| Node agent | `X-Node-Token` + `X-Node-Install` | `services/auth/nodeToken.ts` |
| Enrolling agent | single-use code, nothing else | `services/auth/nodeEnrol.ts` |
| Scanner feed | key in a **form field** | `api/scanner-ingest.ts` |
| Discord dashboard | Discord OAuth + session cookie | `services/dashboardSession.ts` |
| Whisper watcher | `WHISPER_ADMIN_TOKEN` | `api/whisper.ts` |

`/api/node-ingest/` and `/api/node-enrol` are exempt from the key gate because
they do their own token auth, and an enrolling agent has no credential of any
kind yet.

**Roles live in the backend's own PostgreSQL, not in Supabase.** Supabase is
authentication and identity only. Never propose row-level security on the
incident or role tables — RLS is a Supabase feature and does not apply to them.

## Configuration

`src/config.ts` is the **only** place `process.env` is read. Everything else
imports `config`. zod parses once at startup and prints a flat list of problems
and exits if anything required is missing or malformed.

`PORT` defaults to 3000. `NODE_ENV` defaults from the legacy `DEV_MODE` flag so
one `.env` drives everything.

Almost every variable is **optional**, and the pattern is consistent: an unset
variable turns its feature off rather than breaking the server. Specifically —

- unset `DATABASE_URL`: the server still boots and `/api/health` answers;
- unset `PAGERMON_URL`: the pager source registers and returns an empty
  snapshot, and `/api/pager/hits` is an empty FeatureCollection;
- unset `RDIO_DATABASE_URL`, `GEMINI_API_KEY`, `FEEDER_TOKEN_SECRET`, the
  Discord block, or the Wire media block: the matching routes answer **503 with
  a clear "not configured" body**, and the staff panel hides the card rather
  than showing a failure for a feature nobody turned on;
- unset `MAP_KEY` (NASA FIRMS): the source stays empty.

**`WHISPER_BACKENDS` is the exception to that pattern — it never 503s.** With
it unset, `src/api/whisper.ts` answers:

| Route | Unconfigured response |
|---|---|
| `POST /api/whisper/v1/audio/transcriptions` | **404** (`whisper.ts:68`) |
| `GET /api/whisper/v1/models` | **404** (`whisper.ts:100`) |
| `GET /api/whisper/status` | **200** with `configured: false` (`whisper.ts:113`) |
| `GET /api/whisper/history` (`whisper.ts:164`) | **200** with `configured: false`, empty rows (`whisper.ts:139`) |
| `POST /api/whisper/drain` | **404** with an error body (`whisper.ts:193`) |

The 200-with-`configured: false` shape is a contract the staff panel and the PC
watcher depend on, not an oversight — see
[`transcription.md`](transcription.md). The only 503 in that file
(`whisper.ts:81`) is a pass-through of an upstream whisper server's own 503,
which is a different condition entirely.

Kill switches that stay off while the credential remains configured:
`ADSB_DISABLED`, `TRANSPORT_DISABLED`, `TFNSW_DISABLED`,
`TFNSW_POSITIONS_DISABLED` (on by default), `CENTRALWATCH_DISABLED`,
`MARINETRAFFIC_DISABLED`, `CF_TRANSFORMS_DISABLED`,
`RDIO_INCIDENT_ALERTS_ENABLED` (off by default), `WIRE_PUBLIC` (off by default).

`backends/env.sample` is the annotated template. **`backends/ecosystem.config.js`
is stale and unused — never cite it for a port or an environment variable.**

Two secrets are load-bearing and never leave the server:
`RDIO_INTERNAL_API_KEY` (the one key inside the central rdio-scanner) and
`PAGERMON_INGEST_API_KEY` plus its per-state variants. A node agent never holds
either; the backend substitutes them when it forwards.

## Database

PostgreSQL, 119 numbered migrations in `src/db/migrations/`, applied by
`src/db/migrate.ts` at boot and by `npm run migrate` before the restart on
deploy. The runner is idempotent, so applying twice is a no-op. New schema is a
new numbered file; never edit an applied one.

Three separate pools:

| Pool | Variable | Access |
|---|---|---|
| Main | `DATABASE_URL` | read/write |
| rdio-scanner | `RDIO_DATABASE_URL` | **read-only** — `src/services/rdio.ts`. Never add a write path |
| Bot data | `BOT_DATA_DATABASE_URL` | read, for the Discord dashboard. The bot writes it |

`src/services/rdio.ts` sets a global type parser for OID 1114
(`timestamp without time zone`), because rdio stores UTC values in a naive column
and `pg` would otherwise parse them in the Node process's local timezone.

### Removed, so you do not go looking

- `/api/rdio/transcripts/search` was **removed** (2026-09). Its browse mode ran
  an unbounded `COUNT(*)` plus a leading-wildcard `ILIKE` over the whole
  calls-joined-transcripts set every 30s per open staff tab, blowing the rdio
  pool's statement timeout and starving its five connections. The staff
  Transcripts view is now a whisper throughput dashboard
  (`/api/whisper/history`) that never touches the rdio database, and the bot's
  `/ts` command was retired with it. Do not reinstate either without solving the
  query.
- Waze: no source, no routes, and migrations `071`/`074` dropped the archive and
  heatmap tables.

## Tests

```bash
cd backends/node
npx tsc --noEmit
npx vitest run
```

Unit tests in `test/unit/`, no database required. **Both must pass before you
commit a code change.** For a documentation-only change, do not run the suite —
run the smallest check that proves the work.

CI runs the same two commands (`.github/workflows/tests.yml`).

## Deploy

`scripts/deploy.sh`, run **by the owner** on the host. Never by you.

What it does, in order: discard `package-lock.json` drift, `git pull --ff-only`,
re-exec itself if the pull changed it, `npm install`, reinstall the matching
Chromium, check `ffmpeg-static` is usable, `npm run build`, cross-compile the
three Go agents into `downloads/` with sha256 sidecars **only when the manifest
version differs from the built binary**, `npm run migrate`, then
`pm2 restart api-node --kill-timeout 30000`.

Three consequences you have to work with:

1. **An `npm install` on the server is erased by the next deploy.** The script
   runs `git checkout -- backends/node/package-lock.json` before it pulls.
   Commit dependency changes; never install them on the box. Do not remove that
   line — without it, `npm install`'s lockfile rewrite makes the next
   `git pull --ff-only` abort.
2. **A Go agent change does not reach any node until its version is bumped** in
   `assets/node-versions.json`. The rebuild is skipped when the built binary
   already reports the manifest version.
3. **The 30-second kill timeout lives in the deploy script, not in an ecosystem
   file.** A transcription legitimately runs for seconds and a SIGKILL through
   one loses that reception's transcript for good; `shutdown()` in
   `src/index.ts` waits for in-flight requests, and this gives that wait room to
   matter. A quiet restart is still instant.

## Vocabulary

**Ingested radio traffic is a *reception*, never a "call".** The exception is
quoting rdio-scanner's own names — its endpoint really is `call-upload` and its
table really is `calls`. Describing *our* traffic as a "call" is wrong.

## See also

- [`../architecture.md`](../architecture.md) — the system, and the full trace
  from a transmission in the air to where it surfaces.
- [`transcription.md`](transcription.md) — the whisper router.
- [`radio-node.md`](radio-node.md), [`pager-node.md`](pager-node.md),
  [`aircraft-node.md`](aircraft-node.md) — what talks to the ingest routes.
- [`../uptime-kuma-monitors.md`](../uptime-kuma-monitors.md) — monitors against
  `/api/status`.
