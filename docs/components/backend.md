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
  src/api/*.ts          61 route modules, one per endpoint group
  src/api/v1/           the versioned public API: mirror, response shapes,
                        zod schemas, the OpenAPI document
  src/sources/*.ts      one module per polled upstream
  src/services/         pollers, node hub, whisper router, LLM, auth, …
  src/store/live.ts     LiveStore — current state, in memory
  src/store/archive.ts  ArchiveWriter — history, batched into PostgreSQL
  src/db/pool.ts        pg pool wrapper
  src/db/migrate.ts     migration runner
  src/db/migrations/    137 numbered .sql files
  src/lib/              logger, timezone, masks, relay-error formatting
  assets/               node-versions.json (agent manifest), rfs-zones.json
  openapi.json          the API description, generated — `npm run openapi`
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
| `npm run openapi` | regenerate `openapi.json` after changing anything on `/api/v1/` |
| `npm run deploy` | **the owner's deploy. Never run this** |

All the `tsx`/`node` scripts load `../.env` when present, so one `backends/.env`
drives everything — except `npm run openapi`, which uses placeholders on
purpose: the description must not depend on whose machine generated it.

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

### The 39 registered sources

39 sources are declared and all register by default, so a running backend
holds **39**.

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
| `ausgrid`, `ausgrid_stats` | power | 2m |
| `qld_fire`, `qld_warning` | rfs | 2m |
| `sa_cfs`, `sa_mfs` | rfs | 2m |
| `vic_emergency` | rfs | 2m |
| `wa_incident`, `wa_warning` | rfs | 2m |
| `essential_current`, `essential_future` | power | 3m |
| `aviation_cameras` | misc | 5m |
| `endeavour_planned`, `endeavour_maintenance` | power | 5m |
| `nt_fire` | rfs | 5m |
| `traffic_roadwork`, `traffic_flood`, `traffic_fire`, `traffic_majorevent`, `traffic_alpine`, `traffic_lga` | traffic | 5m |
| `roads_qld`, `roads_vic` | traffic | 2m |
| `roads_sa`, `roads_act` | traffic | 5m |
| `weather_radar` | misc | 5m |
| `dorsal_reports` | misc | 5m |
| `beachwatch`, `beachsafe` | misc | 10m |
| `firms_hotspots` | misc | 15m |
| `weather_current` | misc | 30m |

**Ausgrid can be switched off.** It polls by default, from Ausgrid's
`/api/outages-map/outages/` endpoints: today's list every poll, and the
30-day list of planned work at most every half hour, kept in memory between
reads. `register()` in `src/sources/ausgrid.ts:355` returns immediately when
`AUSGRID_DISABLED` is set to `true`; then `ausgrid` and `ausgrid_stats` do not
register, there is no `ausgrid` key in `/api/status`, and `register()` also
drops the two snapshots LiveStore loaded from disk at startup, so the routes
answer empty rather than with whatever was fetched before polling stopped.

The seven LiveTraffic hazard kinds come from one table in
`src/sources/traffic.ts:428`. `traffic_lga` is council-submitted local-road
records — a genuinely separate reporting stream with verified zero overlap
against the others on id, coordinate and street-plus-suburb.

**The other states' roads** come from each state's own feed
(`src/sources/interstateRoads.ts`), one source per state: QLD from
QLDTraffic's `events_v2.geojson` (leaving out what Transport for NSW supplies
to it), SA from the Department for Infrastructure and Transport's open-data
roadworks and incidents plus Traffic SA's closures and detours and the
outback road report, ACT from Roads ACT's traffic-management plans and
unplanned closures, VIC from Transport Victoria's planned and unplanned
disruption APIs when `VIC_TRANSPORT_API_KEY` is set (else from the Live
Traffic aggregate's `vicRoadInfo` group, through `fetchTrafficWorks`). Each
record takes the Live Traffic shape with `state`, `source` and `credit`, and
its `type` (Incident, Roadwork, Flood, Major Event) decides the feed it is
served on: `/api/traffic/{incidents,roadwork,flood,majorevent}` answer NSW's
records then every state's of that type (`interstateRecords`), with `?state=`
to narrow. History files each under the NSW feed's source for its type
(`archiveByType`). Records whose end has passed are left out: Traffic SA's
map layer keeps closures for months after they end. A state read with several
requests keeps each part's last good records for 30 minutes when one fails.

These used to be `traffic_works`: Live Traffic NSW's aggregate web feed, which
re-shares SA, QLD, VIC and ACT road events — late, and cut off (it carried
exactly 1,000 of SA's 1,446 roadworks). That source and `/api/traffic/works`
are gone; `traffic_works` remains only as an archive source for older records
and as an alias of the `traffic_roadwork` alert type, so presets that held it
keep their roadwork alerts.

#### `dorsal_reports` writes once per report, not once per poll

`src/sources/dorsal.ts` is the only source whose `archiveItems` keeps state,
and the reason is worth knowing before anyone "simplifies" it.

The archive parents are append-only (`src/store/archive.ts` `insertChunk`), so
the normal contract is that every poll re-archives everything currently in the
snapshot and readers collapse the repeats with `DISTINCT ON` or `unique=1`.
That is right for an incident feed, where the repeats *are* the history of a
changing record. Dorsal is not that: a report is published once and almost
never edited, and the snapshot holds a rolling 14 days. Re-archiving it every
five minutes would be roughly **49,000 rows a day for a feed that produces
25** — on a database whose `node_radio_events` indexes we have already had to
go and reclaim 2.4 GB from.

So a report is written exactly once, the first time the process sees its id,
and only if it falls inside the last 7 days. Two consequences:

- **The seen-set is seeded from `archive_misc_latest` on the first poll.** That
  is what makes the cold-start pass a 7-day *backfill* on an empty database and
  a no-op on every deploy restart afterwards. If the seed query fails the
  source still starts; the cost is one duplicated window, which readers already
  collapse.
- **Sidecar `last_seen_at` stops advancing immediately**, because nothing
  re-touches the row. Cleanup prunes it on the normal `DATA_RETENTION_DAYS`
  schedule like any other archived record, which is the intended behaviour, not
  a bug to fix.

Two things upstream exposes that this source deliberately does not take.
`/api/public/surfspot/search` embeds the owning account's email,
`resetPasscode` and Facebook id in every record — it is never called. And
`report/list` carries reporter display names on community submissions; the
`account` object is dropped at normalisation and provenance is carried as the
upstream `official` flag instead. A unit test asserts the name never reaches a
row.

The route defaults to a **48-hour** window. That is not a performance choice:
the Discord bot polls it bare and de-duplicates against `seen_alerts`, which
its retention job prunes at 7 days, so a longer default would re-fire every
report on day eight. The map asks for `?hours=168` explicitly.

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

61 modules in `src/api/`, composed by `createApp()` in `src/server.ts`. Each
defines its own `/api/...` paths and is mounted at `/`, so handler URLs are
exactly the paths they declare.

**The public data API is also served under `/api/v1/`** — see
[`../api.md`](../api.md) for what is there, the versioning policy and the
response and error shapes. Routers are not rewritten for it: after a router's
`app.route('/', …)`, `mirror(app, router, [paths])` (`api/v1/mirror.ts`)
registers the listed routes a second time under `/api/v1/` with the same
handler chain — the same `scoped()` declaration, validators and
`describeRoute()` metadata — and records them for the OpenAPI document. Only
the listed paths are versioned, a listed path the router lacks throws at boot,
and router-level middleware travels with the routes it covers. Handlers answer
through `ok()`, `geo()` and `fail()` (`api/v1/respond.ts`), which wrap the body
in `data` and use the `{ error: { code, message } }` shape on `/api/v1/`, and
send exactly the old body on `/api/`.

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

`GET /` returns an endpoint catalogue kept from the Python backend's shape. It
lists unversioned paths only.

### Middleware, in order

1. **Request logger** — a replacement for Hono's. Quiet on 2xx/3xx for
   high-volume paths (`/api/heartbeat`, `/api/config`, `/api/health`,
   `/api/status`, `/api/check-editor/*`, and their `/api/v1/` twins); `info`
   for anything slower than 500 ms,
   for 4xx, and for all 2xx outside production; `warn` for 5xx; silent for
   OPTIONS. Every line is tagged with the caller it identified — `browser`,
   `discord-bot`, `node`, `rdio`, `whisper`, `whisper-node`, `other` — and
   colour-coded outside production. A request that passed the credential gate
   also carries `auth=jwt|browser|static` or `auth=key:<prefix>` (a named
   key's lookup prefix, never the key).

   Path beats header for `rdio` and `whisper`: nothing else posts to those
   routes, and the callers that do cannot set custom headers at all.

2. **Compression** — brotli/gzip. `/api/data/history` payloads compress about
   10×.

3. **Short public cache** — a fixed list of cheap GETs (and their `/api/v1/`
   twins) gets `public, max-age=30, stale-while-revalidate=300`.
   Auth-sensitive and per-user paths are excluded.

4. **CORS** — an explicit allowlist of exact first-party origins, extensible
   without a code change via `CORS_ALLOWED_ORIGINS`. The allowlist is
   `services/auth/firstParty.ts`, shared with the browser-token gate below so
   the origins that may read responses are exactly the origins that may hold
   a session token. Deliberately narrow: `credentials: true` is required for
   the dashboard's session cookie — a wildcard, or a `*.forcequit.xyz` regex,
   would let a compromised sibling subdomain make credentialed reads with a
   logged-in user's cookie.

5. **`optionalSupabaseJwt`** — identify the user before any gate, so a privileged
   route can role-check the real person.

6. **`requireApiKey`** — the credential gate. Short-circuits for OPTIONS, a
   list of public endpoints (a `/api/v1/` path is public exactly when its
   `/api/` twin is), anything outside `/api`, and any request already
   authenticated as a Supabase user; otherwise resolves one of the three
   credential classes in the table below and tags the request with it. Its
   refusals keep their old bodies on `/api/` and use the v1 error shape on
   `/api/v1/`, with the same status.

After the routes, `app.notFound` and `app.onError` (`api/v1/respond.ts`) answer
`/api/v1/` in JSON — `404 not_found`, and a `500 internal_error` that says
nothing about the cause — and leave every other path with Hono's plain-text
`404 Not Found` and `Internal Server Error`, as before. The gate runs first, so
an unknown `/api/…` path without a credential is still a 401.

### Authentication

A static site cannot keep a secret, so a browser is never given one. A page
asks `POST /api/session/token` for a **browser session token** — 15 minutes,
HMAC-signed, bound to the caller's address (/24, /48) and User-Agent, issued
only to requests carrying a first-party `Origin`/`Referer`, rate-limited per
address — and the gate accepts it only alongside a first-party origin. Copied
to curl it fails; past its lifetime it fails; the page re-mints silently
(`api-session.js`). Nothing is stored server-side.

People who need scripted access get a **named API key** from the `api_keys`
table (migration 120; the request flow below is the usual route, and
`scripts/issue-api-key.ts` / `scripts/revoke-api-key.ts` are the server-side
shortcuts). Only the sha256 and a lookup prefix are stored.
A key carries scopes (what it may read — below), its own per-minute limit, an
optional expiry and a `revoked_at`. **Keys are refused from web pages**: a
request presenting one with a browser `Origin` header, or any `Sec-Fetch-Site`
header (every current browser sends it, a page cannot forge or drop it, and no
script does — it also catches a same-origin GET, which has no `Origin`), is a
403, so a key cannot be used from another site, nor pasted into ours. There was
an exception for a page on the API's own origin, the documentation page once
served at `/api/docs`; the reference is now on the site and reads the
description signed in, and the API's origin serves no pages, so it is gone.

Keys are normally **requested, not issued**. A logged-in user asks from their
profile (`POST /api/api-keys/requests`, a name, a reason and, optionally, the
scopes they would like; one open request at a time — pending, or approved and
not yet generated), which raises a staff Discord alert (`api_key_request`,
with the scopes asked for). Owner or staff approve or reject it on the staff
page (Moderation → API keys; `/api/staff/api-keys/*`), and approval sets the
scopes the key gets (required — `api_key_requests` keeps the ask in
`requested_scopes` and the grant in `scopes`, migration 129), the per-minute
limit and an optional expiry — but approval does not create the key. The **user** mints it (`POST /api/api-keys/requests/:id/claim`), so the
plaintext is shown once to its owner and never to staff; nothing stores it.
Until they do, staff can withdraw the approval (`…/withdraw`), and a grant
whose expiry passes before it is generated lapses on its own. A user holds at
most three active keys. From the profile they can **re-issue** a live key
(`POST /api/api-keys/:prefix/reissue`: a new secret and prefix on the same
row, so its limit, expiry and history carry on; the plaintext is shown once,
and the old key stops resolving once the change commits) or **delete** one
(`…/delete`: revoked if live, and gone from their list; staff still see it).
Staff list and revoke any key from the staff page, and change a live key's
scopes (`POST /api/staff/api-keys/:id/scopes`; the owner gets a bell, and the
key's cached row is dropped so the change applies to its next request). The
pickers on both pages read `GET /api/api-keys/scope-catalog`. The caps are checked under
a per-user advisory lock, with a partial unique index on pending requests as
the backstop. The requests live in `api_key_requests` (migration 121); the
routes are in `api/apiKeys.ts`.

**What each key does is counted** (migration 127, `services/auth/apiKeyUsage.ts`):
every request the gate resolves to a key (its real secret, on a path that
takes a key — a public path never looks at the key, and a request that
already carries a session is counted as that session, not the key), by key,
hour, path and status, including the ones the gate refuses (from a web page, revoked or
expired, over the limit, a path keys may not read). A wrong secret against a
real prefix is not counted against the key. The tally is in memory and written
to `api_key_usage_hourly` once a minute and at shutdown; a failed write drops
that minute rather than counting it twice. Paths fold identifiers to `:id`
and keep a non-GET method in front, and a key may add 100 distinct paths an
hour before the rest are counted as `(other paths)`. The hourly rows are pruned
after 90 days; `api_keys.requests_total` keeps the lifetime count. The staff
page shows each key's 24-hour and 7-day figures by class (OK, 404, refused,
429, other 4xx, 5xx) and, per key, a 24h / 7d / 30d chart and its busiest
paths (`GET /api/staff/api-keys/:id/usage`). A window is a whole number of
hours ending with the current one, so the list, the tiles and the chart agree.
Paths are shown as requested, `/api/v1/` and `/api/` apart, and the list's
**Unversioned** column (`unversionedD7`; `unversioned` for one key's window)
counts requests on unversioned `/api/…` paths — the keys to warn before an old
path is retired. It includes feeds that are not on `/api/v1/` yet, which have
nowhere to move.
`scripts/issue-api-key.ts` remains the no-review path for a key minted on the
server.

The static `NSWPSN_API_KEY` remains for server-side callers that predate the
above — the Discord bot and rdio's transcripts plugin — and is no longer
returned by `/api/config` or carried in `config.js`.

| Caller | Credential | Where |
|---|---|---|
| Browser page | session token `bt1.…` from `POST /api/session/token` | `services/auth/browserToken.ts`, `api/session.ts` |
| Script / CLI user | named key `ak_<prefix>_…` (DB, hashed), CLI-only, **GET on what its scopes cover** | `services/auth/apiKeys.ts`, `services/auth/apiKeyScopes.ts`, `api/apiKeys.ts` |
| Discord bot, rdio plugin | static `NSWPSN_API_KEY` | `services/auth/apiKey.ts` |
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

### What a named key can read: scopes

Passing the credential gate is not an authorisation decision — most routes have
nothing but the gate, so "credentialed" used to mean "everything a signed-in
page can see". For a key that was wrong: an audit of the registered routes
found one could read any account's complete role matrix
(`/api/check-admin/:userId`), read anyone's profile, enumerate every live
visitor session, walk the whole rdio corpus with its transcripts and audio
links, spend the server's paid what3words quota, post an unbounded takedown
notice into the private staff Discord channel, inflate a contributor's view
counts, and push 25 MB of audio at the GPU transcription backends. So a key is
confined at the gate itself, and it only ever reads: **GET or HEAD**, nothing
else.

What it reads is its **scopes** (`services/auth/apiKeyScopes.ts`, migration
129): a set of **feed** × **window** cells, written `<feed>:<window>` like the
role names (`fires:live`, `fires:history`). `FEEDS` lists the feeds in the order
the staff and profile tables show them — Fires, Weather warnings, ACT
Ambulance, Power outages, Road hazards, Public transport, Aircraft, Ships,
Weather, Beaches & sharks, Pager messages, Radio summaries — each with a label,
a line saying what it covers, and the windows it has (`live` — what the map
shows now, polled and cached; `history` — stored records over time). A feed
groups the sources a person would ask for together: Fires is every fire service
and the QFD and DFES public warnings (read merged at `/api/v1/fires`,
`api/fires.ts`, each record's `kind` saying which), satellite hotspots and fire
danger ratings; Power outages is all three distributors (read merged at
`/api/v1/outages`, `api/outages.ts`, each record's `kind` and `status` saying
whether it is unplanned or planned and whether the power is off now).

Each route a key may reach **declares** the one cell it serves with
`scoped({ feed, window })` as its first middleware; a key passes when it holds
that cell. There are no wildcards, so a feed added later reaches nobody until
staff tick it. The gate reads the declaration off the matched route
(`routeNeed`, Hono's `matchedRoutes`), so there is no second list to keep in
step. A route with no declaration is out of reach of every scoped key:
`403 api_key_scope_forbidden`, with the cell the route needs in the message. A
path no route serves is the ordinary 404. `GET /api/api-keys/scope-catalog`
gives the pickers the feeds with only the windows some route declares
(`declaredNeeds`), and a test keeps `FEEDS` and the declarations in step.

Stored history spans feeds, so `/api/data/history` (`/api/v1/history` on v1 —
`lib/apiVersion.ts` renames it, and keeps the old v1 name answering for pages
opened before the site moved off it, until old paths are retired) and its
`/incident` route
declare `scoped({ feed: '*', window: 'history' })`, which any `<feed>:history`
cell satisfies; the handler then narrows the query to `historySourcesFor(...)`,
the archived sources of the feeds whose history the key holds
(`HISTORY_SOURCES` maps each archived `source` to its feed — `wa_warning` is
`fires`, `bom_land` is `warnings`, `traffic_fire` is `roads`). Naming another source is a 403; a key's
history answer is `Cache-Control: private, no-store`. Callers that are not
named keys (the site, the static key) are not narrowed. `user_incident` is not
in the map, so no key reads it. `?feed=` takes the table's feeds in place of
archived sources (`feed=fires` is every fire service's), and a key is refused a
feed whose history it does not hold.

Never declared, so never readable with a key: anything about people or nodes —
profiles and accounts, user-submitted incidents, site statistics, the cache and
collection internals, and node status (`/api/adsb/receivers`,
`/api/radio/monitored-sites`) — and this site's own reference data: boundaries
(the council names too), the radio site list and reception transcripts — and
other people's pictures and words: the camera routes (`/api/traffic/cameras`,
`/api/qld/…`, `/api/aviation/…`, `/api/centralwatch/…`) and the news outlets'
feeds (`/api/news/…`), which the map and the live page read and which are off
`/api/v1/`; `/api/traffic/all-feeds` drops Live Traffic's camera records
(`liveCams`). The fire-tower image route stays public for the map's
`<img src>`; the camera list it is reached from needs a credential. The
per-agency fire incident and warning routes (`/api/rfs/incidents`,
`/api/qld-fire/{incidents,warnings}`, `/api/vic-emergency/events`,
`/api/nt-fire/incidents`, `/api/wa-emergency/{incidents,warnings}`,
`/api/sa-fire/{cfs,mfs}`) are undeclared too and off `/api/v1/`: the map and
the bot read them in each agency's own shape, keys read the merged feed. So are
each distributor's outage routes (`/api/endeavour/…`, `/api/essential/…`,
`/api/ausgrid/…`), which the map, the live page and the bot read; keys read
`/api/v1/outages`. Live
data means what the public map shows; the only
node-derived data in it is aircraft and pager traffic. The feed list lives here
and not in `shared/alert-catalog.json`, which is what the bot alerts on (a test
keeps every archived source the catalog names mapped to a feed).

**`read` is the old rule, kept.** Every key made before scopes holds `read`, and
`read` still means exactly what a key could reach then: GET or HEAD on a fixed
list of upstream-feed prefixes (`LEGACY_READ_PREFIXES` — the emergency-service,
traffic, transport, aircraft, AIS, weather, power, shark-report and boundary
routes, plus `/api/pager/` and `/api/radio/`), anything else
`403 api_key_path_forbidden`. The list had the camera and news routes too until
the owner took cameras and news out of what keys read (2026-11-02);
`/api/traffic/cameras`, under the traffic prefix, is excluded by name. A key holding `read`, or holding nothing in the
new form, is judged by that list alone, so turning scopes on changed nothing
for existing keys. New keys are granted feeds — the staff table does not offer
`read`, though the approve route still accepts it from staff — and a user
cannot ask for it.

Refused requests of either kind still spend the key's per-minute budget, so
scanning for an opening is not free.

One route is declared `ANY_KEY`: `GET /api/v1/openapi.json`, the API
description. Every valid key may fetch it, `read` keys included, and what comes
back is filtered to that key — the public operations plus those its scopes
reach (`api/v1/openapi.ts`, which reuses the gate's own check). The document is
generated from the routes by `npm run openapi` into `backends/node/openapi.json`
and committed; `test/unit/api/openapi.test.ts` fails while it is stale. The
reference page is on the site (`api-docs.html`, viewer vendored at
`vendor/scalar/`), not here. It reads `GET /api/api-keys/openapi.json` with the
Supabase session (`api/apiKeys.ts`): the same document filtered to the union of
the person's active keys — an operation stays when any one of them reaches it
(`filterSpecForKeys`) — the whole of it for owner and staff, and
`403 no_active_key` for someone with no active key.

**Roles live in the backend's own PostgreSQL, not in Supabase.** Supabase is
authentication and identity only. Never propose row-level security on the
incident or role tables — RLS is a Supabase feature and does not apply to them.

### Contact: tickets, bug reports, suggestions

The contact page sends three things, and they are two kinds of thing.

A **support ticket** (`api/tickets.ts`, `support_tickets` + `support_ticket_messages`,
migration 114) is a private conversation: subject, category, a message thread, and a
status that says whose court the ball is in (`open` — staff must act; `awaiting_user`;
`closed`, which the user can reopen by replying for 72 hours). Owner and `support`
work the queue on the staff page and can open a ticket *at* a user. A new ticket or
user reply is a Discord staff alert (kind `ticket`); staff replies and closes are a
bell to the owner.

A **bug report or suggestion** (`api/feedback.ts`, table `feedback`, migration 128)
is the report, its description and a **status** staff set — no thread — and it is
**public**: every one is posted to a Discord board channel with its status, and each
status change is relayed under that post. Statuses are `open`, `in_progress`, `done`,
`declined`, `duplicate`, labelled in the kind's own words (Looking into it / Planned,
Fixed / Added, Won't fix / Not added). When a report needs a back-and-forth, staff
**link it to a ticket** (`POST /api/staff/feedback/:id/ticket`, or `…/ticket/new` to
open one at the reporter with the report quoted); a handler's ticket view lists the
reports linked to it. Reports are never pruned. Five open per reporter.

They arrive two ways. From the site, a logged-in account (`POST /api/feedback`,
`source = 'site'`). From Discord, any member of the server posting in an intake
channel: the bot files it with `POST /api/discord/feedback`, gated by
`services/auth/botSigned.ts` — the static key **and** an HMAC of the body in
`X-Bot-Signature`, computed with `BOT_ACTION_SIGNING_SECRET` and the outbox's
canonical form (`verifyBotActionSig`). Without the secret the route answers 503:
nothing else tells the bot from anyone holding the key. If the reporter's Discord is
linked to a site account (`findUserByDiscordId`, a walk of the Supabase admin list)
the report is filed under it too. The board post's message id comes back on
`POST /api/discord/feedback/:id/board` and becomes the staff page's board link.

Everything Discord-side goes through the signed outbox (`services/staffNotify.ts`):
the staff alert (kinds `bug_report`, `suggestion`, with a new `update` event for a
status that is not the end of the matter), `feedback_channels` whenever the owner
changes a kind's intake or board channel on the staff page (`PUT
/api/staff/discord-notify {feedback}`), and `feedback_board` (`new` for site reports
— the bot posts Discord ones itself, with their attachments — `status`, `delete`).
See `docs/components/discord-bot.md`, "Bug reports and suggestions".

## Configuration

`src/config.ts` is the **only** place `process.env` is read. Everything else
imports `config`. zod parses once at startup and prints a flat list of problems
and exits if anything required is missing or malformed.

`PORT` defaults to 3000. `NODE_ENV` defaults from the legacy `DEV_MODE` flag so
one `.env` drives everything.

**Two variables are required and have no default, and the backend refuses to boot
without either**: `NSWPSN_API_KEY`, the shared static key for the server-side
callers that cannot hold a session token or a named key, and
`BROWSER_TOKEN_SECRET`, which signs browser session tokens. Both used to fall
back — the key to a literal, the secret to the key — and in both cases the
fallback was a value published in this repository's history, so a deploy whose
`.env` forgot the variable ran on a credential an attacker already has. Failing to
boot is the only safe answer, because there is nothing a default can be that is
not also public. `BROWSER_TOKEN_SECRET` must be at least 16 characters; generate
one with `openssl rand -hex 32`.

Beyond those two, almost every variable is **optional**, and the pattern is
consistent: an unset variable turns its feature off rather than breaking the
server. Specifically —

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
`ADSB_DISABLED`, `ADSB_HISTORY_IMPORT` (on by default), `TRANSPORT_DISABLED`, `TFNSW_DISABLED`,
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

PostgreSQL, 137 numbered migrations in `src/db/migrations/`, applied by
`src/db/migrate.ts` at boot and by `npm run migrate` before the restart on
deploy. The runner is idempotent, so applying twice is a no-op. New schema is a
new numbered file; never edit an applied one. The most recent are `130`–`136`,
which moved `node_radio_events` to daily partitions (below), and `137`, which
dropped the recorded flag's columns and `node_pending_recordings`.

A file whose **first line** is `-- noTransaction` runs outside a transaction,
which is what `CREATE INDEX CONCURRENTLY` needs; the runner sends the whole file
as one query, so such a file must hold exactly one statement. `131` and `132`
are the first. Deploy runs migrations while the old process is still serving,
so anything that would hold a table lock for more than a moment belongs in one
of these, and a transactional file that takes a brief exclusive lock should
start with `SET LOCAL lock_timeout` so it fails the deploy rather than queueing
every writer behind a long read.

The database runs in its own Proxmox LXC container: 2 vCPU, 5 GB RAM,
`shared_buffers` 1 GB, on a ZFS mirror since 2026-11-03 (it was on a single
2.5-inch 5400 rpm drive before, which is where the cold-read numbers below
were measured). Nothing here
is sized for a working set that does not fit in page cache, which is why the
next section exists.

Three separate pools:

| Pool | Variable | Access |
|---|---|---|
| Main | `DATABASE_URL` | read/write |
| rdio-scanner | `RDIO_DATABASE_URL` | **read-only** — `src/services/rdio.ts`. Never add a write path |
| Bot data | `BOT_DATA_DATABASE_URL` | read, for the Discord dashboard. The bot writes it |

`src/services/rdio.ts` sets a global type parser for OID 1114
(`timestamp without time zone`), because rdio stores UTC values in a naive column
and `pg` would otherwise parse them in the Node process's local timezone.

**The main read pool runs with JIT off** (`options: '-c jit=off'` in
`db/pool.ts`). Postgres JIT-compiles a statement when its *estimated* cost passes
`jit_above_cost`, and on this workload the estimate is what goes wrong —
subqueries, composite `COUNT(DISTINCT)`s and range predicates the planner cannot
see. The measured case was the staff overview: 824 ms of a 906 ms query spent
compiling for ~80 ms of work. On a 2-vCPU host, compile time is taken from
everything else. The writer pool keeps JIT, because the hourly rollup builds are
genuinely large and run off the request path.

### node_radio_events: the one table whose storage needs watching

Every reception the radio nodes report lands here — ~6 M rows on a 30-day
retention, ~220 k a day — and it is read by the hourly rollup, by every staff
Data page and by the ingest path's own grouping lookup. Before migration `124`
it carried 5795 MB of index against 1454 MB of heap, in a container with 1 GB of
shared buffers; that ratio decides whether a day-wide range scan costs ~120 ms
warm or ~35 s cold, and the cold case stalled the rollup's catch-up after the
`123` deploy until `services/nodeHourlyRollup.ts` was given smaller batches and
its own statement timeout.

**Since `135` it is partitioned by UTC day.** Partitions are named
`node_radio_events_YYYYMMDD`, bounds `[day, day + 1)` in UTC. The table as it
was before is the first partition, `node_radio_events_legacy`, covering
everything below the bound `133` set (three days after that deploy); it was
attached, not copied, so the cutover was catalogue-only. Readers did not
change: they all bound `received_at`, so the planner reads only the days they
ask for.

- **Retention is `DROP TABLE`**, one partition a day, in
  `services/nodeEventsPruner.ts`: a day goes once its upper bound is past both
  30 days ago and the rollup's high-water mark (the old rule, unchanged). That
  replaced ~36 k single-row `DELETE`s an hour and the dead tuples they left. The
  legacy partition is trimmed by the old `DELETE` until its own bound passes the
  line, then dropped the same way.
- **The runway**: the same hourly job, and boot before the port binds, make
  sure a partition exists from yesterday to a week ahead, through
  `ensure_node_radio_events_partition(date)` (migration `135`), which creates
  the table, sets its storage parameters and then `ATTACH`es it (a lighter lock
  than `CREATE ... PARTITION OF`). An insert that still finds no partition fails
  that one event, which the agent re-sends, and triggers the runway at once.
- **Every DROP and ATTACH runs under a 3 s `lock_timeout`.** Both briefly lock
  the parent, which every insert also needs; waiting behind a long staff read
  would hold ingest up for as long as the read. A timeout is retried next hour.
- **Storage parameters are per partition.** A partitioned parent's are not
  inherited, so the function sets the 0.05 autovacuum factors on each day (see
  below). Day partitions keep the default fillfactor since `137`.
- **ANALYZE the parent.** Autovacuum analyses partitions but never a
  partitioned parent, whose statistics the planner uses for the grouping
  lookup's self-join; the pruner does it daily and after each drop.
- **Eight indexes per day**, not twelve: the dedup index (below), `id`,
  `logical_call_id`, `received_at`, `(system, talkgroup, received_at)`,
  `(source_unit, received_at)`, `(node_id, talkgroup, received_at)` and `110`'s
  partial talkgroup index. The four partial call-predicate indexes from
  `080`/`115`/`116` were not carried over (the predicate matches 81 % of rows,
  and a day partition bounds the time range they existed to order); they
  remain on the legacy partition until it is dropped.
- **There is no primary key on the parent.** A partitioned table's unique
  indexes must include the partition key, and `id` must stay one global
  sequence for `logical_call_id` and the `ORDER BY id` in the rollup, so `id`
  has a plain index on every partition instead.

Things to know before touching it:

- **The dedup index is correctness, not performance.** Both ingest paths say
  `ON CONFLICT DO NOTHING` (`services/nodeEvents.ts`), which is what makes a
  re-sent agent batch idempotent. It is `(node_id, stream_id, source_event_id,
  received_at)` — `received_at` because a partitioned table requires it. Never
  drop it. The conflict target is left open on purpose: a named target must
  match exactly one valid index, which would tie the insert to the state of a
  migration. `scripts/reindex-nre-unique.mjs` rebuilt the old three-column
  version and does not apply since `135`; a day partition's copy is ~1/30 the
  size and is dropped with its day, so bloat no longer accumulates.
- **A reception more than 48 h from server time is skipped**, not re-stamped
  with the server's clock (`radioReceivedAt`). With `received_at` in the dedup
  key, a re-stamped re-send would be stored twice, and a reception filed at a
  made-up time could never join its real call anyway. It is logged, and counted
  as neither accepted nor failed so the agent does not re-send it forever. The
  pager path still re-stamps.
- **The node-led indexes are inflated by a type choice.** `node_id` is `TEXT`
  averaging 35 bytes — a UUID stored as text — so every index leading on it pays
  that per row. By contrast `idx_nre_time` (`received_at` alone) is 123 MB and
  shows no bloat at all. Narrowing the column would shrink four indexes at once,
  but it is a foreign key to `nodes(id)` and has not been attempted.
- **`logical_call_id` is set by the INSERT, never by a follow-up UPDATE.** It used
  to be stamped immediately after insert, which left a dead row version for
  *every* event — and since the column is indexed, the update could never be HOT,
  so all fourteen indexes took a second entry each. That was the engine behind a
  standing 17 % dead-tuple ratio. The grouping lookup already knows the call
  before inserting; the event that OPENS a call draws its own id from
  `node_radio_events_id_seq` in a CTE and writes it to both `id` and
  `logical_call_id`. That last step came later (`130`), once production showed
  30 % of rows open a call, not the one in ~29 that had been assumed. If you add
  a column to either insert, keep it that way.
- **Nothing updates a row after insert except a patch merge.** The `recorded`
  flag and `audio_bytes` were stamped by matching each rdio upload back to one
  reception (`markRecorded`, with `node_pending_recordings` for uploads that
  beat their event). That matching was removed and `137` dropped the columns,
  their hourly counterparts and the pending table. Per-node upload counts and
  bytes still come from the relay itself, in `node_call_stats`.
- **`id` is a plain sequence default, not an identity** (`130`). PostgreSQL 17+
  will not attach a table with an identity column of its own. The sequence is
  named in the insert SQL and is deliberately not `OWNED BY` any column: it
  belonged to the table that is now the legacy partition, and dropping that
  must not take the parent's default with it.

`124` also sets `autovacuum_vacuum_scale_factor` and
`autovacuum_analyze_scale_factor` to 0.05 on `node_radio_events` and
`adsb_tracks` (and production also carries `fillfactor = 85` on the legacy
table, set by hand); `135` put the same three on every day partition. `137`
took fillfactor back off the days: its 15 % reserve was there so the recorded
update could stay on its page, and that update no longer exists. At the 0.2 cluster default, `node_radio_events` needed ~1.13 M
dead tuples before autovacuum would fire and simply idled below the threshold for
days at a time. These are deliberate per-table overrides — not leftovers to tidy
away.

### Aircraft history: one row per minute, written once

The map's history playback (`GET /api/v1/adsb/history?at=&trail=`) asks one
question: where was every aircraft between `at − trail` and `at`. So history
is stored as time slices, the way readsb's heatmap is: `adsb_track_chunks`
(`139`) holds one row per minute with every aircraft heard in it, as `json`
`{hex: {c, r, t, e, s, p: [[msIntoMinute, lat, lon, alt]]}}` under lz4. A
30-minute window is at most 31 primary-key rows.

- **Collected as heard, not re-cut from the live trail.**
  `services/adsbTrackArchive.ts` takes each poll's observed (never estimated)
  aircraft and our receivers' 1 Hz points into a per-minute accumulator. The
  live trail in `sources/adsb.ts` is a display structure that thins and
  crushes itself, and the old hour-row archive (`adsb_tracks`, `103`) re-cut
  history from it every minute, so stored hours got coarser after they were
  written and a restart could wipe the first part of the hour.
- **Written once.** A minute is reduced (Douglas-Peucker at ~50 m, a point at
  least every 30 s) and inserted about 15 s after it closes, never updated:
  one insert a minute in place of ~26 k updates an hour. Positions arriving
  for a minute already written are dropped and counted.
- **`seq` lets two processes share a minute.** A graceful shutdown writes the
  minute in progress; the next process writes its part of the same minute
  under the next seq, and the reader unions them. Seq −1 rows are the
  one-off backfill from `adsb_tracks`.
- **Only a write proves a minute is final.** The route long-caches an answer
  only when its minute is at or below what this process has written
  (`adsbArchivedThroughMs`); before the first write after a restart,
  everything gets the 30 s cache.
- **Registration by hex** lives in `adsb_identities`, written only when it
  changes (and refreshed daily so retention never takes a flying aircraft),
  for the receiver hover labels that a time-sliced table cannot answer by
  index.

`adsb_tracks` was kept as the rollback path until the playback had been
checked across the cutover, then dropped by `140`.

**Gap fill from adsb.lol's archive (seq −2).** adsb.lol publishes each UTC
day's full traces (ODbL) as a GitHub release, `adsblol/globe_history_<year>`,
about 14:30 AEDT the next day, so this is daily: nothing finer exists.
`services/adsbGlobeHistory.ts` checks hourly for a released day in the last
week that has no seq −2 marker and imports the newest one, in a worker
thread (`adsbGlobeHistoryWorker.ts`): it streams the ~3.7 GB tar
(`lib/tarEntries.ts`), keeps trace points inside the Australian bbox, and per
aircraft-minute adds only the history points more than 10 s from a stored
point that survive the live writer's reduction together with the stored ones
(a turn, or a gap of 30 s+). A minute with nothing stored gets the history
on its own. Rows are written `ON CONFLICT DO NOTHING`, and a `'{}'` row at the
day's first minute marks it done, last, so a cut-short run is simply redone.
About 110 GB a month of download; `ADSB_HISTORY_IMPORT=false` turns it off,
and so does `ADSB_DISABLED`.

### Rollup reads: the seam has to be visible to the planner

Every fleet-wide staff Data read (overview tiles and lists, systems, talkgroups,
the Units list, the system and site drill-downs and their Radios list) is two
halves: the hourly rollup tables below a seam, and raw
`node_radio_events` from the seam onwards. The seam is where the rollup has got
to (`node_rollup_state.rolled_up_to`, capped at the current hour).

It used to be a scalar subquery pasted into every statement, and that was the
single largest cost on the Data tab. Postgres cannot see a subquery's value at
plan time, so it guessed a third of the table for the live hour — 1.49 M rows
against ~7.5 k — and the inflated cost triggered JIT. Measured on prod, same
rows: **906 ms as a subquery, 19 ms with the value visible**. That was ~75% of
every warm overview poll.

So handlers now call `resolveRollupEdge()` once per request, on the database's
clock, and pass the result to `rollupEdgeSql()`, which renders a `timestamptz`
literal. `radioTotalsFromRollup` and `radioListsFromRollup` take the seam as a
**required** argument, so no new caller can quietly bring the subquery back. A
bind parameter would plan as well; the literal keeps every call site's parameter
numbering unchanged.

The other half of the Data tab's slowness is storage, not SQL. The first open
after the cache has gone cold took 23.8 s, and a cold 30-day read 17.7 s,
against 60–157 ms warm. Index bloat on this table makes that worse.

**Do not read a whole window of `node_radio_events` on a staff request.** The
Units list (`/api/node-data/radio-aliases`) did, grouping every event in the
window by radio, and on prod it never finished: 109 s for 24h and 74 s for 7d
against the 30 s timeout. Only ~4–7 k pages came off disk. The rest was
visibility checks: rows newer than the last vacuum are checked against the heap
one by one, in index (radio-id) order, at ~26 ms a random read on this storage.
It now reads `node_radio_hourly_unit` like the overview's top-radios list, in one
pass over its whole history (~27 MB): 80–200 ms for every window, All included.
That also made it list every radio heard rather than only the ~8 % that
transmitted an over-the-air alias — the endpoint keeps its old name. Migration `125` added `last_heard` to that table so "Last
heard" stays exact; rows rolled up before it fall back to the hour. Only a
node-scoped request still reads detail, because the rollup has no node column.

The system and site drill-downs (`/api/node-data/system`, `/site`), the Radios
list inside them (`/radios`) and the talkgroup list's per-row top site / node
did the same, and timed out the same way: a 7-day site is ~116k rows over ~40k
heap pages, fast when cached (66–232 ms per query) and past 30 s when not.
Migration `126` added `node_radio_hourly_heard`: one row per (hour, node,
system, site, talkgroup, radio), each reception filed under the one radio that
keyed it, so its receptions sum to `node_radio_hourly_sys`'s exactly (checked on
prod: 58 of 58 hour×site groups equal). It also added `encrypted_calls_site`
to `node_radio_hourly_sys` (and a recorded counterpart, no longer written): the
fleet-wide column credits a simulcast call to its lowest site, which on one prod range read 3,374 encrypted
calls across the sites against 7,114 per site. `heard` is pruned at 32 days —
nothing reads it past the drill-downs' 30. Fleet-wide drill-downs read
`drillSources` / `drillFromRollup`; node-scoped ones keep their raw queries.
Their `calls` is receptions, as everywhere else on the tab, not stored rows,
which counted a grant and its call twice.

**Backfilling a rollup without moving the seam.** Migrations 081/084/085/123
rebuilt history by rewinding `rolled_up_to` — which is also the seam, so every
Data view fell back to a month of raw detail until the catch-up finished (and
after 123 it stalled). `node_rollup_state.rederive_below` is a second cursor
that walks DOWN: each tick, after the forward pass has had first call on the
batches, the job re-derives the hours just below it, newest first, and lowers
it in the same transaction. It stops three hours above the oldest detail row —
re-deriving an hour the pruner has half-deleted would replace its rows with a
fraction of them — and clears itself. Use it for the next new rollup column:
set `rederive_below = rolled_up_to` in the migration and leave the seam alone.
Until it passes an hour, that hour reads as before (NULL / no rows), newest
windows first.

**Decode errors are not sites.** A corrupted control-channel decode attributes
a reception to a site or system that does not exist, and SDR-Trunk keeps every
site id a channel has ever decoded in that channel's site list, re-sending it
every minute. So a single reception listed NSWPSN's Braidwood RD twice (R1 S244,
named after the real S100's channel) and a 30-day window grew a "system 2201".
The Systems tree and the system drill-down list a site or system only once it
has been heard in `SITE_MIN_HOURS` (2) distinct hours of the window — the
phantom was heard in 1 hour over 30 days, the quietest real site in 54 — and
the drill-down lists one site per (node, channel) from snapshots: the one with
traffic, or for a quiet channel the one whose control frequency it decoded.

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

- [`../api.md`](../api.md) — the public data API on `/api/v1/`: keys, scopes,
  versions, response and error shapes, and how to add an endpoint.
- [`../architecture.md`](../architecture.md) — the system, and the full trace
  from a transmission in the air to where it surfaces.
- [`transcription.md`](transcription.md) — the whisper router.
- [`radio-node.md`](radio-node.md), [`pager-node.md`](pager-node.md),
  [`aircraft-node.md`](aircraft-node.md) — what talks to the ingest routes.
- [`../uptime-kuma-monitors.md`](../uptime-kuma-monitors.md) — monitors against
  `/api/status`.
