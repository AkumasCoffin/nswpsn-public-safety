# AusAware architecture

How the whole system fits together, and the full trace from a radio transmission
in the air to where it surfaces. A pager transmission becomes a map pin; a voice
transmission does not, and step 9 explains why.

Every claim here names the file it came from. If a path in this document does not
exist, the document is wrong — fix it.

Before changing anything, read [`../AGENTS.md`](../AGENTS.md).

---

## The shape of it

```
      public APIs                 off-air RF                 contributors
  (RFS, BOM, LiveTraffic,     (P25 voice, POCSAG,          (map editor,
   power, FIRMS, ADS-B,        ADS-B 1090 MHz)              The Wire)
   TfNSW, AnyTrip, …)                 │                          │
          │                           ▼                          │
          │                  ┌──────────────────┐                │
          │                  │  feeder nodes    │                │
          │                  │  radio · pager   │  Go 1.26       │
          │                  │  aircraft        │                │
          │                  └────────┬─────────┘                │
          │                   WS + HTTPS relay                   │
          ▼                           ▼                          ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │              backends/node  —  TypeScript + Hono                   │
   │                                                                    │
   │   src/index.ts        boot: migrate, register, prewarm, poll, serve │
   │   src/services/       poller · node hub · whisper router · LLM      │
   │   src/store/live.ts   LiveStore — current state, in memory          │
   │   src/store/archive.ts ArchiveWriter — history, into Postgres       │
   │   src/api/*.ts        57 route modules, composed by src/server.ts  │
   └───────┬──────────────────────────┬─────────────────────┬───────────┘
           │                          │                     │
           ▼                          ▼                     ▼
   static site (repo root)      Discord bot            PostgreSQL
   Leaflet, no build step       discord-bot/bot.py     (canonical store)
   map · live · logs · wire     presets · dispatch     119 migrations
```

Three things are **outside** the backend and must not be confused with it:

- **The central rdio-scanner** — its own server with its own PostgreSQL. The
  backend forwards receptions into it, and reads its database **read-only**
  (`src/services/rdio.ts`). Never writes there.
- **The central Pagermon** — self-hosted. The backend polls it for the map
  (`PAGERMON_URL`) and forwards pager node traffic into it
  (`PAGERMON_INGEST_URL`).
- **The two whisper servers** — external hosts, source not in this repo. See
  [`components/transcription.md`](components/transcription.md).

**Supabase is authentication and identity only.** PostgreSQL is the canonical
store for incidents, archive, roles, node registry and everything else.

## Backend boot order

`backends/node/src/index.ts` is the entry point. `preflight()` runs before the
port is bound, in this order:

1. `liveStore.hydrateFromDisk()` — repopulate the live cache from
   `STATE_DIR/<source>.json` so a restart does not serve empty data.
2. `runMigrations()` — `src/db/migrate.ts`, idempotent. A failure is logged but
   startup continues, so `/api/health` stays observable.
3. `registerAllSources()` + `registerAllPowerSources()` — every upstream
   declares itself in the registry. Nothing fetches yet.
4. `liveStore.startPersistLoop()`, `ensureArchivePartitions()`,
   `archiveWriter.startFlushLoop()`.
5. Background loops: activity mode, filter cache, stats archiver, hourly
   cleanup, node-event pruner, node hourly rollup, ADS-B daily flush, node
   uptime flush, whisper health probe, video processor, Wire purge, memory watch.
6. `prewarmAll(30_000)` — every source's first poll fires in parallel with a 30s
   ceiling, so the first request after a restart is not served an empty store.
7. `startPolling()` — the registry is walked and each source gets its own
   interval.
8. Then `createApp()` from `src/server.ts`, `serve()` on `config.PORT`
   (default 3000), and `attachNodeWebSockets(server)`.

Shutdown (`SIGTERM`/`SIGINT`) drains in-flight requests first, then stops the
loops in dependency order — pollers before the writers — then flushes LiveStore
and ArchiveWriter and closes all three connection pools. This matters: a
transcription in flight is a reception's only chance at a transcript, and rdio
does not come back for it.

## The two stores

`src/store/live.ts` — **LiveStore**. The current snapshot of every source, in
memory, keyed by source name. Every `/api/*` live endpoint reads from here and
**never from PostgreSQL**. Persisted per source to `STATE_DIR/<source>.json` via
temp-file-plus-rename, so the file on disk is always either the previous
snapshot or a complete current one.

That separation is the point. The old architecture kept live state and history in
the same tables with UPDATE-driven liveness columns, and the write contention
that produced was the root cause of repeated cascading failures.

`src/store/archive.ts` — **ArchiveWriter**. Batched inserts into four
partitioned tables: `archive_traffic`, `archive_rfs`, `archive_power`,
`archive_misc` (`ArchiveTable`, `archive.ts:32`). A source declares its family in
the registry; `familyTable()` maps family to table. `/api/data/history` reads
them — the read side's list is `ALL_ARCHIVE_TABLES` in
`src/services/dataHistoryQuery.ts:93`, the same four — and so does `/logs`.

## Path 1 — a polled source reaches the map

Take NSW RFS incidents.

1. **Declare.** `src/sources/rfs.ts` calls `registerSource({ name:
   'rfs_incidents', family: 'rfs', intervalMs: 60_000, fetch })`. The registry
   contract is in `src/services/sourceRegistry.ts`: a stable `name` (also the
   LiveStore key), an archive `family`, a poll cadence, and an async `fetch()`
   that returns a snapshot of whatever shape that source likes — the store is
   opaque to it.
2. **Register.** `src/sources/registerAll.ts` calls that module's `register()`
   once at boot.
3. **Schedule.** `src/services/poller.ts` walks the registry and arms one
   `setInterval` per source at its own `intervalMs`. Every source polls on that
   single interval, 24/7 — the old active/idle split tied to a page-activity
   heartbeat was removed, because the Discord bot is a round-the-clock consumer.
   A failing fetch increments a failure counter and gets backoff
   (`src/sources/shared/backoff.ts`), and feeds per-source health
   (`src/services/sourceHealth.ts`, surfaced on `/api/status`).
4. **Store.** The snapshot lands in LiveStore under `rfs_incidents` and fans out
   to `archive_rfs` rows. A source whose snapshot is neither a GeoJSON
   `FeatureCollection` nor a flat array supplies its own `archiveItems()` — the
   pager source does, so each message becomes its own row instead of the whole
   snapshot landing as one wrapper.
5. **Serve.** `src/api/rfs.ts` reads LiveStore and answers
   `GET /api/rfs/incidents`. `src/server.ts` mounts it. The global
   `requireApiKey` gate applies; `/api/health`, `/api/config`, `/api/heartbeat`
   and a short list of others are public. A browser-usable key is handed out by
   `/api/config`, which is why CORS is a narrow allowlist of exact first-party
   origins rather than a wildcard.
6. **Draw.** `map.html` fetches `/api/rfs/incidents` and adds Leaflet markers.
   For a set of cheap GETs the response carries
   `Cache-Control: public, max-age=30, stale-while-revalidate=300`, so repeat
   hits are absorbed by the CDN.

### Registry contents

36 entries on a default boot — 38 are declared, and the two Ausgrid sources do
not register unless `AUSGRID_DISABLED` is set to `false`
(`backends/node/src/sources/ausgrid.ts:262`), which it is not by default.
Cadences range from 8s (`adsb_aircraft`) to 30m (`weather_current`). The full
list with each cadence is in the
[backend component doc](components/backend.md#the-source-registry).

### Layers that are not registry sources

- **Public transport** — `src/sources/tfnsw.ts` and `src/api/transport.ts`.
  AnyTrip supplies route metadata, colours, shapes and headsigns; official TfNSW
  GTFS-Realtime supplies authoritative vehicle coordinates where it knows the
  trip, plus service alerts. Positions are **off by default**
  (`TFNSW_POSITIONS_DISABLED=true`) because layering the raw TfNSW frame over
  AnyTrip's interpolation made trains jump. Alerts need only `TFNSW_API_KEY`.
  `/api/transport/*` is deliberately excluded from the cacheable-path list: that
  middleware ignores query strings, so a CDN would cross-serve one viewport's
  vehicles to every viewport.
- **MarineTraffic** and **Central Watch** — both need a real browser session, so
  each parks a Playwright Chromium tab (`src/services/marinetrafficBrowser.ts`,
  `src/services/centralwatchBrowser.ts`). Both have kill switches
  (`MARINETRAFFIC_DISABLED`, `CENTRALWATCH_DISABLED`) so a host without Chromium
  still boots and serves last-good JSON.
- **News RSS** — `src/sources/news.ts`, `/api/news/rss`.
- **What3Words** proxy, **boundaries**, **agency reference data**.

## Path 2 — a radio transmission reaches the public site

This is the trace the project exists for. Each step names its file.

### 1. In the air

A P25 transmission on the Government Radio Network. A volunteer's antenna and
RTL-SDR pick it up. If the talkgroup is encrypted there is no audio to recover —
the reception is still *observed* (identity, site, encryption flag) but it
carries nothing to listen to or transcribe. Most police traffic is in this
category.

### 2. SDR-Trunk decodes it

The radio node supervises a **forked, headless SDR-Trunk** build
(`AkumasCoffin/sdrtrunk-vce`, branch `feature/node-control`). The agent launched
it, downloaded it, and pinned it: `backends/node/assets/node-versions.json`
carries the version and per-platform sha256.

The fork exists because upstream SDR-Trunk is a GUI application. The fork adds a
headless control server — REST on `127.0.0.1:<P>`, spectrum WebSocket on `P+1` —
so an agent can drive it with no display and no human.
See [`components/forked-runtimes.md`](components/forked-runtimes.md).

### 3. Two things leave SDR-Trunk, on different paths

**Audio** goes to the node's own local rdio-scanner (also a pinned fork, on
`127.0.0.1:17391` — `feeder-nodes/radio-node/cmd/nodeagent/main.go:54-60`).
`internal/configapply` has configured that rdio with exactly one *downstream*,
pointing at the agent itself.

**Metadata** is read by the agent directly from the control server:

- `internal/activityship` GETs `/activity/events` after a persisted cursor every
  ~4s — a cursor-paged feed of reception, data and page activity carrying the
  real P25 identity (system id, talkgroup, source radio, site, encryption flag).
- `internal/siteship` GETs `/site/snapshots` every ~60s — per-P25-site metadata,
  including active patch groups.

### 4. The agent buffers, then relays

`internal/relay` is a localhost HTTP listener impersonating rdio's `call-upload`
endpoint. It **must** answer 200 — the local rdio drops the recording on
anything else — so it accepts, enqueues to `internal/queue` (a disk-backed,
bounded FIFO, one file per reception) and returns.

The queue drains to the backend over HTTPS:

| Agent sends | Backend route |
|---|---|
| audio + metadata | `POST /api/node-ingest/call-upload` |
| activity batches | `POST /api/node-ingest/activity` |
| site snapshots | `POST /api/node-ingest/site-snapshots` |
| site survey results | `POST /api/node-ingest/site-survey` |

All in `backends/node/src/api/node-ingest.ts`. Authentication is
`X-Node-Token` + `X-Node-Install` — **not** the site API key — which is why the
`/api/node-ingest/` prefix is exempt from the global key gate
(`src/services/auth/apiKey.ts`).

The disk queue is what makes a node survive its own internet. A dropped link
delays receptions; it does not lose them.

### 5. The backend forwards the audio and keeps the key

`call-upload` swaps the node's key for the single server-held
`RDIO_INTERNAL_API_KEY` and forwards into the central rdio-scanner at
`RDIO_INTERNAL_URL`.

That indirection is the design: there is exactly **one** key inside the central
instance, so cutting a node's feed is toggling its `enabled` flag in the staff
panel — not juggling rdio keys. The node never holds that credential. The same
applies to the pager path and `PAGERMON_INGEST_API_KEY`.

### 6. The backend records the reception

`src/services/nodeEvents.ts` writes `node_radio_events`.

The non-obvious part: those rows come **only** from the activity feed, not from
the audio upload. Activity events carry the real P25 identity; the upload only
calls `markRecorded()` to flag the closest matching event row `recorded=true`
with its audio size. So a reception exists in the record whether or not anyone
could hear it — which is exactly right for an encrypted talkgroup.

**Grouping.** One transmission heard by N nodes arrives as N receptions. Rows
within ±5s on the same `(systemId, target)` share one logical id — the detail-row
id of the first member. Assignment is serialised per key with
`pg_advisory_xact_lock(hashtext(key))` so two simultaneous receptions cannot each
start their own group. The ±5s window is not arbitrary: it is SDR-Trunk's own
`CrossSiteCallDeduplicator.SAME_CALL_WINDOW_MILLISECONDS`.

Each write lands a detail row (30-day retention —
`src/services/nodeEventsPruner.ts`) and rolls into permanent hourly buckets
(`node_radio_hourly`, `node_radio_hourly_sys`) in the same transaction.

Every `record*` and `mark*` function is fire-safe: a failure is logged and
swallowed. Capture must never affect the relay path.

### 7. Transcription, if there is audio

The central rdio's `transcripts` plugin (from
`AkumasCoffin/rdio-scanner-plugins`) POSTs the audio to
`<backend>/api/whisper/v1/audio/transcriptions`. `src/api/whisper.ts` hands it to
`src/services/whisperRouter.ts`, which picks a healthy, non-draining faster-whisper
server from `WHISPER_BACKENDS` in preference order. Full detail in
[`components/transcription.md`](components/transcription.md).

The transcript is stored by the plugin, in the central rdio's database.

### 8. Where it surfaces

**On the public map — the repeater layer.** That layer is **two** endpoints, and
the split matters: one supplies the geography, the other the liveness.
**Neither carries a reception.**

*Geography* — `GET /api/radio/grn-sites` (`src/api/radio-public.ts:246`) serves
the GRN repeater site list from the `grn_sites` table
(`SELECT id, data FROM grn_sites ORDER BY id`, `:253`), cached in process for
30s and serving stale on a query failure (`:94`, `:258-262`). The table is
seeded once at boot from `data/nswpsn/NSW GRN Version 1.json` when it is empty
(`seedGrnSitesIfEmpty`, `:215`, called from `index.ts:204`). `map.html:18474`
fetches the list. It is **owner-editable live**:
`PATCH /api/radio/grn-sites/:id` (`:287`) is behind `requireRole(isOwner)` and
constrains the body to the dataset's own 16 keys via `GrnSitePatchSchema`
(`:268`) — `map.html:18602` is that editor, authenticated with the signed-in
person's Supabase JWT rather than the site key.

*Liveness* — `GET /api/radio/monitored-sites` (`src/api/radio-public.ts:97`)
lists the P25 GRN sites the fleet is receiving right now. `map.html:18451`
fetches it, inside `_rptLoadMonitored`, and `_rptApplyMonitored` badges each
matching repeater pin as *Monitoring*. The comment block at `map.html:18386` is worth reading before you
touch any of it: the agents re-report every ~60s regardless of traffic, so the
badge is *"a live lock, not a traffic echo"*.

Both are listed in `CACHEABLE_PATHS` (`src/server.ts:259-260`), so the CDN
absorbs repeat hits.

"Right now" is SDR-Trunk's own `site_last_seen_ms` — when the decoder last
actually heard the site — inside a five-minute window. Three predicates are
load-bearing there, and the middle one was found the hard way: the agent
re-reports every site it has ever observed on every poll, so `received_at`
freshness alone badged a stopped channel as live forever. `site_last_seen_ms`
freezes the moment decoding stops, so it carries the liveness.

The response carries **site identity only** — RFSS/site, the GRN's zero-padded
`"004-083"` form, decoded site name, NAC, how many nodes hold it, when it was
last confirmed. Never node identity, never node location. Which receivers exist
and where they are stays role-gated.

**On `/live`.** `live.html:3090` reads `/api/summaries/latest` — hourly summaries
generated by `src/services/llm.ts` (Gemini, prompt in
`backends/prompts/rdio_hourly.txt`) from the transcripts in the central rdio
database.

**On `/staff`.** `staff.html` holds a WebSocket to `/api/node-ws/staff`
(`src/api/node-ws.ts`, authenticated by a first-message Supabase JWT, `owner` or
`dev` only) for the Live view, and reads `/api/node-data/*` for decode health,
per-site decode, coverage, relationships and history. This is where a reception
is visible as a reception.

**On `/feeds` — the audio itself, and this is the widest public surface of the
lot.** `feeds.html` is a link page, not a player: it carries no audio element
and calls no AusAware API except `/api/heartbeat` (`feeds.html:164`). It links
out to three services hosted alongside AusAware but **not served by this
backend**:

| Link (`feeds.html`) | What it is |
|---|---|
| `radio.forcequit.xyz` (`:106`) | the **central rdio-scanner's own web UI** — live P25 receptions with per-talkgroup search and playback |
| `nsw-pager.forcequit.xyz` (`:116`) | the NSW Pagermon instance — live POCSAG messages |
| `qld-pager.forcequit.xyz` (`:126`) | a second Pagermon for QFES paging |

So the audio a radio node uploaded is publicly listenable, with no login, on
the central rdio's own interface. It does not pass through `backends/node` to
get there — the backend reads that same database read-only for transcripts and
summaries, while rdio serves its own UI. `data-sources.html:147` and
`terms.html:174` name the same hosts, and `terms.html:174-176` is the statement
of what they are: streams of *"publicly receivable, non-encrypted radio and
pager transmissions received from over-the-air signals."*

Two things follow that are easy to get wrong. The site badge on the map is
**not** how a reception is heard — it is a liveness lock, and the hearing
happens on an external host this repo does not configure. And nothing in this
repo gates those three links, so do not describe the public surface as
transcript-free or audio-free; it is neither.

**As a push, optionally.** `src/services/rdioIncidentAlerts.ts` watches the
central rdio database for a burst of transcribed receptions on one talkgroup and
publishes one ntfy notification per incident, with a cooldown so one incident is
one push. Off unless `RDIO_INCIDENT_ALERTS_ENABLED=true`.

### 9. What a radio reception does *not* do

**A radio reception does not itself drop an incident pin on the map.** Nothing
in the pipeline geocodes radio traffic into a location — there is no step that
turns a transcript into a coordinate, and no code in `backends/node/src` that
could.

Be careful with the stronger version of that claim, though, because it is false:
**the surface is narrow on the *map*, not narrow in general.** On the map itself
a reception produces only a site badge. Beyond the map it produces an hourly
summary on `/live`, a role-gated staff Live row, a permanent row in the central
rdio database — and, via the `/feeds` links in step 8, **publicly listenable
audio with a transcript search on the central rdio's own web UI, with no login
at all.** AusAware's own API is the narrow part; the rdio instance behind it is
not.

One AusAware route belongs in the same list: `GET /api/rdio/calls/:callId`
(`backends/node/src/api/transcripts.ts:93`) returns a single reception with its
transcript, is advertised in the root endpoint catalogue
(`src/server.ts:499-500`), and is reachable by anyone holding the shared browser
key. No page in this repo calls it; whether it should stay reachable is under
review.

The radio-adjacent feed that *does* produce pins is pager:

```
POCSAG page → pager node (rtl_fm | multimon-ng → internal/pagerdecode)
            → POST /api/node-ingest/pager-upload
            → central Pagermon
            → src/sources/pager.ts polls PAGERMON_URL every 60s
            → coordinates + incident id parsed out of the message body
            → LiveStore 'pager' + archive_misc rows
            → GET /api/pager/hits  (GeoJSON FeatureCollection)
            → map.html:6265 — a pin
```

**There is no geocoder in that chain, and that is the whole reason it works.**
The dispatch system puts the coordinates *in the message body*, as a trailing
`[lon,lat]` bracket. `parsePagerCoords` (`src/sources/pager.ts:75`) pulls them
out with a regex — two patterns, the bracketed form first and a looser
unbracketed fallback — and returns them swapped to `[lat, lon]`. Nothing is
looked up: no geocoder, no gazetteer, no address matching, no network call.
But the fallback pattern's bracket is optional, so it also matches any bare
comma-separated number pair in the body — a unit/lot number next to a street
number (`UNIT 5, 12 SMITH ST` → `[12, 5]`) parses as if it were coordinates,
and nothing downstream range-checks the result. So a pager pin can be **less**
accurate than whatever the dispatch system wrote, not just as accurate: the
fallback can manufacture a coordinate pair out of text that carried none.

That is also the real difference between the two paths. A pager message arrives
carrying its own coordinates; a voice transmission arrives carrying audio. One
can be mapped by parsing, the other would need something that does not exist
here.

Two consequences worth knowing:

- A page is left off the map only if `parsePagerCoords` found nothing at all —
  an FRNSW `FRINC` turnout, for instance, whose format carries an incident
  number and no comma-separated number pair anywhere in the body, so both
  regex patterns miss and it archives with `lat`/`lon` null (nullable in
  `src/sources/pager.ts:46-47`, shown in `/logs`). That is **not** the same as
  "no coordinates in the body guarantees no pin" — see above: the unbracketed
  fallback can turn an unrelated number pair (a unit and lot number, a street
  number pair) into coordinates it then maps.
- Coordinates are **inherited within an incident**. `src/sources/pager.ts:279-281`
  runs two passes: parse ids and coords, then group by `incident_id` so every
  message in a group inherits whichever message in that group had explicit
  coords. So a follow-up page with no bracket still lands on the right pin,
  provided something else in its incident carried one.

So the honest one-line answer to "trace a transmission to a pin": a **pager**
transmission becomes a pin; a **voice** transmission becomes a monitored-site
badge, an hourly summary, a staff Live row, and a row in the permanent record.

### Contributing without an agent

`src/api/scanner-ingest.ts` exists for a contributor running their own
rdio-scanner off a desktop scanner, who cannot run the agent or SDR-Trunk. They
point one rdio downstream at `/api/scanner-ingest` and rdio appends
`/api/call-upload` itself. Authentication is a **form field**, not a header,
because rdio's downstream sender cannot set custom headers — which is also why
this cannot reuse `/api/node-ingest/call-upload`.

Their talkgroup and radio ids are the same network, so those are the point. Their
*labels* are not — every display name still resolves from AusAware's own global
config by talkgroup id. A scanner has no control-channel view, so the feed
contributes receptions only and its rows carry a null site. That is a property of
the source, not a gap to fix. Setup: [`scanner-feed-setup.md`](scanner-feed-setup.md).

## Path 3 — the node control plane

Separate from ingest, and bidirectional.

**Enrolment.** The installer writes a single-use code and an empty token. On
first run `internal/enrol` presents the code to `POST /api/node-enrol`
(`src/api/node-enrol.ts`) and gets a node token. That route is authenticated by
the code and nothing else, so its guards are the whole defence: 128 bits of
randomness, single-use, expiring, per-IP rate limited, and failure messages that
say only which of three things went wrong and never anything about a node.

Tokens are `HMAC(FEEDER_TOKEN_SECRET, "<user_id>:<version>")` — the download
endpoint can regenerate one without the database ever storing plaintext.
Rotating that secret invalidates every node's token at once.

**The WebSocket.** `internal/wsclient` holds a persistent outbound connection to
`/api/node-ws/agent`: a `hello` on connect, a status heartbeat every 15s,
answers to `cmd` frames, and a ping every 30s because Cloudflare Tunnel kills an
idle WebSocket at around 100s. Reconnects with backoff. The envelope is
`{ "t": type, "id"?: correlation, "data"?: payload }`
(`internal/protocol` on the agent, `src/services/nodes/protocol.ts` on the
backend).

`src/services/nodes/hub.ts` is pure connection plumbing and deliberately imports
neither config nor the database; the Live row shaper is injected into it from
`src/api/node-ws.ts`.

**Config push.** The backend merges global and per-node config
(`src/services/nodes/configMerge.ts`, `globalConfig.ts`, `configSchema.ts`) and
pushes it down. The radio agent's `internal/configapply` turns one payload into:
stable per-agency rdio API keys (`internal/keys`, UUIDv4 per system id,
persisted so they survive restarts), a full rdio config PUT, and a full
SDR-Trunk config import. SDR-Trunk's config is applied **first** — a flaky rdio
must not prevent the channel list from landing.

**Self-update.** Agents fetch `backends/node/assets/node-versions.json` (served
by `/api/node-updates/manifest`, kind-aware: a pager node is served
`pager-agent` as its `agent` component) at start, every 6h, and on
`cmd{action:'update'}`. Download to `.pending`, sha256-verify, swap, restart. An
empty sha256 means "nothing to do" — not an error.

**A Go agent change does not reach any node until its version is bumped in that
manifest.** `scripts/deploy.sh` skips the Go rebuild entirely when the built
binary already reports the manifest version.

**Live view.** A staff browser's WebSocket gets shaped node rows
(`src/services/nodeLive.ts`). Those read from `src/services/nodeCallWindow.ts`, a
short rolling memory of what each node reported — because nothing else in the
chain remembers: SDR-Trunk rebuilds its active list from live decoder state per
request, the agent rebuilds its slice from that, and the hub stores only the last
frame. Without the window, a reception was on screen if and only if it happened
to be mid-transmission at the instant of a poll.

## Path 4 — ADS-B, which is two different things

1. **Public aggregators.** `src/sources/adsb.ts` fans out across adsb.lol,
   adsb.fi and airplanes.live every 8s — the fastest source in the registry.
   Serves `/api/adsb/aircraft` and prebuilt `/api/adsb/trails`. Kill switch:
   `ADSB_DISABLED`.
2. **AusAware's own aircraft nodes.** The aircraft agent supervises
   `dump1090-fa`, reads its `aircraft.json`/`stats.json`
   (`internal/decoderjson`) and POSTs snapshots to
   `/api/node-ingest/adsb-upload`. The backend keeps per-node statistics,
   coverage folds, range, tracks and daily counters
   (`src/services/nodes/adsb*.ts`).

The agent adapts gain itself (`internal/autogain`) and measures crystal error
(`internal/sdrppm`), so from `dump1090`'s point of view gain is always fixed.
Notably the agent reads JSON files rather than the SBS socket, so the decoder
needs no network stack at all — there is a test asserting no `--net` flag creeps
back in (`internal/decoder/decoder_test.go`).

Antenna position is mandatory for this node kind: without exact lat/lon
`dump1090` reports no range statistics and cannot decode surface positions.

## Path 5 — The Wire

News and media posts, written in `wire-compose.html` and read at `wire.html`.

- `src/api/wire.ts` + `src/api/wireComments.ts` — CRUD, comments, slugs, view
  counting (per-day de-dupe on `hash(ip + ua + VIEW_HASH_SALT)`).
- Photos go **browser → Cloudflare Images** via a one-time direct-upload URL the
  backend mints; the bytes never touch the origin. Only named variants are
  served, which strips EXIF and GPS on delivery (`src/services/wire.ts`).
- Video goes browser → Cloudflare R2 by presigned PUT, ≤50 MB MP4.
  `src/services/videoProcessor.ts` then runs an ffmpeg pass: watermark burn-in,
  bitrate normalisation, EXIF strip, poster frame. The binary comes from
  `ffmpeg-static`; `scripts/deploy.sh` checks it is usable on every deploy
  because without it videos are served exactly as uploaded, silently.
- Deleted posts have a 5-day recovery window, then
  `src/services/wirePurge.ts` removes them.
- `workers/wire-embed/worker.js` is a Cloudflare Worker on
  `nswpsn.forcequit.xyz/wire*`. Real browsers pass through untouched; a social
  crawler asking for a specific post gets the same origin HTML with its `<head>`
  rewritten from `/api/wire/og/...`, so shared links unfurl. It fails open.
- `WIRE_PUBLIC` defaults to `false`, which restricts the read endpoints to the
  owner.

## Data stores

| Store | Owner | Access |
|---|---|---|
| Backend PostgreSQL | AusAware | read/write. 119 numbered migrations in `src/db/migrations`, applied by `src/db/migrate.ts` at boot and by `npm run migrate` on deploy |
| rdio-scanner PostgreSQL | central rdio-scanner | **read-only** via `RDIO_DATABASE_URL` (`src/services/rdio.ts`) |
| Bot PostgreSQL | Discord bot | read via `BOT_DATA_DATABASE_URL` for the dashboard; the bot writes it |
| Supabase | — | **authentication and identity only** |
| `STATE_DIR/*.json` | backend | LiveStore snapshots, atomic writes |
| Cloudflare Images / R2 | — | The Wire's media |

Archive tables are partitioned by month. `src/services/cleanup.ts` creates this
and next month's partitions hourly and drops expired ones — the boot sequence
also runs it once, because the first flush after a month rollover otherwise
fails with "no partition found for row".

Three separate connection pools exist and are closed separately at shutdown:
the main pool, the rdio pool, and the bot-data pool.

### Waze is gone, and the data with it

Waze was retired as a data source. **The ingest, the routes and the data are
gone; the references are not.** Nothing about Waze works, but a contributor who
greps for it will get a lot of hits — so read this section before concluding
the layer is still wired up.

What is genuinely gone:

- No ingest. The `SourceFamily` union in `src/services/sourceRegistry.ts:20`
  does not include `waze` and no source module registers one.
- No routes. There is no `/api/waze/*` handler anywhere in `backends/node/src`.
- No data. Migration `071_drop_police_heatmap.sql` dropped the two derived
  heatmap tables, and migration `074_drop_archive_waze.sql` dropped
  `archive_waze` and `archive_waze_latest` with `CASCADE`. That migration's own
  comment says it: *"Irreversible, and deliberately so — this is the last of the
  Waze data."*

**The leftovers are extensive.** Four of them are live breakage — the frontend
asks for routes that no longer exist:

- `map.html:3827` and `map.html:9489` fetch `/api/waze/police-heatmap`
- `map.html:10864` fetches `/api/waze/police`
- `src/server.ts:453` still advertises `police-heatmap` in the root endpoint
  catalogue

The rest is dead reference rather than breakage, and there is a lot of it —
roughly 88 mentions across 16 non-migration files under `backends/node/src`.
The ones most likely to mislead:

| Where | What survives |
|---|---|
| `services/sourceRegistry.ts:6` | the doc comment above `SourceFamily` still lists `waze` as a family — in the same file whose line 20 proves it is not one |
| `store/filterCache.ts:68-71`, `:147`, `:193` | the four `waze_*` alert types, a `waze: { name: 'Waze', … }` display entry, and a `waze:` group |
| `store/filterCache.ts:343-352` | two **orphaned doc comments**. The first describes `wazeAlertType()` and points at `services/wazeAlerts` and `api/waze-ingest`; the second describes a bbox-snapshot reader and points at `store/wazeIngestCache`. All four of those are gone — and because the comments were left behind, they now sit directly above the unrelated `dimSlotFor()` at `:353`, which reads as its documentation and is not |
| `store/filterCache.ts:7-15` | the module header still describes Waze as a live in-memory path |
| `services/sourceHealth.ts:48` | a `waze` threshold entry |
| `api/status.ts:44`, `:64` | `STATUS_WAZE_STALE_SECS` and `WAZE_BOOT_GRACE_SECS`; live `/api/status` still reports `cleanup.last_waze_ended` |
| `config.ts:86-102` | the userscript ingest keys and the bbox TTL are still parsed |

Migrations under `src/db/migrations/` also mention Waze throughout. That is
correct and must stay — a migration is a record of what happened.

**None of this is evidence the layer exists.** Treat every Waze reference
outside the migrations as residue until someone removes it, and do not extend
any of it.

## Authentication, in layers

1. `optionalSupabaseJwt` runs first (`src/services/auth/supabaseJwt.ts`), so a
   logged-in user is identified before any gate.
2. `requireApiKey` (`src/services/auth/apiKey.ts`) is the global
   `NSWPSN_API_KEY` gate. It short-circuits for OPTIONS preflights, a list of
   public endpoints, anything outside `/api`, and any request already
   authenticated as a Supabase user.
3. Role checks (`src/services/auth/roles.ts`) gate the staff surfaces. Roles
   live in the backend PostgreSQL, not in Supabase.
4. Node agents use `X-Node-Token` + `X-Node-Install`
   (`src/services/auth/nodeToken.ts`); the `/api/node-ingest/` and
   `/api/node-enrol` prefixes are exempt from the key gate for that reason.
5. The Discord dashboard uses Discord OAuth with its own session cookie
   (`src/services/dashboardSession.ts`, `discordOauth.ts`).
6. The whisper watcher uses `WHISPER_ADMIN_TOKEN` — a headless script cannot
   hold a user session.

The request log tags every line with the caller it identified — `browser`,
`discord-bot`, `node`, `rdio`, `whisper`, `whisper-node`, `other` — so a busy
tail separates into streams. Path wins over header for `rdio` and `whisper`,
because nothing else posts to those routes and the callers that do cannot carry
anything else (`src/server.ts`).

## Operations

- One pm2 process, `api-node`, fronted by Cloudflare Tunnel on
  `api.forcequit.xyz`. The site is served by Apache straight out of the repo
  root at `nswpsn.forcequit.xyz`.
- Deploy is `backends/node/scripts/deploy.sh`, run **by the owner**: discard
  lockfile drift, `git pull --ff-only`, re-exec if the script itself changed,
  `npm install`, reinstall the matching Chromium, build, cross-compile the three
  Go agents into `downloads/` with sha256 sidecars when their manifest version
  changed, `npm run migrate`, then
  `pm2 restart api-node --kill-timeout 30000`.
- The 30-second kill timeout is deliberate and lives in the deploy script, not
  in an ecosystem file: a transcription legitimately runs for seconds, and a
  SIGKILL through one loses that reception's transcript for good.
- `backends/ecosystem.config.js` is **stale and unused**. Nothing on the box
  runs from it. Never cite it for a port, an environment variable, or a timeout.
- `/api/status` answers in Uptime Kuma's shape; recipes are in
  [`uptime-kuma-monitors.md`](uptime-kuma-monitors.md). It returns 200 unless
  the database or the archive writer is broken.

## Component docs

| Component | Doc | Entry point |
|---|---|---|
| Backend | [`components/backend.md`](components/backend.md) | `backends/node/src/index.ts` |
| Public site | [`components/public-site.md`](components/public-site.md) | repo-root `*.html` |
| Discord bot | [`components/discord-bot.md`](components/discord-bot.md) | `discord-bot/bot.py` |
| Radio node | [`components/radio-node.md`](components/radio-node.md) | `feeder-nodes/radio-node/cmd/nodeagent/main.go` |
| Pager node | [`components/pager-node.md`](components/pager-node.md) | `feeder-nodes/pager-node/cmd/nodeagent/main.go` |
| Aircraft node | [`components/aircraft-node.md`](components/aircraft-node.md) | `feeder-nodes/aircraft-node/cmd/nodeagent/main.go` |
| Forked runtimes | [`components/forked-runtimes.md`](components/forked-runtimes.md) | — |
| Transcription | [`components/transcription.md`](components/transcription.md) | `backends/node/src/services/whisperRouter.ts` |
