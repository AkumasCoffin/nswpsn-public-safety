# The public data API — `/api/v1/`

**Base URL: `https://api.forcequit.xyz`. Reference: signed in at `https://nswpsn.forcequit.xyz/api-docs`.**

The data AusAware shows on its map — fire and emergency incidents, warnings,
hazards — available to scripts and servers with an API key. It is the same data,
from the same handlers, that the site's own pages read.

AusAware is an independent project. It relays what agencies publish in their
own public feeds; it is not affiliated with, endorsed by or acting for any of
them, and the data stays theirs. Credit NT Fire and Rescue Service wherever you
show its records — its feed asks not to be re-published, and the reference
repeats this on the fires endpoint.

This document is for people using the API and for anyone changing it. How the
backend is put together is in [`components/backend.md`](components/backend.md);
read [`../OPERATING-RULES.md`](../OPERATING-RULES.md) before changing anything.

---

## Getting a key

1. Sign in at nswpsn.forcequit.xyz and open your profile → **API keys**.
   Request a key: a name, what it is for, and optionally which feeds you want,
   live, history or both.
2. Staff review the request and choose what the key may read — its **feeds**
   (below) — its per-minute limit (120 unless set otherwise) and, optionally,
   an expiry.
3. Once approved, press **Generate key** on the same card. The key
   (`ak_<prefix>_<secret>`) is shown **once**; only its hash is stored.

From the same card you can re-issue a key (new secret, same settings and
history) or delete it, and open the API reference. A person holds at most three
active keys.

Send the key on every request, either way:

```bash
curl -H "X-API-Key: ak_…" https://api.forcequit.xyz/api/v1/fires
curl -H "Authorization: Bearer ak_…" https://api.forcequit.xyz/api/v1/fires
```

**Keys are for scripts and servers, not web pages.** A request that carries a
key and a browser's `Origin` or `Sec-Fetch-Site` header is refused
(`403 api_key_browser_use`) — whichever site it comes from,
nswpsn.forcequit.xyz included. A key in a web page
would be a key anyone could copy. Keys only read: GET (and HEAD), nothing else.

## Feeds and scopes

A key is granted **feeds**, each **live**, **history** or both — on the staff
page and the profile that is a table, one row per feed. *Live* is what the map
shows now, polled from each source and held in memory; *history* is the records
this server stored over time. Each ticked cell is one scope, written
`<feed>:<window>`:

| Feed | Scope(s) | Covers |
|---|---|---|
| Fires | `fires:live` · `fires:history` | Every fire service in one feed — NSW RFS, QLD Fire Department, VIC Emergency, WA DFES, SA CFS and MFS, NT Fire and Rescue — plus satellite hotspots and fire danger ratings |
| Warnings | `warnings:live` · `warnings:history` | Bureau of Meteorology warnings and the QLD and WA emergency warnings |
| ACT Ambulance | `ambulance:live` · `ambulance:history` | ACT Ambulance Service incidents |
| Power outages | `outages:live` · `outages:history` | Endeavour Energy, Ausgrid and Essential Energy |
| Road hazards | `roads:live` · `roads:history` | Live Traffic NSW incidents, roadworks, floods, fires, major events, alpine conditions, council works |
| Cameras | `cameras:live` | Traffic, QLD traffic and flood, airport and fire-tower cameras |
| Public transport | `transport:live` | Vehicles, stops, lines, trips, departures, alerts |
| Aircraft | `aircraft:live` · `aircraft:history` | ADS-B positions, trails, photos; history replays past positions |
| Ships | `ships:live` | AIS vessel positions and details |
| Weather | `weather:live` | Current conditions, radar, the forecast grid, point forecasts |
| Beaches & sharks | `beaches:live` · `beaches:history` | Beachwatch, BeachSafe, SharkSmart reports |
| News | `news:live` | Emergency and weather news from public RSS feeds |
| Pager messages | `pager:live` · `pager:history` | Pager hits from the Pagermon relay |
| Radio summaries | `summaries:history` | The hourly and ad-hoc radio summaries |

Every endpoint declares the one cell it serves, and a key reads it when it holds
that cell — nothing broader: there are no wildcards, and a feed added later is
granted to nobody until staff tick it. The reference lists each endpoint's cell
as `x-scopes`. A refusal names what was missing:

```json
{ "error": { "code": "api_key_scope_forbidden",
             "message": "This key's scopes do not cover this endpoint. It needs warnings:live." } }
```

No feed reaches anything about people or the receiving nodes — accounts and
profiles, user-submitted incidents, site statistics, node status — nor this
site's own reference data: boundaries, the radio site list and reception
transcripts are not part of the API.

**`read`** is the rule every key made before scopes still holds: GET on the
fixed list of paths keys could always read, on `/api/` and on `/api/v1/` alike.
That list is by path prefix and predates scopes, so it is broader in places: it
includes the per-agency fire feeds at their old paths, `/api/boundaries/` and
everything under `/api/radio/` — and it reaches `/api/v1/fires`, the fire feeds
it already read, merged. A scope that is neither `read` nor a cell of the table
grants nothing. New keys are granted feeds; a request cannot ask for `read`.

## What is on `/api/v1/`

| Tag | Endpoint | Scope |
|---|---|---|
| Fires | `GET /api/v1/fires` — every fire service, one feed ([below](#fires)) | `fires:live` |
| Fires | `GET /api/v1/firms/hotspots` — satellite hotspots | `fires:live` |
| Fires | `GET /api/v1/rfs/fdr` — fire danger ratings and total fire bans by district | `fires:live` |
| Warnings | `GET /api/v1/bom/warnings` · `/qld-fire/warnings` · `/wa-emergency/warnings` | `warnings:live` |
| Ambulance | `GET /api/v1/act-ambulance/incidents` | `ambulance:live` |
| Power outages | `GET /api/v1/endeavour/{current,maintenance,future,planned}` · `/endeavour/current/all` · `/endeavour/future/all` · `/endeavour/postcodes` | `outages:live` |
| Power outages | `GET /api/v1/essential/{outages,planned,future}` · `/essential/outages/current` · `/essential/outages/raw` | `outages:live` |
| Power outages | `GET /api/v1/ausgrid/outages` · `/ausgrid/stats` | `outages:live` |
| Road hazards | `GET /api/v1/traffic/{incidents,roadwork,flood,fire,majorevent,alpine,lga}` and each one's `/raw` · `/traffic/works` · `/traffic/lga-incidents` · `/traffic/all-feeds` | `roads:live` |
| Cameras | `GET /api/v1/traffic/cameras` · `/aviation/cameras` · `/aviation/cameras/{airport}` · `/qld/cameras` · `/qld/flood-cameras` · `/centralwatch/sites` | `cameras:live` |
| Cameras | `GET /api/v1/centralwatch/cameras` · `/centralwatch/image/{cameraId}` | none — public |
| Public transport | `GET /api/v1/transport/{vehicles,lines,stops,alerts}` · `/transport/shape/{id}` · `/transport/trip/{date}/{tripId}/{instance}` · `/transport/departures/{stopId}` | `transport:live` |
| Aircraft | `GET /api/v1/adsb/aircraft` · `/adsb/trails` · `/adsb/photo/{hex}` | `aircraft:live` |
| Aircraft | `GET /api/v1/adsb/history` | `aircraft:history` |
| Ships | `GET /api/v1/marinetraffic/vessels` · `/marinetraffic/vessel/{id}` | `ships:live` |
| Weather | `GET /api/v1/weather/{current,radar,point}` · `/weather/grid/manifest` · `/weather/grid` | `weather:live` |
| Beaches & sharks | `GET /api/v1/beachwatch` · `/beachsafe` · `/beachsafe/beach/{slug}` · `/dorsal/reports` | `beaches:live` |
| News | `GET /api/v1/news/rss` · `/news/sources` | `news:live` |
| Pager | `GET /api/v1/pager/hits` | `pager:live` |
| Radio summaries | `GET /api/v1/summaries` · `/summaries/latest` | `summaries:history` |
| History | `GET /api/v1/data/history` · `/data/history/incident/{source}/{source_id}` | the history of at least one stored feed (`*:history`) — see [Stored history](#stored-history) |
| History | `GET /api/v1/data/history/filters` | none — public |
| Reference | `GET /api/v1/agency/extended` · `/agency/extended/{slug}` | none — public |
| Service | `GET /api/v1/health` · `/api/v1/status` | none — public |
| — | `GET /api/v1/openapi.json` | any valid key |

Most of these are still served at their old `/api/…` path as well, the same
handler with the same scope, until that path is retired. Not on `/api/v1/`, and
not readable by a key in the feed form: the per-agency fire incident feeds
(`/api/rfs/incidents`, `/api/qld-fire/incidents`, `/api/vic-emergency/events`,
`/api/nt-fire/incidents`, `/api/wa-emergency/incidents`, `/api/sa-fire/cfs` and
`/mfs`), which this site's map and the Discord bot read — keys read them merged,
at `/api/v1/fires` — and boundaries (`/api/boundaries/…`, the council names
included), the radio site list and reception transcripts.

A few old paths have no `/api/v1/` twin because another endpoint already
answers the same thing: `/api/endeavour/current/raw` and `/future/raw` (use
`/endeavour/current/all` and `/future/all`), `/api/essential/outages/future`
(use `/essential/future`) and `/api/beachsafe/details`, which is always empty
(use `/beachsafe/beach/{slug}`). They go when the old paths are retired.

The fire-tower cameras' `imageUrl` is a path on this API, to be joined to its
origin: `/api/v1/centralwatch/image/{id}` from `/api/v1/`, and the old
`/api/centralwatch/image/{id}` from the old path.

### Fires

`GET /api/v1/fires` is every fire service's incidents in one GeoJSON
FeatureCollection — NSW RFS, the QLD Fire Department, VIC Emergency, WA DFES,
SA CFS and MFS and NT Fire and Rescue — each record in the same shape:

| Property | |
|---|---|
| `id` | Stable across polls, built from the agency's own id |
| `source` | `rfs`, `qld_fire`, `vic_emergency`, `nt_fire`, `wa_incident`, `sa_cfs` or `sa_mfs` — the same names stored history uses |
| `agency` | The publishing service: `RFS`, `QFD`, `CFA`, `SES`, `DFES`, `SACFS`, `SAMFS`, `NTFRS`, … |
| `state` | `NSW`, `QLD`, `VIC`, `NT`, `WA` or `SA` |
| `layer` | `fire`, or `flood` / `hazard` for the other incidents these services publish (and `hazard` for any kind of record not known yet) |
| `title`, `status`, `location` | As the agency words them |
| `category` | Kind of incident as the agency classes it — "Grass Fire", "Building Fire", … |
| `alert_level` | Advice, Watch and Act, Emergency Warning — or empty / "Not Applicable" |
| `responsible_agency` | As the agency gives it |
| `updated_at` | Last update, ISO 8601 UTC; `null` when the agency gives no time with a zone — then `details.updated` has its own wording |
| `is_active` | `false` once closed (only with `all=1`) |
| `polygons` | Fire grounds or warning areas, outer rings of `[longitude, latitude]`; empty when none |
| `details` | The agency's own extra fields under their own names (RFS `size`, `councilArea`; QFD `vehiclesOnScene`; VIC `capEvent`, `text`; WA `region`; SA `district`; …) |

The services publish more than fires — VIC Emergency carries floods and other
hazards, the QLD and NT services rescues and hazmat — and those come too,
tagged by `layer`. Filters, each a comma list: `source=`, `state=` (any case)
and `layer=` (`layer=fire` for fires only); an unknown value is a
`400 validation_failed` naming it. Closed incidents are left out unless
`all=1`. The collection also carries `sources`, one entry per service asked
for, with how many records it gave (after the filters) and when this server
last read it (`updated_at`, `null` if it has not yet) — so a service that is
down shows as missing, not as having no fires. Records from NT Fire and Rescue
are theirs: credit NTFRS if you republish them.

### Stored history

`/data/history` serves what this server stored from each source over time —
by default one record per incident, its latest, newest first — and
`/data/history/incident/{source}/{source_id}` every stored snapshot of one
incident. Records carry the archived source they came from (`source`), and a
key reads an archived source when it holds that feed's history:

| Feed | Archived `source` |
|---|---|
| `fires:history` | `rfs`, `qld_fire`, `vic_emergency`, `nt_fire`, `wa_incident`, `sa_cfs`, `sa_mfs` |
| `warnings:history` | `bom_land`, `bom_marine`, `qld_warning`, `wa_warning` |
| `ambulance:history` | `act_ambulance` |
| `roads:history` | `traffic_incident`, `traffic_roadwork`, `traffic_flood`, `traffic_fire`, `traffic_majorevent`, `traffic_alpine`, `traffic_lga`, `traffic_works` |
| `outages:history` | `endeavour_current`, `endeavour_planned`, `endeavour_maintenance`, `ausgrid`, `essential_current`, `essential_planned`, `essential_future` |
| `beaches:history` | `dorsal` |
| `pager:history` | `pager` |

So `fires:history` reads the stored history of every fire service, but not
`qld_warning`. Without `?source=`, a key's answer holds only the sources it
covers; naming one it does not is a `403 api_key_scope_forbidden` that says
which. People's own reports (`user_incident`) are never readable through a key.
Because the answer depends on the key, it comes with
`Cache-Control: private, no-store`. In the reference these endpoints list
`*:history` as their `x-scopes`: the history of any feed in the table above
gets in (`aircraft:history` and `summaries:history` have endpoints of their
own, so they do not), and the answer is narrowed to the feeds the key holds.

Page with `cursor` (the previous page's `next_cursor`); `offset` works too, up
to 10,000. `total` is the number of matching records, or 0 when they could not
be counted in time, and is not computed for a page fetched by cursor. A page
that some part of the archive did not answer in time is not served: it is a
`503 service_unavailable` naming the tables in `details.failed_tables`, so a
short page never reads as the whole answer. `/data/history/filters` (public)
lists the sources, categories, statuses and states there are to filter by.

## Versions

`/api/v1/` is the public data API. Within v1 a change only adds: new endpoints,
new fields, new optional parameters. Removing or renaming something, or
changing what a field means, is a new version.

Each v1 endpoint is also still served at its original `/api/…` path, by the
same handler. The old path answers exactly as it always did — same body, same
errors — and is there for the clients that use it now. Once every client of a
feed has moved to `/api/v1/`, its old path is retired. Staff can see, per key,
how many requests still use unversioned paths (Moderation → API keys,
**Unversioned**), which is how they tell who to warn first.

Not part of the versioned API, and staying where they are for good:

- **Site-internal routes** — accounts, profiles, staff tools, tickets, the
  Wire, the editor, the dashboard, key management. They serve this site's
  pages, not third parties, and are not a public contract.
- **Machine endpoints with callers that cannot move**: the feeder-node routes
  (`/api/node-ingest/`, `/api/node-enrol`, `/api/node-updates/`, the node
  WebSocket) — an agent that receives a 404 deletes the data it was sending;
  the scanner feed (`/api/scanner-ingest/`), the transcription service
  (`/api/whisper/`), the Discord dashboard's OAuth callback.
- **`/api/health` and `/api/status`** — monitors point at them. Both are on
  `/api/v1/` as well.

## Requests and responses

**JSON, wrapped.** Every v1 answer that is not GeoJSON is an object with the
payload under `data`:

```json
{ "data": { "warnings": [ … ], "count": 3, "counts": { "land": 3, "marine": 0 } } }
```

**GeoJSON is not wrapped.** A FeatureCollection is answered as itself, so
GeoJSON tools can read the response directly. Coordinates are
`[longitude, latitude]` (WGS84), as GeoJSON has them.

**Field names.** Names the API itself defines — `data`, `error`, query
parameters, and every field of an endpoint made for this API, such as
`/api/v1/fires` — are `snake_case`. A feed carried over from the old paths
keeps the field names it always had, which for some are camelCase
(`alertLevel`, `updatedISO`): the map, the bot and existing clients read those
names, and v1 does not rename them.

**Times** take the form each feed gives them, and the reference says which:
an ISO 8601 timestamp (with its offset), unix seconds, or text for display
(`updated`, `issued`). Where a record has both, the ISO field (`updatedISO`,
`acq_datetime`) is the one to compute with. Times this API writes itself —
stored history's `fetched_at_iso` and `last_seen_iso`, the filters' date range —
are ISO 8601 in UTC (`…Z`) on `/api/v1/`. (The old paths write those as Sydney
wall-clock time with no offset, and still do.)

**Query parameters** are listed per endpoint; a parameter given twice is read by
its first value. An optional switch such as `all=1` reads anything else as
"off"; a required parameter that is missing or wrong is a
`400 validation_failed`, with the failing fields in `details.issues`.

**How fresh.** Most feeds are served from memory, refreshed from their source on
its own cadence — every fifteen seconds or so for aircraft, every one to fifteen
minutes for the rest — so asking more often than that gains nothing. The fire
danger ratings change a few times a day and are read from the RFS at most every
ten minutes. Some endpoints read their source when asked instead — the traffic
`/raw` feeds, a single airport's cameras, a vessel's details, and the transport
endpoints, which keep each answer for a few seconds; the reference says so on
each. Some responses carry their
own `Cache-Control`.

**When a source can't be read.** A feed served from memory keeps answering with
its last good data. An endpoint that reads its source when asked answers an
error on `/api/v1/` — `503 service_unavailable`, or `502` where the source
answered but gave nothing usable — so an outage never reads as "nothing to
report". (The old `/api/…` paths answered an empty list in some of those cases,
and still do.)

**Rate limit.** Per key, per minute — 120 unless staff set otherwise. Over it:
`429 rate_limited`. Requests refused for scope or path count against the limit
too.

## Errors

```json
{ "error": { "code": "rate_limited", "message": "This key is limited to 120 requests per minute" } }
```

`code` is stable — branch on it. `message` is for people and may change.
`details` appears for some codes.

| Status | `code` | When |
|---|---|---|
| 400 | `validation_failed` | A query parameter is missing or not valid |
| 401 | `unauthorized` | No key |
| 401 | `token_origin`, `token_expired`, `token_invalid` | A browser session token (the site's own pages) that is not valid here |
| 403 | `invalid_api_key` | The key is wrong, revoked or expired |
| 403 | `api_key_browser_use` | A key sent from a web page |
| 403 | `api_key_scope_forbidden` | The key's scopes do not cover the endpoint |
| 403 | `api_key_path_forbidden` | A `read` key on a path outside its list |
| 403 | `forbidden` | Not allowed for another reason |
| 404 | `not_found` | No endpoint at that path, or nothing by that id |
| 429 | `rate_limited` | Over the key's per-minute limit |
| 502 | `service_unavailable` | The source the endpoint reads answered, but with an error or nothing usable |
| 503 | `auth_unavailable` | The key could not be checked just now |
| 503 | `service_unavailable` | Something the endpoint depends on is down |
| 500 | `internal_error` | A fault on this side |

The unversioned `/api/…` paths keep the error bodies they always had.

## The reference: `openapi.json` and `/api-docs`

`GET /api/v1/openapi.json` is the OpenAPI 3.1 description of `/api/v1/`, and
every valid key may fetch it — a `read` key too. It is **filtered to the key**:
the public endpoints plus the ones its scopes reach. It is not wrapped in
`data`, so tools can read it as it is, and it is never cached.

The readable reference is a page on the site, linked to your account:
`https://nswpsn.forcequit.xyz/api-docs`, and the **API docs** button on your
profile's API keys card. Sign in and it shows the description as far as your
**active** keys reach — every endpoint at least one of them can read, plus the
public ones; a revoked or expired key adds nothing. Owner and staff see the
whole document. With no active key the page says so and points to the API keys
card; signed out, it asks you to sign in.

The page never handles a key. It reads the description signed in, from
`GET /api/api-keys/openapi.json` on the API (`api/apiKeys.ts`) — the Supabase
session, not a key; `403 no_active_key` when you hold none; never cached — the
same committed document `/api/v1/openapi.json` serves, filtered once per key
and joined (`filterSpecForKeys` in `api/v1/openapi.ts`). "Try it" is off:
a key sent from a web page is refused (`api_key_browser_use`), so it could never
work there. Each endpoint keeps its example request to copy into a terminal.

The viewer is [Scalar](https://github.com/scalar/scalar) (MIT), vendored at
`vendor/scalar/` in the site and loaded from there — nothing comes from a CDN.
It is set to send nothing anywhere (no telemetry, request proxy, web fonts, AI
agent or MCP), and loads only once there is a reference to show. That
directory's README says how to update it.

## Changing the API

The description is generated from the routes, so the code is the documentation.
To put an endpoint on `/api/v1/`, or change one that is there:

1. **Scope.** The route declares the feed and window it serves, as its first
   middleware: `scoped({ feed, window })` (`services/auth/apiKeyScopes.ts`).
   Without one, no scoped key can read it. A new feed is a new row in `FEEDS`
   there (label, what it covers, its windows) — staff then see it in the table,
   and nobody holds it until they tick it.
2. **Description.** `describeRoute({ operationId, tags: [the feed's tag], summary,
   description, responses })` from `hono-openapi`. Responses come from
   `api/v1/schemas.ts`: `okJson(schema, …)` plus `GATED_ERRORS` for a route that
   needs a key, `VALIDATION_ERROR` where input can be refused.
3. **Response schema.** A zod schema of what the handler returns, named with
   `.meta({ id })`, and checked against the handler at compile time:
   `assertShape<SameShape<z.infer<typeof Schema>, TheType>>()` where the handler
   returns a named type, `body satisfies z.infer<typeof Schema>` where it builds
   the body inline. If the two drift, `tsc` fails.
4. **Input.** `queryValidator(z.object({ … }), legacyBody?)`. The same chain
   serves the old path, so the schema must keep any tolerance the old parsing
   had (`.catch(default)`), and a refusal passes the old 400 body for the old
   path. It reads each parameter's first value, as `c.req.query(name)` does.
5. **Answer** with `ok(c, data)`, `geo(c, featureCollection)` or
   `fail(c, status, code, message, legacyBody?)` (`api/v1/respond.ts`) — never
   `c.json` directly — so each path gets its own shape.
6. **Mirror** it: `mirror(app, router, ['/api/…'])` in `src/server.ts`, after the
   router's `app.route`. Only the paths listed are versioned.
7. **Regenerate**: `npm run openapi` in `backends/node`, and commit
   `openapi.json` with the change. `test/unit/api/openapi.test.ts` fails while
   it is stale, and checks every v1 operation is described and declares its
   credential.
8. **Deploy before anything uses it.** The site goes live on push; the backend
   only when the owner deploys. A page, the bot or a document switches to a v1
   path in a later commit, after the deploy.
