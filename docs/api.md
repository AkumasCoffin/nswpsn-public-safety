# The public data API — `/api/v1/`

**Base URL: `https://api.forcequit.xyz`. Interactive reference: `https://api.forcequit.xyz/api/docs`.**

The data AusAware shows on its map — fire and emergency incidents, warnings,
hazards — available to scripts and servers with an API key. It is the same data,
from the same handlers, that the site's own pages read.

AusAware is an independent project. It relays what agencies publish in their
own public feeds; it is not affiliated with, endorsed by or acting for any of
them, and the data stays theirs. Credit NT Fire and Rescue Service wherever you
show its records — its feed asks not to be re-published, and the reference
repeats this on that endpoint.

This document is for people using the API and for anyone changing it. How the
backend is put together is in [`components/backend.md`](components/backend.md);
read [`../OPERATING-RULES.md`](../OPERATING-RULES.md) before changing anything.

---

## Getting a key

1. Sign in at nswpsn.forcequit.xyz and open your profile → **API keys**.
   Request a key: a name, what it is for, and optionally which feeds and whether
   you want live data, history or both.
2. Staff review the request and choose what the key may read — its **scopes**
   (below) — its per-minute limit (120 unless set otherwise) and, optionally,
   an expiry.
3. Once approved, press **Generate key** on the same card. The key
   (`ak_<prefix>_<secret>`) is shown **once**; only its hash is stored.

From the same card you can re-issue a key (new secret, same settings and
history) or delete it. A person holds at most three active keys.

Send the key on every request, either way:

```bash
curl -H "X-API-Key: ak_…" https://api.forcequit.xyz/api/v1/rfs/incidents
curl -H "Authorization: Bearer ak_…" https://api.forcequit.xyz/api/v1/rfs/incidents
```

**Keys are for scripts and servers, not web pages.** A request that carries a
key and a browser `Origin` header is refused (`403 api_key_browser_use`) —
whichever site it comes from, nswpsn.forcequit.xyz included. A key in a web page
would be a key anyone could copy. The one exception is the API's own reference
page (below), on the API's own origin, where you paste your key yourself. Keys
only read: GET (and HEAD), nothing else.

## Scopes

A scope is one of three dimensions:

| Scope | Means |
|---|---|
| `source:<key>` | whose data: `rfs`, `firms`, `vic`, `qfd`, `dfes`, `safire`, `ntfrs`, `actas`, `bom`, `livetraffic`, `transport`, `adsb`, `aviation`, `marinetraffic`, `weather`, `beach`, `dorsal`, `endeavour`, `ausgrid`, `essential`, `centralwatch`, `qldtraffic`, `news`, `boundaries`, `pager`, `radio`, `rdio` |
| `type:<family>` | what kind: `incidents`, `warnings`, `outages`, `hazards`, `cameras`, `weather`, `transit`, `aircraft`, `vessels`, `pager`, `radio`, `summaries`, `transcripts`, `news`, `boundaries` |
| `window:live` | what the map shows now — polled from each source and held in memory |
| `window:history` | stored records over time |

Any dimension may be `*`. Every endpoint declares the source, type and window
it serves; a key reads it when it holds a matching scope **in all three**. So
`source:rfs type:* window:live` reads every live RFS endpoint, and
`source:* type:warnings window:live` reads every live warning feed. Reference
data (boundaries, the radio site list) fits either window. The reference lists
each endpoint's needs as `x-scopes`, and shows a key the public endpoints and the
ones its scopes reach. A refusal names what was missing:

```json
{ "error": { "code": "api_key_scope_forbidden",
             "message": "This key's scopes do not cover this endpoint. It needs source:bom, type:warnings, window:live." } }
```

No scope reaches anything about people or the receiving nodes — accounts and
profiles, user-submitted incidents, site statistics, node status — however wide
it is.

**`read`** is the rule every key made before scopes still holds: GET on the
fixed list of paths keys could always read, on `/api/` and on `/api/v1/` alike.
That list is by path prefix and predates scopes, so it is broader in one place:
it includes everything under `/api/radio/`, the receiving sites' list among it.
Staff may still grant `read`; a request cannot ask for it.

## What is on `/api/v1/`

| Tag | Endpoint | Scopes it needs |
|---|---|---|
| Incidents | `GET /api/v1/rfs/incidents` · `/rfs/incidents/raw` | `source:rfs type:incidents window:live` |
| Warnings | `GET /api/v1/rfs/fdr` — fire danger ratings and total fire bans by district | `source:rfs type:warnings window:live` |
| Incidents | `GET /api/v1/act-ambulance/incidents` | `source:actas type:incidents window:live` |
| Incidents · Warnings | `GET /api/v1/qld-fire/incidents` · `/qld-fire/warnings` | `source:qfd`, `type:incidents` or `type:warnings`, `window:live` |
| Incidents | `GET /api/v1/nt-fire/incidents` | `source:ntfrs type:incidents window:live` |
| Incidents | `GET /api/v1/sa-fire/cfs` · `/sa-fire/mfs` | `source:safire type:incidents window:live` |
| Incidents | `GET /api/v1/vic-emergency/events` | `source:vic type:incidents window:live` |
| Incidents · Warnings | `GET /api/v1/wa-emergency/incidents` · `/wa-emergency/warnings` | `source:dfes`, `type:incidents` or `type:warnings`, `window:live` |
| Hazards | `GET /api/v1/firms/hotspots` | `source:firms type:hazards window:live` |
| Warnings | `GET /api/v1/bom/warnings` | `source:bom type:warnings window:live` |
| Reference | `GET /api/v1/agency/extended` · `/agency/extended/{slug}` · `/boundaries/lga-names` | none — public |
| Service | `GET /api/v1/health` · `/api/v1/status` | none — public |
| — | `GET /api/v1/openapi.json` | any valid key |

The other feeds a key can read — roads, transport, aircraft, ships, weather,
beaches, shark reports, power, cameras, news, boundaries, pager traffic, the
radio site list, the radio summaries and reception transcripts — are still on
their unversioned `/api/…` paths, and keys read them there with the same scopes.
They move to `/api/v1/` group by group.

`/rfs/incidents/raw` on v1 is the RFS's own `majorIncidents.json`, exactly as
published at the last poll; the old `/api/rfs/incidents/raw` keeps its RSS-based
shape until it is retired.

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
parameters — are `snake_case`. A feed's records keep the field names they have
always had, which for several fire feeds are camelCase (`alertLevel`,
`updatedISO`): the map, the bot and existing clients read those names, and v1
does not rename them.

**Times** take the form each feed gives them, and the reference says which:
an ISO 8601 timestamp (with its offset), unix seconds, or text for display
(`updated`, `issued`). Where a record has both, the ISO field (`updatedISO`,
`acq_datetime`) is the one to compute with.

**Query parameters** are listed per endpoint; a parameter given twice is read by
its first value. An optional switch such as `all=1` reads anything else as
"off"; a required parameter that is missing or wrong is a
`400 validation_failed`, with the failing fields in `details.issues`.

**How fresh.** The live feeds are served from memory, refreshed from each source
every one to fifteen minutes depending on the source, so asking more than once
a minute gains nothing. The fire danger ratings change a few times a day and are
read from the RFS at most every ten minutes. Some responses carry their own
`Cache-Control` (30 seconds for several feeds, a minute for the agency tables).

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
| 503 | `auth_unavailable` | The key could not be checked just now |
| 503 | `service_unavailable` | Something the endpoint depends on is down |
| 500 | `internal_error` | A fault on this side |

The unversioned `/api/…` paths keep the error bodies they always had.

## The reference: `openapi.json` and `/api/docs`

`GET /api/v1/openapi.json` is the OpenAPI 3.1 description of `/api/v1/`, and
every valid key may fetch it — a `read` key too. It is **filtered to the key**:
the public endpoints plus the ones its scopes reach. It is not wrapped in
`data`, so tools can read it as it is, and it is never cached.

`https://api.forcequit.xyz/api/docs` renders it. The page asks for your key,
keeps it in that browser tab only (`sessionStorage`), fetches the description
with it and shows it with "Try it" on each endpoint. The page and its viewer are
served by this API; its Content-Security-Policy lets it reach this API and
nothing else, so the key is sent nowhere but here. The viewer is
[Scalar](https://github.com/scalar/scalar) (MIT), vendored at
`backends/node/assets/scalar/` — that directory's README says how to update it.

## Changing the API

The description is generated from the routes, so the code is the documentation.
To put an endpoint on `/api/v1/`, or change one that is there:

1. **Scope.** The route declares what it serves, as its first middleware:
   `scoped({ source, type, window })` (`services/auth/apiKeyScopes.ts`). Without
   one, no scoped key can read it.
2. **Description.** `describeRoute({ operationId, tags: [one tag], summary,
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
