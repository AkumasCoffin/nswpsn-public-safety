# Uptime Kuma monitor recipes

All monitors below hit the same endpoint:

```
GET https://api.forcequit.xyz/api/status
```

Use Uptime Kuma's **HTTP(s) - JSON Query** monitor type. Paste the
`Expression` into the JSON Query Expression field, and the literal
`Expected` value (no quotes) into Expected Value. JSONata is the query
language — see https://jsonata.org for syntax docs.

The endpoint always returns 200 unless backend infrastructure (DB or
archive writer) is broken, in which case it returns 503. The JSON body
carries the finer ok/degraded/down detail used by these expressions.

## Overall

| Monitor | Expression | Expected |
|---|---|---|
| Backend ok | `status` | `ok` |
| Backend not down | `status != "down"` | `true` |
| Uptime > 60s | `uptime_secs > 60` | `true` |

## Backend internals

The check keys written by `backends/node/src/api/status.ts` are `database`,
`archive_writer`, `archive_buffer`, `filter_cache`, `ingest`, `cleanup`,
`ram_cache`, `rdio_scheduler` and `sources`.

| Monitor | Expression | Expected |
|---|---|---|
| Database | `checks.database.ok` | `true` |
| Archive writer | `checks.archive_writer.ok` | `true` |
| Archive buffer | `checks.archive_buffer.ok` | `true` |
| Filter cache | `checks.filter_cache.ok` | `true` |
| rdio summary scheduler | `checks.rdio_scheduler.ok` | `true` |

`cleanup` and `ram_cache` are present for response-shape parity with the
pre-rewrite backend, which had those subsystems. They report informational
fields and `ok: true` so a dashboard panel renders rather than showing a dash,
and they never trip the overall status — so there is nothing useful to monitor
on them.

## Sources

The `sources` block is built from the **source registry**
(`backends/node/src/services/sourceRegistry.ts`), so each key is a registry
source name. A source is `ok` when LiveStore holds a snapshot for it and
`unknown` when it does not. Each entry also carries
`soft_threshold_secs` (its poll cadence) and `hard_threshold_secs` (twice that).

| Monitor | Expression | Expected |
|---|---|---|
| RFS | `checks.sources.rfs_incidents.ok` | `true` |
| BOM | `checks.sources.bom_warnings.ok` | `true` |
| Pager | `checks.sources.pager.ok` | `true` |
| LiveTraffic incidents | `checks.sources.traffic_incidents.ok` | `true` |
| LiveTraffic roadwork | `checks.sources.traffic_roadwork.ok` | `true` |
| LiveTraffic flood | `checks.sources.traffic_flood.ok` | `true` |
| LiveTraffic fire | `checks.sources.traffic_fire.ok` | `true` |
| LiveTraffic majors | `checks.sources.traffic_majorevent.ok` | `true` |
| LiveTraffic council roads | `checks.sources.traffic_lga.ok` | `true` |
| Endeavour | `checks.sources.endeavour_current.ok` | `true` |
| Ausgrid | `checks.sources.ausgrid.ok` | `true` |
| Essential | `checks.sources.essential_current.ok` | `true` |
| NASA FIRMS | `checks.sources.firms_hotspots.ok` | `true` |
| ACT Ambulance | `checks.sources.act_ambulance.ok` | `true` |
| Aircraft (aggregators) | `checks.sources.adsb_aircraft.ok` | `true` |

Any other registry source works the same way — use its registry name. A rollup is
available too: `summary.sources_total`, `summary.sources_ok` and
`summary.sources_unknown`.

## Notes

- **Group these in Uptime Kuma**: Settings → Add Monitor → Type: Group →
  drag related monitors in. Two natural groups: "Backend Internals" and
  "Data Sources". Keeps the dashboard scannable.

- **rdio-scanner stays unknown for ~65 min after a backend restart**
  because the rdio summary scheduler runs hourly. It'll flip green on
  the first successful summary cycle.

- **Source-level outages don't 503** the endpoint. They flip overall
  `status` to `degraded`, which trips the "Backend healthy" monitor and
  the relevant per-source monitor, but a basic HTTP-status monitor
  pointed at `/api/status` would still see 200. Use these JSONata
  monitors for source-level alerting.

- **Thresholds are constants in the backend**, not environment variables —
  changing one is a code change and a deploy, not a restart.
  `STATUS_DB_TIMEOUT_SECS`, `STATUS_WRITER_STALE_SECS`,
  `STATUS_BUFFER_WARN_RECORDS` and `STATUS_FILTER_CACHE_STALE_SECS` are at
  the top of `backends/node/src/api/status.ts`. Per-source soft and hard
  thresholds live in `SOURCE_THRESHOLDS` in
  `backends/node/src/services/sourceHealth.ts`.

- **The Waze monitor no longer has anything to report.** Waze was removed as
  a source — no ingest, no routes, and migrations `071`/`074` dropped its
  tables — so `sources.waze` is not populated. Drop that monitor.

- **Endpoint is unauthenticated** so external monitors can hit it without
  juggling API keys. The information surface is intentionally just
  health booleans + ages, not data — no incident contents.
