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

| Monitor | Expression | Expected |
|---|---|---|
| Database | `checks.database.ok` | `true` |
| Archive writer | `checks.archive_writer.ok` | `true` |
| Archive buffer | `checks.archive_buffer.ok` | `true` |
| Waze ingest | `checks.waze_ingest.ok` | `true` |
| Police heatmap | `checks.police_heatmap.ok` | `true` |
| Filter cache | `checks.filter_cache.ok` | `true` |

## Sources

| Monitor | Expression | Expected |
|---|---|---|
| RFS | `sources.rfs.ok` | `true` |
| BOM | `sources.bom.ok` | `true` |
| Pager | `sources.pager.ok` | `true` |
| LiveTraffic incidents | `sources.traffic_incidents.ok` | `true` |
| LiveTraffic roadwork | `sources.traffic_roadwork.ok` | `true` |
| LiveTraffic flood | `sources.traffic_flood.ok` | `true` |
| LiveTraffic fire | `sources.traffic_fire.ok` | `true` |
| LiveTraffic majors | `sources.traffic_major.ok` | `true` |
| Endeavour | `sources.power_endeavour.ok` | `true` |
| Ausgrid | `sources.power_ausgrid.ok` | `true` |
| Waze | `sources.waze.ok` | `true` |
| rdio-scanner | `sources.rdio.ok` | `true` |

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

- **Threshold tuning** is via env vars on the backend (no monitor change
  needed): `STATUS_DB_TIMEOUT_SECS`, `STATUS_WRITER_STALE_SECS`,
  `STATUS_WAZE_STALE_SECS`, `STATUS_BUFFER_WARN_RECORDS`,
  `STATUS_HEATMAP_STALE_SECS`, `STATUS_FILTER_CACHE_STALE_SECS`.
  Per-source soft/hard thresholds live in `_SOURCE_THRESHOLDS` in
  `external_api_proxy.py`.

- **Endpoint is unauthenticated** so external monitors can hit it without
  juggling API keys. The information surface is intentionally just
  health booleans + ages, not data — no incident contents.
