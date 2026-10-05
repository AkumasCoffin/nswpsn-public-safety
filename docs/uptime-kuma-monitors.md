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
source name. Each entry carries `soft_threshold_secs` (its poll cadence) and
`hard_threshold_secs` (twice that).

A source is *intended* to read `ok` when LiveStore holds a snapshot for it and
`unknown` when it does not. **That is not what it currently does** — see the
warning below before building any of these.

> ⚠️ **The monitors in this table cannot fire.** `src/api/status.ts:376` tests
> `liveStore.getData(s.name) !== undefined`, but `getData` returns `null`
> rather than `undefined` when a source has no snapshot
> (`src/store/live.ts:151-153`). `null !== undefined` is always true, so `has`
> is unconditionally true: every registered source reports `ok: true`,
> `status: 'ok'`, and `summary.sources_unknown` is structurally always `0`.
> Confirmed against the live endpoint, which reported 36 total / 36 ok /
> 0 unknown with every source `ok: true`.
>
> Build these and they go green and stay green whether the sources are
> polling or not, which is worse than having no monitor at all. Until that
> comparison is fixed, source-level alerting has to come from somewhere other
> than `/api/status`.

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
| Essential | `checks.sources.essential_current.ok` | `true` |
| NASA FIRMS | `checks.sources.firms_hotspots.ok` | `true` |
| ACT Ambulance | `checks.sources.act_ambulance.ok` | `true` |
| Aircraft (aggregators) | `checks.sources.adsb_aircraft.ok` | `true` |

Any other registry source works the same way — use its registry name, subject to
the same warning. A rollup is available too: `summary.sources_total`,
`summary.sources_ok` and `summary.sources_unknown`.

**There is no `ausgrid` key.** The two Ausgrid sources do not register at all:
`AUSGRID_DISABLED` defaults to `'true'` (`backends/node/src/sources/ausgrid.ts:262`)
because the upstream endpoints have 404'd for months. An expression pointing at
`checks.sources.ausgrid.ok` does not resolve against the live payload.

## Notes

- **Group these in Uptime Kuma**: Settings → Add Monitor → Type: Group →
  drag related monitors in. Two natural groups: "Backend Internals" and
  "Data Sources". Keeps the dashboard scannable.

- **rdio-scanner stays unknown for ~65 min after a backend restart**
  because the rdio summary scheduler runs hourly. It'll flip green on
  the first successful summary cycle.

- **A source-level outage does not change the overall `status` at all.** It
  does not 503 and it does not flip `degraded` either: the sources branch at
  `backends/node/src/api/status.ts:438-443` is deliberately an empty `if`
  body, with a comment saying freshness is left to each source's own entry.
  Only `database` (`:415`), `archive_buffer` (`:423`) and `filter_cache`
  (`:428`) move `status`. Nor can `sources` contribute to `failedChecks` — it
  is a map of source names with no top-level `ok` field. So an overall-status
  monitor will never tell you a source has stopped, and per the warning above
  the per-source monitors will not either.

- **Thresholds are constants in the backend**, not environment variables —
  changing one is a code change and a deploy, not a restart.
  `STATUS_DB_TIMEOUT_SECS`, `STATUS_WRITER_STALE_SECS`,
  `STATUS_BUFFER_WARN_RECORDS` and `STATUS_FILTER_CACHE_STALE_SECS` are at
  the top of `backends/node/src/api/status.ts`.

- **Per-source thresholds are derived, not configured.** Each source's
  `soft_threshold_secs` and `hard_threshold_secs` are computed inline from its
  registry cadence at `backends/node/src/api/status.ts:380-383` — soft is the
  cadence, hard is twice it. `SOURCE_THRESHOLDS` in
  `backends/node/src/services/sourceHealth.ts` is a different and older table,
  read only by the admin dashboard (`src/api/dashboard.ts:85`); `status.ts`
  does not import it, and most of its keys are not registry source names. It
  has no effect on `/api/status`. Live proof: `firms_hotspots` reports
  `soft=900` (its 15m cadence) while `SOURCE_THRESHOLDS.firms_hotspots` is
  `3600`.

- **The Waze monitor no longer has anything to report.** Waze was removed as
  a source — no ingest, no routes, and migrations `071`/`074` dropped its
  tables — so `sources.waze` is not populated. Drop that monitor.

- **Endpoint is unauthenticated** so external monitors can hit it without
  juggling API keys. The information surface is intentionally just
  health booleans + ages, not data — no incident contents.
