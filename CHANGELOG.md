# Changelog

All notable changes to NSW PSN are recorded here. Newest first. Each entry
groups what changed under the conventional headings (`Added`, `Changed`,
`Fixed`, `Removed`, `Security`). Dates are ISO-8601 in the deployment's local
calendar.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [2026-04-26] — Preset architecture, admin dashboard, project-wide cleanup

Multi-chat refactor moving the Discord bot off the legacy
`alert_configs` / `pager_configs` schema onto a multi-preset-per-channel
design, plus a full Discord-OAuth admin/management web dashboard, retirement
of the in-process Waze scraper, and a memory-leak / dead-code sweep.

### Added
- **Web dashboard** (`/dashboard.html`) — Discord-OAuth-authenticated.
  Sidebar of channels → preset list, per-preset editor with alert-type
  chips, role picker, capcodes, enabled/ping toggles, per-type overrides.
  Mobile-responsive with a hamburger drawer.
- **Multi-preset architecture** — channels can now have many named presets
  with their own subscription bundles. 4-tier mute hierarchy (guild →
  channel → preset → per-type) replaces the flat per-row `enabled` flag.
- **Per-preset filters** — keyword include/exclude, severity floor
  (RFS watch-and-act, BOM major, etc.), and a Leaflet-backed bbox geofilter
  with click-and-drag rectangle. Applied after the mute cascade in
  `preset_alert_matches`.
- **`/summary` paged navigator** — Components V2 view that walks back up
  to 24 hours of hourly radio summaries, or every summary for a given
  `date:YYYY-MM-DD`. Heading shows date + time range + summary count +
  page count. Navigation deletes and re-sends so buttons stay at the
  channel bottom.
- **Admin dashboard** — gated by `DASHBOARD_ADMIN_IDS`. Servers / user
  installs / dashboard users / preset stats; per-source-feed health panel
  with auto-refresh + Clear button; per-guild preset breakdown; recent
  bot-action log; broadcast composer with live preview, per-server
  channel override, and auto-detection of staff channels.
- **Bot action queue** (`pending_bot_actions` Postgres table) — backend
  enqueues `sync` / `test` / `cleanup` / `broadcast`; bot polls every
  10 s and writes results back. Admin actions never block the request worker.
- **Source health monitoring** — every upstream feed (RFS, BOM, traffic_*,
  Endeavour, Ausgrid, Waze, pager, rdio) tracked with last-success /
  last-error / consec-fails, persisted to Postgres so counters survive
  `pm2 restart`.
- **Persistent dashboard sessions** — `dash_sessions` table; logins
  survive `pm2 restart 1`.
- **Fire log** — `preset_fire_log` records every dispatch; the overview
  shows "fired N× in 7d · last fire 3h ago" per preset.
- **Clean URLs in `.htaccess`** — `/live`, `/dashboard`, `/map` etc. work
  without `.html`. Old `.html` URLs 301-redirect to the clean form.
- New surviving `/dev` group: `clear-seen`, `channel`, `setup` — owner-only.
- `/overview` slash command (the old `/summary`'s incident-dashboard payload).

### Changed
- **`/summary` and `/overview` swap** — old `/summary` (incident dashboard)
  renamed to `/overview`; `/summary` is now the radio-summary navigator.
- **`alert_presets` schema** — `TEXT[] alert_types`, `BIGINT[] role_ids`,
  `JSONB type_overrides`, `JSONB filters`, `pager_enabled BOOL`,
  `pager_capcodes TEXT`. GIN-indexed `alert_types` for fast lookup.
- Dispatch path rewritten on presets — role-id union per channel, mute
  resolution (`Database.resolve_preset_effective_state`), per-preset
  filter gate.
- `chunk_containers_for_message` — per-container truncation when a single
  container exceeds the 4000-char V2 cap (busy summaries with 30
  transcripts hit this).
- Ausgrid endpoint rewired with bbox + zoom params; per-outage
  `GetOutage` detail merged into the marker payload to recover Streets,
  Cause, JobId, EndDateTime, and StatusText fields the new map API drops.
- `_dash_bot_guild_ids` cached 30 s; `_AUSGRID_DETAIL_CACHE` bounded with
  LRU eviction; `_DASH_GUILD_META_CACHE` and `_dash_discord_cache`
  evict-on-write; `_permission_error_channels` self-sweeps. All
  unbounded module-level state from the leak audit is now bounded.
- env.sample (both): legacy/dead vars stripped, undocumented live ones
  added (`DASHBOARD_FRONTEND_URL`, `DASHBOARD_COOKIE_DOMAIN`,
  `PUBLIC_BASE_URL` on the bot side, `SUMMARY_TZ`).
- `apply_schema_presets.py` now embeds the schema inline (no external
  `.sql` file) and applies the legacy-table `DROP`s.
- `database.py` shrank from 1308 → 937 lines after the legacy method sweep.

### Fixed
- **CORS PATCH stripped by `@app.after_request`** — the override was
  hard-coding `GET, POST, PUT, DELETE, OPTIONS`, masking the Flask-CORS
  config that included `PATCH`. Preset edits silently failed.
  `Access-Control-Max-Age: 60` added so stale preflights age out fast.
- **404-on-send no longer wipes presets** — was auto-deleting the entire
  preset on a single Discord 404 (transient permission issue / channel
  rename / cache miss). Now debounce-logs only; `on_guild_channel_delete`
  still handles real deletions.
- **Tz-naive vs tz-aware sort crash** in `_get_alert_timestamp` when a
  batch mixed RFS (aware) and Waze (occasionally naive) timestamps.
- **`/summary` 400 on Components V2** — Buttons were added at the
  LayoutView top level (only types 1, 9, 10, 12, 13, 14, 17 are valid
  there). Now wrapped in `discord.ui.ActionRow`.
- **Missing guild names in admin overview** — fallback to bot-token
  `GET /guilds/{id}` (cached 10 min) when no active session covers
  the guild.
- **Ausgrid HTTP 500 for months** — root cause was the upstream API
  starting to require bbox + zoom params; the gateway returned generic
  500s rather than 4xx, hiding the real error.
- **rdio source health flapping "Degraded"** every cycle — soft threshold
  was 30 min but the scheduler runs hourly. Bumped to 65 min.
- Single-session-per-user dedup in admin overview (was showing the same
  user multiple times when they had several active cookies).

### Removed
- **`alert_configs` and `pager_configs` Postgres tables** (Phase 3).
- **18 legacy database methods** in `database.py` (`add_config`, `get_config`,
  `get_pager_config`, `set_alert_enabled`, `count_configs`, `remove_guild_data`,
  etc.).
- **Waze browser scraper** — `_waze_browser_worker`, `_start_waze_browser_worker`,
  `_waze_page_fetch`, `_waze_browser_fetch_regions`, all `_waze_proxy_*`
  state, `WAZE_PROXY_*` env vars. ~1000 LoC. Replaced by the userscript
  POSTing to `/api/waze/ingest`.
- **Waze browser dependencies** — `patchright`, `camoufox`, `playwright-stealth`
  uninstalled. Plain `playwright` retained for Central Watch.
- **`/dev status`, `/dev status-public`, `/dev broadcast`, `/dev test`,
  `/dev sync`, `/dev cleanup`, `/dev cleanup-confirm`** — replaced by
  the dashboard admin panel + bot-action queue. Surviving `/dev` group
  is `clear-seen`, `channel`, `setup`.
- Dead helpers post-`/dev` removal: `build_dev_status_data`,
  `build_dev_status_components`, `BroadcastConfirmView` (~450 LoC).
- Vestigial files: `backends/stats_archive.db`,
  `discord-bot/migrate_sqlite_to_postgres.py`,
  `discord-bot/schema_presets.sql`,
  `discord-bot/migrate_presets.py`,
  `backends/active_units_api.py`.
- `.gitignore` no longer references nonexistent paths
  (`ref_data/`, `repo_stuff/`).

### Security
- `_dash_validate_filters` enforces filter shape on POST + PATCH
  (whitelist for `severity_min`, length caps on keyword arrays, lat/lng
  bounds + min ≤ max on bbox).
- `jsonb_set` paths for `type_overrides` validated against the
  `_DASH_ALERT_TYPES` allow-list before being interpolated into SQL —
  no user input ever reaches a path literal.
- HMAC-signed dashboard session cookie (`DASHBOARD_SESSION_SECRET`) with
  server-side session store; cookie only carries `{sid, exp}`.
- Per-guild permission check (`_dash_guild_guard`) on every preset /
  mute-state / channel / role endpoint; admin paths additionally gated
  by `_dash_require_admin()` against `DASHBOARD_ADMIN_IDS`.

---

## [2026-04-10] — Heartbeat + Waze stability

### Fixed
- Discord bot heartbeat blocking: batched DB queries and ran them in
  the executor so Postgres latency can't starve the gateway.
- Waze browser auto-restart on WAF blocks; Flask debug mode disabled in
  production; region polling order shuffled to even out request load.

---

## [2026-04-09] — Critical memory leaks

### Fixed
- Chromium DOM accumulation in long-running browser sessions.
- Cache eviction misses across multiple in-memory dicts.
- Browser process memory management on the Waze scraper path.

---

## [2026-04-07]

### Added
- Connection pooling.
- Session cleanup pass for the dashboard.
- General Hazard Rating (1–10) on beach tooltips.
- Beachsafe weather, tide, UV, attendance, and patrol info.
- Umami event tracking on the live page; fixed auth config loading order.

### Fixed
- Cache-eviction misses; double-firing of analytics events.

### Changed
- Migrated `incidents` + `incident_updates` from Supabase REST onto the
  local Postgres-backed API.
