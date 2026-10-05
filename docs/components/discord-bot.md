# Discord bot — `discord-bot`

**Entry point: `discord-bot/bot.py`.**

Python 3.10+ with discord.py. It polls the AusAware backend, detects what is
new, and dispatches it to subscribed Discord channels. It never talks to an
upstream data provider directly — everything comes through the backend's `/api`.

Read [`../../OPERATING-RULES.md`](../../OPERATING-RULES.md) before changing anything here.

---

## Files

| File | Purpose |
|---|---|
| `bot.py` | Entry point: slash commands, dispatch, mute resolution, the action-queue worker |
| `alert_poller.py` | `AlertPoller` — polls the backend and detects new items |
| `database.py` | Bot data layer. PostgreSQL when `BOT_DATABASE_URL` is set, SQLite otherwise |
| `embeds.py` | Components V2 builders, including the staff-notify embed |
| `alert_catalog.py` | Loads the alert-type catalog from `shared/alert-catalog.json` |
| `apply_schema_presets.py` | Idempotent schema applier — tables, indexes, triggers |
| `migrate_canonical_alert_types.py` | One-off alert-type key migration |
| `gen_readme_alert_types.py` | Regenerates the alert-type table in the bot README |
| `requirements.txt` | `discord.py`, `aiohttp`, `python-dotenv`, `psycopg2-binary` |
| `env.sample` | Annotated environment template |

## Running it

```bash
cd discord-bot
cp env.sample .env
pip install -r requirements.txt
python apply_schema_presets.py   # idempotent; safe to re-run
python bot.py
```

`bot.py` calls `load_dotenv()` **before** importing the local modules, and that
order is load-bearing: `database.py` reads `BOT_DATABASE_URL` at import time to
decide PostgreSQL versus SQLite. Load `.env` after that import and the bot
silently falls back to SQLite even with a valid URL in `.env`.

Required environment: `DISCORD_BOT_TOKEN`, `BOT_OWNER_ID`, `API_BASE_URL`,
`NSWPSN_API_KEY`, `BOT_DATABASE_URL`. See `env.sample`.

`NSWPSN_API_KEY` must match the backend's. `BOT_DATABASE_URL` here and
`BOT_DATA_DATABASE_URL` in the backend's `.env` must point at the **same**
database: the bot writes it, and the backend reads it to serve the web dashboard.

## One catalog, three consumers

`shared/alert-catalog.json` is the single definition of every alert type. Three
things read it:

- the bot, via `alert_catalog.py`;
- the backend, via `backends/node/src/services/alertCatalog.ts`;
- the web dashboard, via `GET /api/dashboard/alert-catalog`.

That file exists because the definitions used to be nine hand-maintained copies
across three languages — `ALERT_TYPES` in `bot.py`, three maps in `embeds.py`,
three ladders in `alert_poller.py`, the backend whitelist, the dashboard's
`PROVIDERS` literal and a README table — with nothing checking them against each
other. The result was Victoria's SES, EMV and ESTA records being ingested but
unalertable, and the backend rejecting keys the dashboard was offering.

**Add an alert type by editing `shared/alert-catalog.json`.** Do not add a
hand-maintained list anywhere.

`alert_catalog.py` walks up from its own location looking for the file, and
`ALERT_CATALOG_PATH` overrides the search.

## Polling and dispatch

Four `@tasks.loop` loops in `bot.py`:

| Loop | Cadence | Does |
|---|---|---|
| `poll_alerts` | 60s | `AlertPoller.check_alerts()`, then batched dispatch. Emits an INFO heartbeat every fifth tick so quiet stretches are visible without DEBUG |
| `poll_pager` | 30s | `check_pager()`, then batched dispatch |
| `process_message_queue` | 1s | Drains a batch and sends it, fanning out **per channel concurrently** |
| `drain_bot_actions` | 10s | Executes queued admin actions from the backend |

`AlertPoller` (`alert_poller.py`) holds the endpoint map. Several canonical alert
types share one backend fetch and are split apart at extraction:

- `bom_land` and `bom_marine` both come from `/api/bom/warnings`, split by
  category;
- `cfa` and `deeca` both come from `/api/vic-emergency/events` — one feed carries
  every Victorian publisher, split on `properties.agency`. SES, EMV and ESTA
  items arrive on the same feed and are **not** alertable types;
- `endeavour_planned` uses `/api/endeavour/planned`; current outages come back as
  `endeavour_current`;
- the interstate feeds are all RFS-shaped GeoJSON and refresh upstream every
  2 minutes (NT: 5), so the 60s cycle mostly sees no change.

Dispatch is **batched**: several alerts destined for the same channel in one poll
cycle are coalesced into a single Components V2 `LayoutView`, chunked by
character budget, rather than one message per alert × config.

Per-channel fan-out in `process_message_queue` is concurrent on purpose. It used
to be serial, and one slow or rate-limited channel then blocked every other —
which is how a burst overflowed the queue. discord.py still enforces Discord's
per-channel and global rate limits, so concurrency cannot exceed them; it just
keeps every channel making progress.

Two robustness details worth knowing before you touch the dispatch path:

- A `@tasks.loop` that raises an unhandled exception **stops and is not
  auto-restarted**, so the loops carry outer guards.
- An over-budget container is dropped rather than raised. By the time dispatch
  runs, the poller has already marked every alert in the batch as seen — so one
  bad container must degrade to "that container missing", never to an exception
  that aborts the cycle and loses the whole batch.

## Subscriptions, presets and muting

Configuration is per channel, with multiple **presets** per channel. Each preset
has its own alert types, role pings and filters:

- keyword include/exclude;
- a severity floor (RFS watch-and-act, BOM major);
- a geographic bounding box, so a preset only fires for alerts inside a region.

Muting is a **four-tier hierarchy with inheritance**: guild → channel → preset →
per-alert-type.

These 14 are the complete registered set — every `.command()` in
`discord-bot/*.py`. There is no `/dev` group, whatever `bot.py` and
`env.sample` still say about one.

| Command | Does |
|---|---|
| `/setup` | Interactive wizard for alerts and/or pager hits |
| `/alert`, `/alert-remove`, `/alert-list` | Manage alert subscriptions |
| `/pager`, `/pager-remove` | Manage pager subscriptions |
| `/mute` | Stop role pings, keep the embeds |
| `/smute` | Silence entirely — no embed, no ping — without unsubscribing |
| `/unmute` | Re-enable |
| `/status` | Bot status and statistics |
| `/overview` | Dashboard of current incidents across Australia |
| `/summary` | Paged navigator over the hourly radio summaries |
| `/dashboard` | Link to the web dashboard |
| `/help` | Commands and alert types |

Most setup has moved to the web dashboard (`dashboard.html`), which is
Discord-OAuth authenticated and served by `backends/node/src/api/dashboard.ts`.

`/ts` — transcript search — **was retired.** Its backing route
(`/api/rdio/transcripts/search`) was removed because its browse mode ran an
unbounded `COUNT(*)` plus a leading-wildcard `ILIKE` over the whole
calls-joined-transcripts set, blowing the rdio pool's statement timeout. Do not
reinstate the command without solving the query first.

## The action queue

The web dashboard's admin panel cannot reach into Discord. Instead the backend
writes a row to `pending_bot_actions` in the bot's database, and the bot's
`drain_bot_actions` loop picks it up — sync, test, cleanup, broadcast,
`staff_notify`.

Rows are **HMAC-signed** with `BOT_ACTION_SIGNING_SECRET`, set to the same value
on both sides (`backends/node/src/services/botActionSign.ts` signs,
`discord-bot/database.py` verifies), so the bot rejects any insert that did not
come from the backend. When the secret is unset, signing is disabled and the bot
**fails open** — a rollout accommodation, and both sides log a warning. Generate
one with `openssl rand -hex 32`.

The drainer reclaims stale `running` rows on each pass, so an action orphaned by
a bot restart is retried rather than stuck.

## Guild removal does not delete anything

`on_guild_remove` logs loudly and **deletes nothing**. Auto-deletion is disabled
deliberately, because Discord's `guild_remove` event fires on network issues and
reconnects as well as on a real removal, and acting on it was losing configs.

Cleaning up a stale guild's configuration is therefore manual, and **there is
no bot command for it.** The 14 commands in the table above are the complete
registered set and none of them does this; removing a stale guild config means
editing the store directly.

Three places still say otherwise, and all three are wrong: `bot.py:814` and
`:819` tell the operator to run `/dev-cleanup`, and `discord-bot/env.sample:14`
describes gating `/dev clear-seen` and `/dev channel`. There is no `/dev`
command group registered anywhere in `discord-bot/`.

The history explains how the stale references got there without anyone
noticing. `CHANGELOG.md:128-131` records seven `/dev` subcommands being
replaced by the dashboard admin panel and the bot-action queue, and says a
`/dev` group *survived* with `clear-seen`, `channel` and `setup`
(`CHANGELOG.md:63` announces that survivor group). It did not survive to the
current tree — nothing registers it now. So `env.sample:14` is describing a
group that was real when it was written, and `bot.py` is naming a `/dev-cleanup`
that was already gone by then. Reported as a code defect, not fixed here.

This is the same principle as the account rule in
[`../../OPERATING-RULES.md`](../../OPERATING-RULES.md): **never delete a user's or a guild's data
because of an absence.** A guild that looks gone may be a transient disconnect.
An account with no roles and no signup request is a normal public user.

## Where its data lives

PostgreSQL when `BOT_DATABASE_URL` is set, SQLite otherwise. This is a **separate
database from the backend's**. `apply_schema_presets.py` owns its schema and is
idempotent; it creates `alert_presets`, `guild_mute_state`,
`channel_mute_state`, `preset_fire_log`, `preset_audit_log`,
`pending_bot_actions`, `dash_sessions` and `source_health`, with their indexes
and triggers.

The backend reads this same database through `BOT_DATA_DATABASE_URL` to serve the
dashboard, on its own pool, closed separately at shutdown.

## Identifying itself

The bot sends `User-Agent: AusAwareBot/…` and `X-Client-Type: discord-bot` on
every request, which is what makes its traffic show as `[discord-bot]` in the
backend's request log. `NSWPSNBot` is the pre-rebrand UA and is still recognised
so a bot that has not restarted yet is still classified correctly rather than
falling into `other` (`backends/node/src/server.ts`).

## Checks

```bash
python -m py_compile discord-bot/*.py
```

That is what CI runs, plus ruff as an informational step that never blocks
(`.github/workflows/tests.yml`).

## Vocabulary

Ingested radio traffic is a **reception**, never a "call" — in command
descriptions and embed copy as much as in code.

## See also

- [`backend.md`](backend.md) — the API this polls, and `api/dashboard.ts`.
- [`../architecture.md`](../architecture.md) — where the data comes from.
- `discord-bot/README.md` — command reference and Discord application setup.
