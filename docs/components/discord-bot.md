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
`NSWPSN_API_KEY`, `BOT_DATABASE_URL`, `BOT_ACTION_SIGNING_SECRET` (the same
value as the backend's; without it the bot executes no queued action and files
nothing). `PUBLIC_BASE_URL` is the SITE's origin for every link the bot builds
itself — set it to the same origin as the backend's `DASHBOARD_FRONTEND_URL`.
See `env.sample`.

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

Five `@tasks.loop` loops in `bot.py`:

| Loop | Cadence | Does |
|---|---|---|
| `poll_alerts` | 60s | `AlertPoller.check_alerts()`, then dispatch. Emits an INFO heartbeat every fifth tick so quiet stretches are visible without DEBUG |
| `poll_pager` | 30s | `check_pager()`, then dispatch |
| `process_message_queue` | 1s | Drains a batch and sends it, fanning out **per channel concurrently** |
| `drain_bot_actions` | 10s | Executes queued admin actions from the backend |
| `run_retention` | 24h | Applies the retention windows to the bot's own tables |

`run_retention` exists because every `cleanup_*` method in `database.py` had one
and nothing ever called it. The only historical caller was a `/dev` clear-seen
command, removed when the dashboard took over — so `seen_alerts` (every FIRMS
cluster of every pass, plus every traffic and outage id), `seen_pager`,
`incident_messages`, `preset_fire_log`, `pending_bot_actions` and
`staff_notify_messages` had all grown without bound since the bot was first
deployed, while the code comments and the backend's dashboard queries assumed the
documented 7/14/30/90-day purge was happening. The window for each table is that
method's own default. Every step is guarded individually: a failure on one table
must not skip the rest and must not stop the loop, because nothing restarts it.

`AlertPoller` (`alert_poller.py`) holds the endpoint map. Several canonical alert
types share one backend fetch and are split apart at extraction:

- `bom_land` and `bom_marine` both come from `/api/bom/warnings`, split by
  category;
- `cfa`, `deeca`, `vicses`, `emv` and `esta` all come from
  `/api/vic-emergency/events` — one feed carries every Victorian publisher, split
  on `properties.agency`. All five are alertable types in their own right, as are
  `nt_bushfires`, `qfd_warning` and `dfes_warning`, and their keyword, severity
  and geo filters work: the catalog now drives the poller's shape branches and
  the embed builder's routing, where the hand-written lists those replaced had
  never gained the newer agencies and still carried the pre-rename `qld_warning`
  and `wa_warning` spellings;
- `endeavour_planned` uses `/api/endeavour/planned`; current outages come back as
  `endeavour_current`;
- the interstate feeds are all RFS-shaped GeoJSON and refresh upstream every
  2 minutes (NT: 5), so the 60s cycle mostly sees no change.

Dispatch is **one message per alert**, each a Components V2 `LayoutView`. Alerts
keep their poll-result order within a channel and the send queue paces them.

It used to pack a poll's alerts for a channel into a single `LayoutView`, which
saved messages while the Waze firehose was running but cost two things that matter
more at today's volume. **Incident tracking**: only the first incident-bearing
alert in a batch could have its message recorded, because the row stores one
`jump_url` per message, so every other incident in that batch was never written
down and could never be linked to — or replied to — when it updated.
**Legibility**: five unrelated incidents shared one timestamp and one block, and
an update could only reply to the whole batch.

**`firms` is the one exception**, for the opposite reason: a satellite hotspot is
not an incident — it has no status to update and nothing to reply to — and one
pass over a fire ground lights up hundreds of them. A poll's hotspots for a
channel are collapsed into a single digest, built after the per-alert loop.

`firms` is also **opt-in, and only honoured with a region**. A preset must carry a
`geofilter` (or a legacy `bbox`), because an unscoped subscription meant every
hotspot in Australia: on 2026-10-12 one poll queued 2,577 hotspots for a single
channel, 9,990 fired inside the hour, the send queue filled, 1,996 messages were
dropped and every other channel's alerts were delayed behind the backlog. The two
presets responsible had swept `firms` in through a "select everything" type list
and carried no filters at all. The rule is enforced in two places:
`_SCOPED_ONLY_ALERT_TYPES` in `discord-bot/database.py` refuses an unscoped
subscription at the single point where a subscription is resolved — which also
stops FIRMS being fetched and clustered at all once nobody is left subscribed, and
retires existing unscoped rows with no operator re-saving them — and
`_OPT_IN_ONLY_TYPES` in `bot.py` keeps a catch-all picker ("Enable All", the
generic `/setup` dropdown) from choosing the type in the first place.

**`shark_report` is in both sets too**, added on the same principle before
rather than after an incident. It is a national feed of coastal events —
roughly 25 a day, 56% of them a tagged shark passing a listening station — and
a channel almost always wants one stretch of coast. Unscoped it is
mostly-elsewhere noise.

That second half only works if the dispatcher can read the alert's
coordinates, and this is the trap: **`preset_has_geoscope` checks that a
geofilter key exists, not that it can ever match.** A scoped-only type whose
location `_alert_lat_lng` cannot find would satisfy the gate and then pass
every geofilter unconditionally — exactly inverting the rule. So any type added
to `_SCOPED_ONLY_ALERT_TYPES` must also have a working branch in
`_alert_lat_lng`; shark reports get one by carrying the `geo_point` feed shape,
which puts them in `_GEO_POINT_ALERT_TYPES`.

Shark reports stay a **single** type with the seven report categories exposed
as subtype filters, rather than one type per category: the provider picker caps
at 25 options, and this keeps the split per-preset. `_alert_subtype_token`
returns the category, which lands in the archive's `category` column — so
`_SUBTYPE_FIELDS_DEFAULT` already covers it and the dashboard needs no entry.
Note also that the feed's endpoint serves a 48-hour window by default because
`seen_alerts` is pruned at 7 days: a type whose records outlive their dedup
rows re-alerts.

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
`staff_notify`, and for bug reports and suggestions `feedback_channels` and
`feedback_board` (next section).

Rows are **HMAC-signed** with `BOT_ACTION_SIGNING_SECRET`, set to the same value
on both sides (`backends/node/src/services/botActionSign.ts` signs,
`discord-bot/database.py` verifies), so the bot rejects any insert that did not
come from the backend. With the secret unset the bot **fails closed**: a queued
action executes with no further authentication, so without the shared secret there
is nothing to tell a backend insert from anyone else's. It refuses every queued
action, logs one explainer the first time — naming what has stopped working: sync,
test, broadcast and staff notifications — and completes each row as `error` with a
reason the dashboard shows, so the misconfiguration surfaces instead of actions
silently vanishing. This used to fail open as a rollout accommodation; the secret
is deployed on both sides now, so an unsigned queue is a misconfiguration, not a
migration state. Generate one with `openssl rand -hex 32`.

The drainer reclaims stale `running` rows on each pass, so an action orphaned by
a bot restart is retried rather than stuck.

## Bug reports and suggestions

The one place the bot WRITES to the backend. Two public channels per kind
(bug, suggestion), chosen on the staff page's Discord settings and pushed here
by the `feedback_channels` action, because the bot never reads the backend's
`app_settings`; the bot keeps them in `feedback_channels`.

**Intake.** The bot posts a how-to (`build_feedback_notice_embed`, with a link
button to the same thing on the site, `build_feedback_notice_view`) — not pinned:
every other message there is removed once filed, so it is the only one standing.
(The `feedback_channels` action records the channels even when that post fails,
say in a channel the bot cannot yet write to, and the start-up refresh posts it
later; the refresh also re-edits a how-to whose words or button URL have
changed, one kind independently of the other.) `on_message` files every
member's message there as a report: a signed `POST /api/discord/feedback` —
the static key plus `X-Bot-Signature`, which is `sign_bot_action(secret,
'feedback', 'bot', body)` with the same `BOT_ACTION_SIGNING_SECRET` and
canonical form as the outbox rows, run the other way (the backend's
`services/auth/botSigned.ts` verifies, and fails closed without the secret).
Attachments are read before anything else and re-sent on the board post, since
the member's message is deleted once filed and Discord's attachment links
expire; the board post is the only place they can live, so a kind with no board
channel cannot keep them — a report that is only a screenshot is then asked for
words, and one filed with screenshots is told they were not kept. Every answer
to the member is an embed (`build_feedback_reply_embed`) in the channel,
mentioning them and self-deleting after 10–30 s, so the channel holds nothing but
the how-to and messages still to be fixed (Discord has no "only you can see
this" for a reply to an ordinary message; that exists for interactions alone).
The confirmation carries an "Open on the board" button. The message must be a `#`
title line with the details on the lines under it (`split_feedback_message`) —
that is what gives a report a title and a description of its own; one without
that shape, or with no details, is answered with the format and LEFT IN PLACE:
the member edits it into shape and `on_message_edit` files it then (their text is
theirs to fix, not the bot's to delete). A second report within 20 s of the
last simply waits for its turn. The backend's refusal is answered and the
message left in place — the cap (five open reports per reporter) or a report it
will not take (its reason is relayed; title and body are cut to the backend's
limits in UTF-16 units, `cut_utf16`, so an emoji counts as the backend counts
it) — as is an outage ("try again in a minute"). Discord's own system messages
(a pin notice has a human author) are not reports. There are no prefix
commands, so the `on_message` override has no `process_commands` to forward to.

**Board.** Every report is posted with its status (`build_feedback_board_embed`):
a Discord one by the bot itself at filing, with the files, after which it tells
the backend the message id (`POST /api/discord/feedback/:id/board`, the
"Open on the board" link on the staff page); a site one when the backend's
`feedback_board` action with event `new` arrives. A status change (`status`)
replies under the post with the new status and staff's note, and edits the
post's Status field; a removed report (`delete`) takes down the replies and then
the post. The map of report → message is `feedback_board_posts`, with the
backend's own copy of the ids (sent as `channel_id`/`message_id` on `status`
and `delete`) standing in when the bot has no record; the bot's replies are
`feedback_board_replies`, and the delete also sweeps the channel after the post
for any of its own replies recorded before that table existed. A post someone
removed by hand still has its replies cleared, then is forgotten. A kind with no
board channel skips the posting; a status change or removal still reaches a
post made while there was one. A board link the backend refuses
(`POST …/board` not 2xx) is logged by `_feedback_post`.

The private staff alert for a new report is an ordinary `staff_notify` with
kind `bug_report` or `suggestion`; a status change arrives as event `resolved`
(done, declined, duplicate) or the newer `update` (looking into it), both of
which edit the alert in place — `update` without the resolved styling.

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
database from the backend's**. `apply_schema_presets.py` owns the preset
schema and is idempotent; it creates `alert_presets`, `guild_mute_state`,
`channel_mute_state`, `preset_fire_log`, `preset_audit_log`,
`pending_bot_actions`, `dash_sessions` and `source_health`, with their indexes
and triggers. The rest — the staff-alert message map, `feedback_channels`,
`feedback_board_posts`, `feedback_board_replies` — is created by
`Database.init_db()` at start-up, also `IF NOT EXISTS`.

The backend reads this same database through `BOT_DATA_DATABASE_URL` to serve the
dashboard, on its own pool, closed separately at shutdown.

### The dashboard login is not the website login

They are two independent systems that both happen to use Discord, and this
surprises people, so: the website authenticates through **Supabase**, with
Discord as one OAuth provider among several, and keeps a JWT in the browser.
`dashboard.html` runs its **own** Discord OAuth app and keeps a signed
`nswpsn_dash_sess` cookie on the API host plus a row in `dash_sessions`.
Neither knows the other exists, so signing in to the site grants nothing on the
dashboard.

That cannot simply be bridged. The dashboard asks Discord for scope `identify
guilds`, and the guild list *is* the dashboard — it is how it knows which
servers you may configure. Supabase's Discord login does not request `guilds`,
so even sharing an identity would still leave a Discord round-trip to collect
the scope, and Supabase provider tokens are not refreshed. Closing this gap
means changing the scope the whole site asks for, and re-consent for every
existing account.

**Sessions slide.** `DASH_SESSION_TTL_SECS` is 30 days and `loadSession`
extends it on use, throttled by `DASH_SESSION_RENEW_AFTER_SECS` (1 day) so an
open dashboard writes one row a day rather than one per request. It was a flat
24h with no renewal, which signed people out a day after login however much
they used it — if you opened the dashboard less than daily you re-authenticated
on literally every visit.

Two things make this work that are easy to break. The renewal must **re-issue
the cookie**, because the cookie carries its own copy of `exp` and
`loadSession` rejects on that one; extending only the server-side row renews
nothing. And the renewal must not go through `putSession`, which bumps
`dashboard_users.login_count` — a renewal is not a login. The last-renewal time
is derived from `exp - TTL` rather than stored, which is also what quietly
migrates sessions issued under the old 24h TTL.

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
