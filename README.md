# NSW PSN

Real-time NSW emergency services monitoring platform — aggregates government APIs, community-sourced data, and third-party feeds into interactive maps, live dashboards, and Discord alerts.

**Website:** https://nswpsn.forcequit.xyz

## Features

### Public site
- **Interactive Incident Map** — live markers for fires, traffic incidents, power outages, weather warnings, Waze hazards, and more
- **Live Dashboard** (`/live`) — real-time scanner feed, active unit tracking, and current incident overview
- **Incident Logs** (`/logs`) — searchable historical log of pager hits and ingested events
- **Map Editor** — community editors can add and manage user-submitted incidents on the map
- **Reference Pages** — quick-reference guides for Fire & Rescue NSW, NSW Ambulance, Rural Fire Service, and aviation callsigns

### Discord bot
- **Per-channel alert presets** — multi-bundle subscriptions per channel, each with its own alert types, role pings, and filters
- **Per-preset filters** — keyword include/exclude, severity floor (RFS watch-and-act, BOM major), and a geographic bbox so a preset only fires for alerts inside a chosen region
- **4-tier mute hierarchy** — guild → channel → preset → per-alert-type, with proper inheritance
- **`/summary`** — paged Components V2 navigator over hourly radio summaries; walk back 24 hours or scope to a specific date
- **`/overview`** — incident dashboard across all NSW data sources
- **`/ts`** — search radio-scanner transcripts (full-text, optional date range)
- **Hourly radio summaries** as a subscribable alert type (LLM-generated from rdio-scanner transcripts)
- Works in DMs and via user-install for personal use

### Web management dashboard (`/dashboard`)
- Discord-OAuth-authenticated UI replacing most slash-command setup
- Sidebar of channels → expandable preset list → click any to edit
- Per-preset editor: alert-type chips, role picker, capcodes, mute toggles, per-type overrides
- Geographic filter with a Leaflet click-and-drag bbox
- Mobile-responsive
- **Admin panel** (gated by `DASHBOARD_ADMIN_IDS`) — global stats, source-health monitor, broadcast composer, queued bot actions (sync / test / cleanup), invite-bot button

## Data Sources

| Provider | Data |
|---|---|
| NSW Rural Fire Service | Active bush/grass fires |
| Bureau of Meteorology | Weather warnings (land & marine) |
| LiveTraffic NSW | Incidents, roadwork, flooding, fires, major events |
| Waze | Hazards, police reports, roadwork (via userscript ingest) |
| Endeavour Energy | Current & planned power outages |
| Ausgrid | Power outages (with per-outage detail enrichment) |
| Essential Energy | Current, planned & future outages |
| Pagermon | Pager messages (self-hosted) |
| rdio-scanner | Radio transcripts → hourly LLM summaries |
| Community Editors | Manually added incidents and map data |

## Architecture

```
                        External APIs / userscripts
                                      │
  ┌───────────────────────────────────┴──────────────────────────────────┐
  │  Flask backend  (caching · prewarm cycle · GeoJSON · Postgres        │
  │                  archive · source health · admin REST API)           │
  └────────────────┬────────────────────────────────────┬────────────────┘
                   │                                    │
                   ▼                                    ▼
       Static frontend (Leaflet maps,         Discord bot  (preset
       live + log dashboards, Discord-         dispatch · mute resolution
       OAuth admin/management dashboard)       · filter gate · action queue)
```

### Components

- **Frontend** — static HTML/CSS/JS with Leaflet maps. No build step. Public auth via Supabase; admin auth via Discord OAuth.
- **Backend** (`backends/`) — Flask API proxy that fetches, caches, converts, and archives upstream data; serves the dashboard's REST API; tracks per-source health. Runs on port 8000 behind Cloudflare.
- **Discord bot** (`discord-bot/`) — polls the backend and distributes alerts to subscribed presets via Discord channels. Polls a `pending_bot_actions` Postgres table to execute admin-triggered sync / test / cleanup / broadcast.
- **Database** — PostgreSQL is the canonical store for both the backend and the bot (separate databases). Supabase hosts user-incident rows and frontend auth.

## Setup

### Prerequisites

- Python 3.10+
- Node.js + PM2 (`npm install -g pm2`)
- PostgreSQL
- A web server (Apache or Nginx) serving the document root
- A Discord application with bot + OAuth2 (for the management dashboard)

### Clone

```bash
git clone https://github.com/AkumasCoffin/nswpsn-public-safety.git /var/www/nswpsn
cd /var/www/nswpsn
```

The repo root contains the frontend files — point your web server's document root at this directory. The `.htaccess` in the repo root enables clean URLs (`/live` instead of `/live.html`) for Apache deployments.

### Frontend config

```bash
cp config.sample.js config.js
# Edit config.js with your Supabase project URL, anon key, API base URL, and API key.
```

`config.js` is git-ignored so each deployment maintains its own values.

### Backend

```bash
cd backends
cp env.sample .env       # fill in values
pip install -r requirements.txt
python init_postgres.py  # first-time schema setup
playwright install chromium  # required for Central Watch image scraping
```

Required env vars: `DATABASE_URL`, `NSWPSN_API_KEY`, `SUPABASE_URL`, `SUPABASE_KEY`, `SUPABASE_SERVICE_ROLE_KEY`, plus the dashboard block (`DISCORD_CLIENT_ID`, `DISCORD_CLIENT_SECRET`, `DISCORD_BOT_TOKEN`, `DASHBOARD_SESSION_SECRET`, `BOT_DATA_DATABASE_URL`, `PUBLIC_BASE_URL`). See `env.sample` for the full list with comments.

Update `cwd` in `ecosystem.config.js` to your install path, then start with PM2:

```bash
pm2 start ecosystem.config.js
pm2 start ecosystem.config.js --env dev   # verbose logging
pm2 save
```

### Discord bot

```bash
cd discord-bot
cp env.sample .env       # fill in values
pip install -r requirements.txt
python apply_schema_presets.py   # creates alert_presets, mute_state, fire_log,
                                 # action_queue, dash_sessions tables (idempotent)
pm2 start bot.py --name "NSWPSN-Bot" --interpreter python3
pm2 save
```

Required env vars: `DISCORD_BOT_TOKEN`, `BOT_OWNER_ID`, `API_BASE_URL`, `NSWPSN_API_KEY`, `BOT_DATABASE_URL`. See `env.sample` for the full list.

### Useful PM2 commands

```bash
pm2 status            # running processes
pm2 logs              # tail all logs
pm2 logs 1 --lines 200   # tail backend (id 1) only
pm2 restart all       # restart everything
pm2 startup           # enable PM2 to start on boot
```

### Updating

Pull and restart whichever side changed:

```bash
cd /var/www/nswpsn && git pull
pm2 restart 1   # backend
pm2 restart 2   # bot
# If the bot's schema changed, also:
cd discord-bot && python apply_schema_presets.py
```

## Environment variables

Both `backends/env.sample` and `discord-bot/env.sample` are documented templates. Copy each to `.env` and fill in. Notes:

- `NSWPSN_API_KEY` must match between the backend's `.env` and any client (frontend `config.js`, Discord bot `.env`).
- `BOT_DATABASE_URL` (in the bot's `.env`) and `BOT_DATA_DATABASE_URL` (in the backend's `.env`) should both point at the **same** Postgres database — the backend reads it for the dashboard, the bot writes it for its own state.
- `DASHBOARD_ADMIN_IDS` (backend) — comma-separated Discord user IDs that get the admin panel. Leave unset to disable.
- `WAZE_INGEST_KEY` (backend) — required by the Violentmonkey userscript when posting Waze data to `/api/waze/ingest`.

## Waze data

Waze data is delivered exclusively by a Violentmonkey userscript running in a real browser. The script polls Waze's live-map georss endpoint and POSTs each region's payload to `/api/waze/ingest`. Setup is documented in `docs/waze-userscript.md`. There is no server-side Waze scraper.

## Project layout

```
.
├── *.html                 # static frontend pages
├── styles.css             # shared styles
├── analytics.js           # Umami event tracking
├── auth-common.js         # Supabase auth helpers
├── config.sample.js       # frontend config template
├── .htaccess              # Apache rules: clean URLs, security headers, file blocks
│
├── backends/
│   ├── external_api_proxy.py   # main Flask app (~16 k LoC)
│   ├── db.py                   # Postgres helpers
│   ├── init_postgres.py        # one-shot schema setup
│   ├── ecosystem.config.js     # PM2 config
│   ├── data/                   # cached upstream snapshots (e.g. centralwatch)
│   ├── prompts/                # LLM prompts for radio summaries
│   └── reference/              # local-only operator data (gitignored)
│
└── discord-bot/
    ├── bot.py                  # main bot, slash commands, dispatch, action worker
    ├── alert_poller.py         # periodic upstream poller → new-alert detection
    ├── database.py             # bot DB layer (presets, mute state, fire log, action queue)
    ├── embeds.py               # Components V2 builders
    └── apply_schema_presets.py # idempotent schema applier (tables, indexes, triggers)
```

## License

All rights reserved.
