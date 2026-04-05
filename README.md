# NSW PSN

Real-time NSW emergency services monitoring platform — aggregates government APIs, community-sourced data, and third-party feeds into interactive maps, live dashboards, and Discord alerts.

**Website:** https://nswpsn.forcequit.xyz

## Features

- **Interactive Incident Map** — live markers for fires, traffic incidents, power outages, weather warnings, and more
- **Map Editor** — community editors can add and manage map data
- **Live Dashboard** — real-time scanner feed and active unit tracking
- **Incident Logs** — searchable historical log of all ingested events
- **Discord Bot** — pushes alerts to subscribed Discord channels
- **Reference Pages** — quick-reference guides for Fire & Rescue NSW, NSW Ambulance, Rural Fire Service, and aviation callsigns

## Data Sources

| Provider | Data |
|---|---|
| NSW Rural Fire Service | Active bush/grass fires |
| Bureau of Meteorology | Weather warnings (land & marine) |
| LiveTraffic NSW | Incidents, roadwork, flooding, fires, major events |
| Waze | Hazards, police reports, roadwork |
| Endeavour Energy | Current & planned power outages |
| Ausgrid | Power outages |
| Essential Energy | Current, planned & future outages |
| Pagermon | Pager messages (community-hosted) |
| Community Editors | Manually added incidents and map data |

## Architecture

```
External APIs + Community Data
  -> Flask backend (caching, GeoJSON conversion, PostgreSQL archival)
    -> Static frontend (Leaflet.js maps, dashboards)
    -> Discord bot (polling + alert distribution)
```

### Components

- **Frontend** — static HTML/CSS/JS with Leaflet.js maps. No build step. Auth via Supabase.
- **Backend** (`backends/`) — Flask API proxy that fetches, caches, converts, and archives data. Runs on port 8000.
- **Discord Bot** (`discord-bot/`) — polls the backend and distributes alerts to subscribed channels.

## Setup

### Prerequisites

- Python 3.10+
- Node.js + PM2 (`npm install -g pm2`)
- PostgreSQL
- A web server (Apache/Nginx) serving a document root

### Clone

Clone into your web server's document root (e.g. `/var/www/nswpsn`):

```bash
git clone https://github.com/AkumasCoffin/nswpsn-public-safety.git /var/www/nswpsn
cd /var/www/nswpsn
```

The root HTML files are the frontend — point your web server's document root at this directory.

### Backend

```bash
cd backends
cp env.sample .env       # fill in values
pip install -r requirements.txt
python init_postgres.py  # first-time schema setup
```

Update `cwd` in `ecosystem.config.js` to match your install path (e.g. `/var/www/nswpsn/backends`), then start with PM2:

```bash
pm2 start ecosystem.config.js          # production
pm2 start ecosystem.config.js --env dev # verbose logging
pm2 save                                # persist across reboots
```

### Discord Bot

```bash
cd discord-bot
cp env.sample .env       # fill in values
pip install -r requirements.txt
pm2 start bot.py --name "NSWPSN-Bot" --interpreter python3
pm2 save
```

### Frontend

Static files served directly by your web server — no build step. Ensure the document root points to the repo root so `index.html`, `map.html`, etc. are accessible.

### Useful PM2 Commands

```bash
pm2 status          # check running processes
pm2 logs            # tail all logs
pm2 restart all     # restart everything
pm2 startup         # enable PM2 to start on boot
```

## Environment

Both `backends/` and `discord-bot/` have `env.sample` files. Copy to `.env` and configure. The `NSWPSN_API_KEY` must match between the backend and its clients (frontend, Discord bot).

## License

All rights reserved.
