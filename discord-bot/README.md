# AusAware Alert Discord Bot

A Discord bot that provides real-time alerts for NSW emergency services, BOM warnings, traffic incidents, power outages, and pager messages.

**Website:** [nswpsn.forcequit.xyz](https://nswpsn.forcequit.xyz/)

## Features

- 🔥 **RFS Incidents** - Bush fire alerts from NSW Rural Fire Service
- ⛈️ **BOM Warnings** - Weather warnings (severe weather, marine, and general)
- 🚗 **Traffic Alerts** - Incidents, roadwork, floods, fires, and major events
- ⚡ **Power Outages** - Endeavour Energy, Ausgrid and Essential Energy (NSW), AusNet, CitiPower, Powercor and Jemena (Victoria), SA Power Networks and Western Power outages
- 📟 **Pager Feed** - Real-time pager messages with capcode filtering

## Commands

### Setup & Configuration

| Command | Description |
|---------|-------------|
| `/setup [channel]` | Interactive setup wizard for alerts and pager |
| `/alert <channel> [type] [role]` | Set up alerts for a channel (leave type empty for ALL) |
| `/alert-remove <channel> [type]` | Remove alert subscriptions (leave type empty for ALL) |
| `/alert-list` | List all alert subscriptions for this server |
| `/pager <channel> [capcodes] [role]` | Set up pager alerts |
| `/pager-remove <channel>` | Remove pager subscription |

### Info Commands

| Command | Description |
|---------|-------------|
| `/help` | Show available commands and alert types |
| `/status` | Check bot status and statistics |

## Setup

### 1. Create a Discord Application

1. Go to [Discord Developer Portal](https://discord.com/developers/applications)
2. Click "New Application" and give it a name
3. Go to "Bot" section and click "Add Bot"
4. Copy the bot token (you'll need this later)
5. Enable "Message Content Intent" under Privileged Gateway Intents

### 2. Configure Bot Permissions

1. Go to "OAuth2" → "URL Generator"
2. Select scopes: `bot`, `applications.commands`
3. Select bot permissions:
   - Send Messages
   - Embed Links
   - Mention Everyone (for role pings)
   - Read Message History
   - Use Slash Commands
   - Manage Messages, Attach Files (the bug-report / suggestion intake channels:
     the bot removes each member's message once filed, leaving only its
     how-to, and re-sends their screenshots on the public board)
4. Copy the generated URL and use it to invite the bot to your server

### 3. Configure Environment

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your configuration
nano .env
```

Required environment variables:
- `DISCORD_BOT_TOKEN` - Your Discord bot token
- `API_BASE_URL` - URL of the NSW PSN API (default: http://localhost:8000)
- `NSWPSN_API_KEY` - API key for authentication

### 4. Install Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 5. Run the Bot

```bash
python bot.py
```

## Alert Types

<!-- BEGIN GENERATED ALERT TYPES -->

_Generated from `shared/alert-catalog.json` by `gen_readme_alert_types.py` — do not edit by hand._

| Provider | Type | Agency | Description |
|----------|------|--------|-------------|
| NSW Rural Fire Service | `rfs` | NSW RFS | RFS Major Incidents |
| NASA FIRMS | `firms` | NASA FIRMS | FIRMS Fire Hotspots |
| VIC Emergency | `cfa` | CFA (Vic) | CFA (Vic) |
| VIC Emergency | `deeca` | DEECA (Vic) | DEECA (Vic) |
| VIC Emergency | `vicses` | SES | VICSES (Vic) |
| VIC Emergency | `emv` | EMV (Vic) | EMV (Vic) |
| VIC Emergency | `esta` | Triple Zero Vic (dispatch) | Triple Zero Vic (dispatch) |
| QLD Fire Department | `qfd` | QLD Fire Dept | QFD Incidents |
| QLD Fire Department | `qfd_warning` | QLD Fire Dept | QFD Warnings |
| DFES (WA) | `dfes` | DFES (WA) | DFES Incidents |
| DFES (WA) | `dfes_warning` | DFES (WA) | DFES Warnings |
| SA Fire Services | `sa_cfs` | SA CFS | SA CFS |
| SA Fire Services | `sa_mfs` | SA MFS | SA MFS |
| NT Emergency | `nt_fire` | NT Fire & Rescue | NT Fire & Rescue |
| NT Emergency | `nt_bushfires` | Bushfires NT | Bushfires NT |
| ACT Ambulance | `act_ambulance` | ACT Ambulance Service | ACT Ambulance |
| Bureau of Meteorology | `bom_land` | Bureau of Meteorology | BOM Land Warnings |
| Bureau of Meteorology | `bom_marine` | Bureau of Meteorology | BOM Marine Warnings |
| Roads (NSW, QLD, VIC, SA, ACT) | `traffic_incident` | NSW, QLD, VIC, SA and ACT road agencies | Traffic Incidents |
| Roads (NSW, QLD, VIC, SA, ACT) | `traffic_roadwork` | NSW, QLD, VIC, SA and ACT road agencies | Traffic Roadwork |
| Roads (NSW, QLD, VIC, SA, ACT) | `traffic_flood` | NSW, QLD, VIC, SA and ACT road agencies | Flood Hazards |
| Roads (NSW, QLD, VIC, SA, ACT) | `traffic_fire` | Transport for NSW | Traffic Fires |
| Roads (NSW, QLD, VIC, SA, ACT) | `traffic_majorevent` | NSW, QLD, VIC, SA and ACT road agencies | Major Events |
| Roads (NSW, QLD, VIC, SA, ACT) | `traffic_alpine` | Transport for NSW | Alpine Conditions |
| Roads (NSW, QLD, VIC, SA, ACT) | `traffic_lga` | Transport for NSW | Council Roads |
| Power outages | `endeavour_current` | Endeavour Energy | Endeavour Current Outages |
| Power outages | `endeavour_planned` | Endeavour Energy | Endeavour Planned Outages |
| Power outages | `ausgrid` | Ausgrid | Ausgrid Outages |
| Power outages | `essential_unplanned` | Essential Energy | Essential Energy Unplanned Outages |
| Power outages | `essential_future` | Essential Energy | Essential Energy Future Outages |
| Power outages | `ausnet` | AusNet | AusNet Outages |
| Power outages | `citipower` | CitiPower | CitiPower Outages |
| Power outages | `powercor` | Powercor | Powercor Outages |
| Power outages | `jemena` | Jemena | Jemena Outages |
| Power outages | `sapn` | SA Power Networks | SA Power Networks Outages |
| Power outages | `westernpower` | Western Power | Western Power Outages |
| User Submissions | `user_incident` | User Submissions | User Incidents |
| Radio Scanner | `radio_summary` | Radio Scanner | Radio Summary |
| The Wire | `wire_article` | The Wire | Wire Articles |
| The Wire | `wire_fleet` | The Wire | Wire Fleet Additions |
| Dorsal Watch | `shark_report` | Dorsal Watch | Shark Reports |

<!-- END GENERATED ALERT TYPES -->

## Pager Capcodes

When setting up pager alerts, you can optionally filter by capcode. Common capcode prefixes:

- `SN` - Shoalhaven
- `SH` - Southern Highlands  
- `IS` - Illawarra South
- `IW` - Illawarra West

Example: `/pager #alerts SNSTGEO,SNHUSKI,SNBAWPO @FireAlerts`

Leave capcodes empty to receive ALL pager messages.

## Example Usage

```
# Set up RFS alerts in #fire-alerts channel, pinging @Firefighters role
/alert #fire-alerts rfs @Firefighters

# Set up BOM weather warnings in #weather channel
/alert #weather bom

# Set up pager feed for specific brigades
/pager #pager-feed SNSTGEO,SNHUSKI @OnCall

# Set up all pager messages (no filter)
/pager #all-pager
```

## Data Sources

This bot uses the AusAware API which aggregates data from:
- NSW Rural Fire Service (RFS)
- NASA FIRMS satellite hotspots
- Bureau of Meteorology (BOM)
- Live Traffic NSW
- Endeavour Energy / Ausgrid / Essential Energy
- AusNet / CitiPower / Powercor / Jemena (Victoria), SA Power Networks, Western Power
- VicEmergency (CFA/DEECA), QLD Fire Department, DFES (WA), SA CFS/MFS, NT Fire & Rescue
- The Wire (AusAware's own reporting: articles + fleet)
- User-submitted incidents & the radio scanner
- Pager feed data

## License

This project is for educational and community safety purposes.

