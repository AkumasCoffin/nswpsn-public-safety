# NSW PSN Alert Discord Bot

A Discord bot that provides real-time alerts for NSW emergency services, BOM warnings, traffic incidents, power outages, and pager messages.

**Website:** [nswpsn.forcequit.xyz](https://nswpsn.forcequit.xyz/)

## Features

- 🔥 **RFS Incidents** - Bush fire alerts from NSW Rural Fire Service
- ⛈️ **BOM Warnings** - Weather warnings (severe weather, marine, and general)
- 🚗 **Traffic Alerts** - Incidents, roadwork, floods, fires, and major events
- ⚡ **Power Outages** - Endeavour Energy and Ausgrid outages
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

| Type | Description |
|------|-------------|
| `rfs` | RFS bush fire incidents |
| `bom` | BOM weather warnings (land, marine, and general) |
| `traffic_incidents` | Traffic incidents (crashes, hazards) |
| `traffic_roadwork` | Road work alerts |
| `traffic_flood` | Flood hazards |
| `traffic_fire` | Fire-related road hazards |
| `traffic_major` | Major events affecting traffic |
| `power_endeavour` | Endeavour Energy power outages |
| `power_ausgrid` | Ausgrid power outages |

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

This bot uses the NSW PSN API which aggregates data from:
- NSW Rural Fire Service (RFS)
- Bureau of Meteorology (BOM)
- Live Traffic NSW
- Endeavour Energy
- Ausgrid
- Pager feed data

## License

This project is for educational and community safety purposes.

