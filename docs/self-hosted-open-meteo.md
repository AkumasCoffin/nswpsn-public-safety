# Self-hosted Open-Meteo

Why this exists: the public Open-Meteo API caps at ~10,000 location-calls a
day, which forces the weather field onto a 0.5° (~55 km) grid — the blocky
look. A self-hosted instance has **no rate limits**, serves the **same
endpoints**, and needs no model-sync pipeline: the container reads Open-Meteo's
public S3 data distribution on demand and keeps a local LRU cache. Switching
the backend to it is a `.env` change, nothing more.

## 1. Run the container

On the Docker VM (not the Postgres VM). `docker-compose.yml`:

```yaml
services:
  open-meteo:
    image: ghcr.io/open-meteo/open-meteo
    container_name: open-meteo
    restart: unless-stopped
    environment:
      # Pull model data on demand from Open-Meteo's public mirror and cache
      # locally — no sync cron, no bulk download.
      REMOTE_DATA_DIRECTORY: https://openmeteo.s3.amazonaws.com/data/
      # The local LRU cache. Bigger = fewer S3 round-trips on cold variables.
      # 8GB is comfortable for the AU grids; raise it if the disk allows.
      CACHE_SIZE: 8GB
    volumes:
      - open-meteo-data:/app/data
    ports:
      # Bind to the VM's LAN address if the backend is on another host —
      # 127.0.0.1 only works when both share the machine.
      - "8081:8080"

volumes:
  open-meteo-data:
```

```bash
docker compose up -d
```

## 2. Smoke test

```bash
# Same API as the public service. bom_access_global is the Bureau's own model.
curl 'http://127.0.0.1:8081/v1/forecast?latitude=-33.87&longitude=151.21&hourly=temperature_2m&models=bom_access_global'
```

- A JSON forecast ⇒ working. First calls are slower (cold cache) — that is the
  S3 fetch; repeats are instant.
- An error naming invalid models lists the valid names — that error message is
  the authoritative model catalogue for your instance.
- Also check the endpoints the backend uses: swap the path for
  `/v1/marine`, `/v1/air-quality`, `/v1/flood`, `/v1/elevation`. If
  `/v1/elevation` is not served by the image, leave
  `OPEN_METEO_ELEVATION_URL` on the public API — the land/sea mask is a
  one-time ~1,500 calls and fits the free tier trivially.

## 3. Point the backend at it

In the backend `.env` (then deploy as usual):

```ini
# The self-hosted instance (adjust host to the Docker VM's address)
OPEN_METEO_FORECAST_URL=http://10.1.0.X:8081/v1/forecast
OPEN_METEO_MARINE_URL=http://10.1.0.X:8081/v1/marine
OPEN_METEO_AIR_URL=http://10.1.0.X:8081/v1/air-quality
OPEN_METEO_FLOOD_URL=http://10.1.0.X:8081/v1/flood
OPEN_METEO_ELEVATION_URL=http://10.1.0.X:8081/v1/elevation

# No rate limit on our own instance: 0 disables the per-minute pacer.
WEATHER_LOCATIONS_PER_MIN=0

# ~16 km grid (the BOM ACCESS-G class). 281x227 = 63,787 cells — trivial
# against localhost. 0.1 (~11 km) also works if the cache disk is generous.
WEATHER_GRID_STEP=0.15
WEATHER_MARINE_STEP=0.5
WEATHER_AIR_STEP=0.75

# Refresh every 3 hours — the models themselves update at that cadence, so
# daily was leaving most model runs unseen.
WEATHER_GRID_INTERVAL_MS=10800000
```

Delete the old grids once after switching, so the finer dataset is not blocked
by the freshness guard holding the coarse one:

```bash
rm -f /var/www/nswpsn/backends/node/state/weather/g_*.bin
rm -f /var/www/nswpsn/backends/node/state/weather/mask-*.bin
rm -f /var/www/nswpsn/backends/node/state/weather/manifest.json
```

(Deleting the manifest is what forces the rebuild; the geometry changed, so
the old mask and grids are for a different grid anyway.)

## 4. What to expect

- Boot log: `weather grid: self-hosted Open-Meteo upstream, no quota applies`,
  with the new cell count.
- First land refresh takes a few minutes (cold S3 cache), later ones seconds.
- The manifest reports the 0.15° geometry and the field visibly tightens;
  the client's upsample factor steps itself down for the denser grid.
- Disk on the VM: the LRU cache grows to CACHE_SIZE and stays there.

## If it misbehaves

- 404/“model not found” → that model is not in the S3 mirror under that name;
  use the error's list of valid names.
- Slow every time (never warms) → CACHE_SIZE too small for the variables in
  play; raise it.
- The backend still shows free-tier log lines → the `.env` URLs did not take;
  check `pm2 env` for the running process.
