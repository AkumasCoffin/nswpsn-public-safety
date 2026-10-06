# Self-hosted Open-Meteo

Why this exists: the public Open-Meteo API caps at ~10,000 location-calls a
day, which forces the weather field onto a 0.5° (~55 km) grid — the blocky
look. A self-hosted instance has **no rate limits**, serves the **same
endpoints**, and needs no model-sync pipeline: the container reads Open-Meteo's
public S3 data distribution on demand and keeps a local LRU cache. Switching
the backend to it is a `.env` change.

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
      # Local LRU cache. Bigger = fewer S3 round-trips on cold variables.
      CACHE_SIZE: 8GB
    volumes:
      - open-meteo-data:/app/data
    ports:
      # Bind reachable from the backend VM, not just localhost.
      - "8081:8080"

volumes:
  open-meteo-data:
```

## 2. THE MODEL MATTERS — do not use the default, and do not use BOM

Verified against the S3 mirror on 2026-10-07:

| Model | State | Notes |
|---|---|---|
| `bom_access_global` | **DEAD — data ends 2025-06-27** | The mirror stopped updating it. Every value returns null, nothing errors. |
| `ukmo_global_deterministic_10km` | current, hourly | **Use this.** 10 km — Windy-class. |
| `ncep_gfs013` | current, hourly | 13 km fallback |
| `ecmwf_ifs025` | current, 3-hourly | 25 km fallback |
| `ecmwf_wam025` / `meteofrance_wave` | current | marine |
| `cams_global` | current | air quality |
| GloFAS (flood) | **not mirrored** | flood stays on the public API |

Because the server's *default* model choice for Australia can land on the dead
BOM dataset, the backend must pin models explicitly — that is what
`OPEN_METEO_FORECAST_MODELS` is for (below). An all-null response with no
error is the signature of a stale model.

## 3. Smoke test

```bash
curl 'http://127.0.0.1:8081/v1/forecast?latitude=-33.87&longitude=151.21&hourly=temperature_2m&models=ukmo_global_deterministic_10km'
```

- Real numbers ⇒ working. All nulls ⇒ stale model (see table).
- An error naming invalid models lists every valid name — that error is the
  authoritative catalogue for your instance.
- First calls are slow (cold S3 cache); repeats are instant.
- Also probe `/v1/marine`, `/v1/air-quality`, `/v1/elevation` the same way.

## 4. Point the backend at it

Backend `.env` (then deploy as usual):

```ini
OPEN_METEO_FORECAST_URL=http://<docker-vm>:8081/v1/forecast
OPEN_METEO_MARINE_URL=http://<docker-vm>:8081/v1/marine
OPEN_METEO_AIR_URL=http://<docker-vm>:8081/v1/air-quality
OPEN_METEO_ELEVATION_URL=http://<docker-vm>:8081/v1/elevation
# Flood is NOT in the S3 mirror — leave it on the public API (737 cells/day,
# comfortably inside the free tier on its own):
#   OPEN_METEO_FLOOD_URL stays unset

# Pin the models — mandatory, see section 2.
OPEN_METEO_FORECAST_MODELS=ukmo_global_deterministic_10km
OPEN_METEO_MARINE_MODELS=ecmwf_wam025
OPEN_METEO_AIR_MODELS=cams_global

# No rate limit on our own instance: 0 disables the per-minute pacer.
WEATHER_LOCATIONS_PER_MIN=0

# 0.1 degrees (~11 km) matches the model class. 421x341 = 143,561 cells —
# trivial against localhost; ~200 MB of grids on the backend's disk.
WEATHER_GRID_STEP=0.1
WEATHER_MARINE_STEP=0.5
WEATHER_AIR_STEP=0.75

# Refresh every 3 hours — the models update at that cadence, and daily was
# leaving most runs unseen.
WEATHER_GRID_INTERVAL_MS=10800000
```

Then wipe the old grids once, so the freshness guard does not hold the coarse
dataset against the new geometry:

```bash
rm -f /var/www/nswpsn/backends/node/state/weather/g_*.bin \
      /var/www/nswpsn/backends/node/state/weather/mask-*.bin \
      /var/www/nswpsn/backends/node/state/weather/manifest.json
```

## 5. What to expect

- Boot log: `weather grid: self-hosted Open-Meteo upstream, no quota applies`
  with the new cell count.
- First land refresh takes minutes (cold cache); later ones seconds.
- The manifest reports the 0.1° geometry; the client steps its upsample factor
  down automatically for the denser grid.
- If a layer suddenly goes all-null weeks from now: check that model's
  `data_end_time` in `https://openmeteo.s3.amazonaws.com/data/<model>/static/meta.json`
  — a mirrored model can stop updating, which is exactly what happened to BOM.

## If it misbehaves

- All nulls, no error → stale or wrong model; pin a current one (section 2).
- "model not found" error → the error's own list is the valid catalogue.
- Slow on every request → CACHE_SIZE too small; raise it.
- Backend still logs free-tier lines → the `.env` URLs did not take; check
  `pm2 env` on the running process.
