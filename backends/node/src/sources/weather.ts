/**
 * Weather sources:
 *   weather_current  - Open-Meteo batch lookup for ~100 NSW locations
 *   weather_radar    - RainViewer tile metadata, returned verbatim
 *
 * Mirrors the Python routes at external_api_proxy.py:6847 + 6999.
 */
import { fetchJson } from './shared/http.js';
import { registerSource } from '../services/sourceRegistry.js';
import { liveStore } from '../store/live.js';

const RADAR_URL = 'https://api.rainviewer.com/public/weather-maps.json';
const OPEN_METEO_BASE = 'https://api.open-meteo.com/v1/forecast';

export interface NswLocation {
  name: string;
  lat: number;
  lon: number;
}

/** Synced from Python's NSW_WEATHER_LOCATIONS at line 6862. */
export const NSW_WEATHER_LOCATIONS: NswLocation[] = [
  { name: 'Sydney CBD', lat: -33.8688, lon: 151.2093 },
  { name: 'Parramatta', lat: -33.8151, lon: 151.0011 },
  { name: 'Penrith', lat: -33.7506, lon: 150.6944 },
  { name: 'Campbelltown', lat: -34.065, lon: 150.8142 },
  { name: 'Liverpool', lat: -33.92, lon: 150.9256 },
  { name: 'Blacktown', lat: -33.7668, lon: 150.9054 },
  { name: 'Hornsby', lat: -33.7025, lon: 151.099 },
  { name: 'Manly', lat: -33.7969, lon: 151.2878 },
  { name: 'Cronulla', lat: -34.0587, lon: 151.152 },
  { name: 'Bankstown', lat: -33.9175, lon: 151.0355 },
  { name: 'Chatswood', lat: -33.7969, lon: 151.1803 },
  { name: 'Bondi', lat: -33.8915, lon: 151.2767 },
  { name: 'Richmond', lat: -33.5997, lon: 150.7517 },
  { name: 'Wollongong', lat: -34.4278, lon: 150.8931 },
  { name: 'Shellharbour', lat: -34.5809, lon: 150.87 },
  { name: 'Kiama', lat: -34.671, lon: 150.8544 },
  { name: 'Nowra', lat: -34.8808, lon: 150.6 },
  { name: 'Ulladulla', lat: -35.3583, lon: 150.4706 },
  { name: 'Batemans Bay', lat: -35.7082, lon: 150.1744 },
  { name: 'Central Coast', lat: -33.4245, lon: 151.3419 },
  { name: 'Newcastle', lat: -32.9283, lon: 151.7817 },
  { name: 'Maitland', lat: -32.733, lon: 151.559 },
  { name: 'Cessnock', lat: -32.834, lon: 151.356 },
  { name: 'Lake Macquarie', lat: -33.0333, lon: 151.6333 },
  { name: 'Port Stephens', lat: -32.7178, lon: 152.1122 },
  { name: 'Singleton', lat: -32.5697, lon: 151.1694 },
  { name: 'Muswellbrook', lat: -32.2654, lon: 150.8885 },
  { name: 'Katoomba', lat: -33.7139, lon: 150.3113 },
  { name: 'Springwood', lat: -33.6994, lon: 150.5647 },
  { name: 'Lithgow', lat: -33.4833, lon: 150.15 },
  { name: 'Bathurst', lat: -33.4193, lon: 149.5775 },
  { name: 'Orange', lat: -33.284, lon: 149.1004 },
  { name: 'Mudgee', lat: -32.5942, lon: 149.5878 },
  { name: 'Cowra', lat: -33.8283, lon: 148.6919 },
  { name: 'Young', lat: -34.3111, lon: 148.3011 },
  { name: 'Parkes', lat: -33.1306, lon: 148.1764 },
  { name: 'Forbes', lat: -33.3847, lon: 148.0106 },
  { name: 'Dubbo', lat: -32.2569, lon: 148.6011 },
  { name: 'Wellington', lat: -32.5558, lon: 148.9439 },
  { name: 'Narromine', lat: -32.2333, lon: 148.2333 },
  { name: 'Gilgandra', lat: -31.7097, lon: 148.6622 },
  { name: 'Coonamble', lat: -30.9544, lon: 148.3878 },
  { name: 'Nyngan', lat: -31.5611, lon: 147.1936 },
  { name: 'Cobar', lat: -31.4958, lon: 145.8389 },
  { name: 'Tamworth', lat: -31.0927, lon: 150.932 },
  { name: 'Armidale', lat: -30.513, lon: 151.669 },
  { name: 'Glen Innes', lat: -29.7333, lon: 151.7333 },
  { name: 'Tenterfield', lat: -29.0492, lon: 152.02 },
  { name: 'Inverell', lat: -29.7756, lon: 151.1122 },
  { name: 'Moree', lat: -29.4658, lon: 149.8456 },
  { name: 'Narrabri', lat: -30.3228, lon: 149.7836 },
  { name: 'Gunnedah', lat: -30.9833, lon: 150.25 },
  { name: 'Quirindi', lat: -31.5, lon: 150.6833 },
  { name: 'Walcha', lat: -31.0, lon: 151.6 },
  { name: 'Port Macquarie', lat: -31.4333, lon: 152.9 },
  { name: 'Kempsey', lat: -31.0833, lon: 152.8333 },
  { name: 'Coffs Harbour', lat: -30.2963, lon: 153.1157 },
  { name: 'Grafton', lat: -29.6908, lon: 152.9331 },
  { name: 'Ballina', lat: -28.8667, lon: 153.5667 },
  { name: 'Lismore', lat: -28.8133, lon: 153.275 },
  { name: 'Byron Bay', lat: -28.6433, lon: 153.615 },
  { name: 'Tweed Heads', lat: -28.1761, lon: 153.5414 },
  { name: 'Casino', lat: -28.8667, lon: 153.05 },
  { name: 'Maclean', lat: -29.45, lon: 153.2 },
  { name: 'Yamba', lat: -29.4333, lon: 153.35 },
  { name: 'Forster', lat: -32.1808, lon: 152.5172 },
  { name: 'Taree', lat: -31.9, lon: 152.45 },
  { name: 'Wagga Wagga', lat: -35.1082, lon: 147.3598 },
  { name: 'Albury', lat: -36.0737, lon: 146.9135 },
  { name: 'Griffith', lat: -34.2833, lon: 146.0333 },
  { name: 'Leeton', lat: -34.55, lon: 146.4 },
  { name: 'Narrandera', lat: -34.75, lon: 146.55 },
  { name: 'Temora', lat: -34.45, lon: 147.5333 },
  { name: 'Cootamundra', lat: -34.65, lon: 148.0333 },
  { name: 'Junee', lat: -34.8667, lon: 147.5833 },
  { name: 'Tumut', lat: -35.3, lon: 148.2167 },
  { name: 'Deniliquin', lat: -35.5333, lon: 144.95 },
  { name: 'Hay', lat: -34.5167, lon: 144.85 },
  { name: 'Finley', lat: -35.65, lon: 145.5667 },
  { name: 'Corowa', lat: -35.9833, lon: 146.3833 },
  { name: 'Cooma', lat: -36.2356, lon: 149.1245 },
  { name: 'Jindabyne', lat: -36.4167, lon: 148.6167 },
  { name: 'Thredbo', lat: -36.505, lon: 148.3069 },
  { name: 'Perisher', lat: -36.4, lon: 148.4167 },
  { name: 'Goulburn', lat: -34.7547, lon: 149.7186 },
  { name: 'Queanbeyan', lat: -35.3547, lon: 149.2311 },
  { name: 'Yass', lat: -34.8333, lon: 148.9167 },
  { name: 'Bega', lat: -36.6736, lon: 149.8428 },
  { name: 'Merimbula', lat: -36.8917, lon: 149.9083 },
  { name: 'Eden', lat: -37.0667, lon: 149.9 },
  { name: 'Bombala', lat: -36.9, lon: 149.2333 },
  { name: 'Broken Hill', lat: -31.9505, lon: 141.4533 },
  { name: 'Wilcannia', lat: -31.5558, lon: 143.3778 },
  { name: 'Bourke', lat: -30.0903, lon: 145.9378 },
  { name: 'Brewarrina', lat: -29.9667, lon: 146.85 },
  { name: 'Lightning Ridge', lat: -29.4333, lon: 147.9667 },
  { name: 'Walgett', lat: -30.0167, lon: 148.1167 },
  { name: 'Menindee', lat: -32.3939, lon: 142.4178 },
  { name: 'Ivanhoe', lat: -32.9, lon: 144.3 },
  { name: 'White Cliffs', lat: -30.85, lon: 143.0833 },
  { name: 'Tibooburra', lat: -29.4333, lon: 142.0167 },
  { name: 'Canberra', lat: -35.2809, lon: 149.13 },
];

/** WMO weather code -> [description, icon]. Synced with Python's
 *  WEATHER_CODES at line 6989. */
const WEATHER_CODES: Record<number, [string, string]> = {
  0: ['Clear', '☀️'],
  1: ['Mostly Clear', '🌤️'],
  2: ['Partly Cloudy', '⛅'],
  3: ['Overcast', '☁️'],
  45: ['Fog', '🌫️'],
  48: ['Rime Fog', '🌫️'],
  51: ['Light Drizzle', '🌧️'],
  53: ['Drizzle', '🌧️'],
  55: ['Heavy Drizzle', '🌧️'],
  61: ['Light Rain', '🌧️'],
  63: ['Rain', '🌧️'],
  65: ['Heavy Rain', '🌧️'],
  71: ['Light Snow', '❄️'],
  73: ['Snow', '❄️'],
  75: ['Heavy Snow', '❄️'],
  80: ['Light Showers', '🌦️'],
  81: ['Showers', '🌦️'],
  82: ['Heavy Showers', '⛈️'],
  95: ['Thunderstorm', '⛈️'],
  96: ['Thunderstorm + Hail', '⛈️'],
  99: ['Severe Storm', '⛈️'],
};

export interface WeatherFeature {
  type: 'Feature';
  geometry: { type: 'Point'; coordinates: [number, number] };
  properties: {
    name: string;
    temperature: number | null;
    feelsLike: number | null;
    humidity: number | null;
    windSpeed: number | null;
    windGusts: number | null;
    windDirection: number | null;
    precipitation: number;
    weatherCode: number;
    weatherDescription: string;
    weatherIcon: string;
  };
}

export interface WeatherSnapshot {
  type: 'FeatureCollection';
  features: WeatherFeature[];
}

function asNumOrNull(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  return null;
}

interface OpenMeteoCurrent {
  temperature_2m?: unknown;
  relative_humidity_2m?: unknown;
  apparent_temperature?: unknown;
  precipitation?: unknown;
  weather_code?: unknown;
  wind_speed_10m?: unknown;
  wind_direction_10m?: unknown;
  wind_gusts_10m?: unknown;
}

interface OpenMeteoEntry {
  current?: OpenMeteoCurrent;
}

export async function fetchWeatherCurrent(): Promise<WeatherSnapshot> {
  const lats = NSW_WEATHER_LOCATIONS.map((l) => l.lat).join(',');
  const lons = NSW_WEATHER_LOCATIONS.map((l) => l.lon).join(',');
  const url =
    `${OPEN_METEO_BASE}?latitude=${lats}&longitude=${lons}` +
    `&current=temperature_2m,relative_humidity_2m,apparent_temperature,` +
    `precipitation,weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m` +
    `&timezone=Australia%2FSydney`;

  const data = await fetchJson<unknown>(url, {
    headers: { 'User-Agent': 'Mozilla/5.0' },
  });
  const list: OpenMeteoEntry[] = Array.isArray(data)
    ? (data as OpenMeteoEntry[])
    : [data as OpenMeteoEntry];

  const features: WeatherFeature[] = [];
  for (let i = 0; i < NSW_WEATHER_LOCATIONS.length; i += 1) {
    const loc = NSW_WEATHER_LOCATIONS[i];
    if (!loc) break;
    if (i >= list.length) break;
    const entry = list[i];
    const cur = entry?.current;
    if (!cur) continue;

    const codeRaw = cur.weather_code;
    const code = typeof codeRaw === 'number' ? codeRaw : 0;
    const [desc, icon] = WEATHER_CODES[code] ?? ['Unknown', '❓'];

    const precipRaw = cur.precipitation;
    const precip = typeof precipRaw === 'number' ? precipRaw : 0;

    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [loc.lon, loc.lat] },
      properties: {
        name: loc.name,
        temperature: asNumOrNull(cur.temperature_2m),
        feelsLike: asNumOrNull(cur.apparent_temperature),
        humidity: asNumOrNull(cur.relative_humidity_2m),
        windSpeed: asNumOrNull(cur.wind_speed_10m),
        windGusts: asNumOrNull(cur.wind_gusts_10m),
        windDirection: asNumOrNull(cur.wind_direction_10m),
        precipitation: precip,
        weatherCode: code,
        weatherDescription: desc,
        weatherIcon: icon,
      },
    });
  }

  return { type: 'FeatureCollection', features };
}

export type WeatherRadarSnapshot = unknown;

export async function fetchWeatherRadar(): Promise<WeatherRadarSnapshot> {
  // RainViewer is US-hosted and the AU→US route flakes regularly. The
  // payload is tiny (~few KB), so a longer ceiling and a single retry
  // is much cheaper than dropping the snapshot every time.
  try {
    return await fetchJson<unknown>(RADAR_URL, { timeoutMs: 20_000 });
  } catch (err) {
    const msg = (err as Error).message ?? '';
    const transient =
      /ETIMEDOUT|ECONNRESET|ECONNREFUSED|ENOTFOUND|EAI_AGAIN|timeout|fetch failed/i.test(
        msg,
      );
    if (!transient) throw err;
    await new Promise((r) => setTimeout(r, 3_000));
    return fetchJson<unknown>(RADAR_URL, { timeoutMs: 30_000 });
  }
}

export default function register(): void {
  // Open-Meteo's free tier counts each (lat,lng) inside a multi-point
  // request as a separate API call against a per-location daily limit
  // (~10k/day). Polling every 5 min × 100 locations × 288 cycles =
  // 28800 calls/location/day — guaranteed 429 within ~8 hours. The
  // 30-min active / 60-min idle cadence below puts us at 4800/2400
  // calls/location/day, comfortably under the ceiling. Surface weather
  // for an emergency dashboard doesn't need sub-30-minute freshness.
  registerSource<WeatherSnapshot>({
    name: 'weather_current',
    family: 'misc',
    intervalActiveMs: 30 * 60_000,
    intervalIdleMs: 60 * 60_000,
    fetch: fetchWeatherCurrent,
  });
  registerSource<WeatherRadarSnapshot>({
    name: 'weather_radar',
    family: 'misc',
    intervalActiveMs: 300_000,
    intervalIdleMs: 600_000,
    fetch: fetchWeatherRadar,
  });
}

export function weatherCurrentSnapshot(): WeatherSnapshot {
  return (
    liveStore.getData<WeatherSnapshot>('weather_current') ?? {
      type: 'FeatureCollection',
      features: [],
    }
  );
}

export function weatherRadarSnapshot(): WeatherRadarSnapshot {
  return liveStore.getData<unknown>('weather_radar') ?? { radar: { past: [], nowcast: [] } };
}
