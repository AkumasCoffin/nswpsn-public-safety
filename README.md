# AusAware

A free, ad-free live view of what Australian emergency services are dealing with
right now. Deepest in NSW.

**Website:** https://nswpsn.forcequit.xyz

AusAware merges public data feeds — fire, traffic, power, weather, beaches,
aircraft, public transport — with publicly receivable, unencrypted radio and
pager traffic picked up off-air by volunteer-run receiver nodes. The result is a
map, a live feed, a searchable log, and a Discord bot.

It is live in production. There is no staging environment.

---

## What this is not

AusAware is **named after** the NSW Public Safety Network. It is not part of it.

There is no affiliation with any agency, government body or radio network. The
site monitors traffic that anyone with a receiver can already hear, and nothing
else.

## Honest limits

Read these before you read the feature list. They are not temporary.

- **Most police traffic on the network is encrypted and carries no audio.**
  AusAware receives what is transmitted in the clear. Radio coverage can never
  be complete, and no amount of work on this repository will change that.
- **Coverage depends on where contributors happen to live.** A receiver node
  hears what its antenna can hear. There is no coverage in an area where nobody
  has put a dongle on a roof, and the project cannot buy its way into one.
- **The project is partial and unfunded by design.** It runs out of pocket on
  donations. Features that would cost money recurring do not get built.
- **The API is scraper-hostile, not scraper-proof.** A static site cannot hold
  a secret, so pages carry a short-lived session token bound to their own
  browser instead of a key; curl, a typed URL or another site gets a 401.
  A determined scraper can still script the mint from one address, through
  one rate-limited, logged choke point. Scripted access is meant to go through
  named API keys, which work from the command line and are refused from web
  pages.
- **Several layers are off unless configured.** NASA FIRMS hotspots, the
  transcription tier, ntfy pushes, The Wire's media storage, TfNSW vehicle
  positions and the Discord management dashboard are each gated on an
  environment variable. What an unset variable does varies by route — most
  answer 503 or return an empty snapshot, but some report their own
  unconfigured state with a 200 instead (`/api/whisper/status` returns
  `configured: false`). See `backends/node/src/config.ts`, and the component
  doc for the layer you care about.
- **Waze is gone.** It was retired as a source and then removed: no ingest, no
  routes, and migrations `071`/`074` dropped the heatmap and archive tables.
  Plenty of dead references survive in both the frontend and the backend —
  including three `map.html` fetches that cannot be answered. They are
  leftovers, not a feature. See
  [`docs/architecture.md`](docs/architecture.md#waze-is-gone-and-the-data-with-it)
  before acting on a `waze` grep hit.

## The stack

| Part | Built with | Lives in | Entry point |
|---|---|---|---|
| Backend API | TypeScript, Hono, `pg`, zod, Vitest (`engines.node >= 20`) | `backends/node` | `backends/node/src/index.ts` |
| Public site | vanilla HTML/CSS/JS + Leaflet, **no build step** | repo root | any `*.html` file |
| Discord bot | Python 3.10+, discord.py | `discord-bot` | `discord-bot/bot.py` |
| Radio node | Go 1.26 | `feeder-nodes/radio-node` | `cmd/nodeagent/main.go` |
| Pager node | Go 1.26 | `feeder-nodes/pager-node` | `cmd/nodeagent/main.go` |
| Aircraft node | Go 1.26 | `feeder-nodes/aircraft-node` | `cmd/nodeagent/main.go` |
| Wire link-unfurl worker | Cloudflare Worker, vanilla JS | `workers/wire-embed` | `worker.js` |
| Database | PostgreSQL, 119 numbered migrations | `backends/node/src/db/migrations` | run by `src/db/migrate.ts` |

The repo root **is** the webroot. `.htaccess` serves it, gives clean URLs
(`/live` for `/live.html`), and blocks `backends/`, `discord-bot/`, `workers/`,
dotfiles and `.md` files from being served.

Supabase holds **authentication and identity only**. PostgreSQL is the canonical
store for everything else.

**There is no Python/Flask backend.** It was deleted months ago and replaced by
`backends/node`. If you find a document that describes one, that document is
wrong.

## What you can look at

| Page | What it is |
|---|---|
| `/map` | The incident map. Fire, traffic, power, weather, pager pins, aircraft, public transport, receiver-monitored P25 repeater sites |
| `/live` | Live dashboard — per-source counts, hourly radio summaries, news |
| `/logs` | Searchable historical log, read from the archive tables |
| `/wire` | The Wire — news and media posts |
| `/feeds` | Direct Feeds — listen-live radio and pager streams |
| `/data-sources` | What is being ingested, and from where |
| `/agency` | Reference pages for Fire & Rescue NSW, NSW Rural Fire Service, NSW Ambulance and aviation callsigns |
| `/feeder` | Run a receiver node — enrolment and installer download |
| `/dashboard` | Discord-OAuth management UI for the bot's alert presets |
| `/staff` | Role-gated operations surface: nodes, receptions, decode health, coverage |

## What is ingested

**36 polled sources** run on a default boot, out of 38 declared in a registry
(`backends/node/src/services/sourceRegistry.ts`), each with its own poll cadence
and its own archive family. Registration happens at boot
(`src/sources/registerAll.ts`, `src/sources/registerPower.ts`) and
`src/services/poller.ts` schedules each one.

| Provider | Data | Cadence |
|---|---|---|
| NSW Rural Fire Service | Active bush and grass fires | 60s |
| Bureau of Meteorology | Weather warnings, land and marine | 60s |
| LiveTraffic NSW | Seven hazard kinds — incidents, roadwork, flood, fire, major events, alpine, council-submitted | 60s–5m |
| Endeavour Energy | Current, planned and maintenance outages | 60s–5m |
| Ausgrid | Outages, with per-outage detail enrichment | **off** — upstream has 404'd for months |
| Essential Energy | Current and future outages | 3m |
| Pagermon (self-hosted) | Pager messages | 60s |
| ACT Ambulance | Incidents | 60s |
| QLD, VIC, WA, SA, NT fire and warning feeds | Incidents and warnings | 2–5m |
| QLD traffic + flood cameras | Camera stills | 60s |
| NSW Beachwatch / Beachsafe | Water quality and beach conditions | 10m |
| NASA FIRMS | Satellite fire hotspots | 15m |
| Aviation cameras | Camera stills | 5m |
| Weather / rain radar | Current conditions, radar frames | 5m–30m |
| Public ADS-B aggregators | Aircraft positions | 8s |
| Community editors | Manually added incidents | 60s |

Plus layers that are not registry sources: AnyTrip and official TfNSW
GTFS-Realtime public transport (`src/sources/tfnsw.ts`), MarineTraffic AIS,
Central Watch cameras (both via a headless Chromium worker), and news RSS.

And the part no public API provides: **off-air radio and pager traffic**, from
the feeder-node fleet.

## The feeder-node fleet

Volunteers run a small Go agent on a machine with an SDR dongle. Three kinds:

| Node | Decodes | Needs |
|---|---|---|
| **radio** | P25 trunked voice (and DMR, and AM/airband), via a forked SDR-Trunk | RTL-SDR or better; Linux or Windows |
| **pager** | POCSAG pages, via `rtl_fm | multimon-ng` | RTL-SDR; Linux (incl. Raspberry Pi) |
| **aircraft** | ADS-B Mode S at 1090 MHz, via `dump1090-fa` | RTL-SDR; Linux (incl. Raspberry Pi) |

All three enrol with a single-use code, hold a persistent WebSocket to the
backend, and buffer to a disk-backed queue so a dropped connection loses
nothing. They also carry a self-update path driven by a version manifest
(`backends/node/assets/node-versions.json`) — though for the agents themselves
that path is **dormant**, because an entry with an empty sha256 is treated as
nothing to do and all three agent entries currently have one. See
[`docs/components/forked-runtimes.md`](docs/components/forked-runtimes.md#self-update-is-dormant-for-the-agents).

A radio node additionally **downloads and runs two forked upstream projects** —
a headless SDR-Trunk build and a matching rdio-scanner — both pinned by version
and sha256 in that manifest. See
[`docs/components/forked-runtimes.md`](docs/components/forked-runtimes.md).

If you already run your own rdio-scanner and would rather not install anything,
one downstream entry is enough: [`docs/scanner-feed-setup.md`](docs/scanner-feed-setup.md).

## Vocabulary

**Ingested radio traffic is a *reception*, never a "call".** Several receivers
hear one transmission; each hearing is a reception, and the backend groups them
back into one logical event. "Call" is kept only when quoting rdio-scanner's own
endpoint and table names.

## Where to start

| You want to | Read |
|---|---|
| Work on this repo at all | **[`OPERATING-RULES.md`](OPERATING-RULES.md)** — the operating rules. Not optional |
| Understand the whole system | [`docs/architecture.md`](docs/architecture.md) |
| Get set up and ship a change | [`CONTRIBUTING.md`](CONTRIBUTING.md) |
| Work on one component | [`docs/components/`](docs/components/) |
| Run a receiver | [`docs/components/radio-node.md`](docs/components/radio-node.md) and its siblings |

### The rules, in four lines

- **Never deploy and never restart anything.** Work finishes at the commit on
  `dev-beta`. The owner deploys.
- **`dev-beta` is permanent and is never merged into `main`** by anyone but the
  owner.
- **No AI attribution in commit messages.**
- **A reception is not a "call".**

The full set, with the reasoning, is in [`OPERATING-RULES.md`](OPERATING-RULES.md).

## Quick start

```bash
git clone https://github.com/AkumasCoffin/nswpsn-public-safety.git
cd nswpsn-public-safety
git checkout dev-beta

# Backend
cd backends/node && npm install && npm run dev
curl http://localhost:3000/api/health

# Public site — nothing to build. Serve the repo root, or open a page.
cp config.sample.js config.js   # git-ignored; fill in Supabase + API values
```

Environment variables are declared and validated in one place:
`backends/node/src/config.ts`. Almost all are optional — an unset variable turns
its feature off rather than breaking startup. `backends/env.sample` is the
annotated template.

## Documentation

```
OPERATING-RULES.md                  the operating rules (prohibitions)
CONTRIBUTING.md                     setup, workflow, which check to run
docs/
  architecture.md                   the whole system, end to end
  components/
    backend.md                      backends/node/src/index.ts
    public-site.md                  the buildless static pages
    discord-bot.md                  discord-bot/bot.py
    radio-node.md                   feeder-nodes/radio-node/cmd/nodeagent
    pager-node.md                   feeder-nodes/pager-node/cmd/nodeagent
    aircraft-node.md                feeder-nodes/aircraft-node/cmd/nodeagent
    forked-runtimes.md              four forks and one original plugin set
    transcription.md                the Whisper router and its two servers
  scanner-feed-setup.md             contribute an existing rdio-scanner
  uptime-kuma-monitors.md           monitor recipes against /api/status
feeder-nodes/radio-node/docs/       SDR-Trunk + rdio config references
```

## Project layout

```
.
├── *.html                      static pages — the repo root is the webroot
├── styles.css  logs.css        shared styles
├── auth-common.js              Supabase auth helpers
├── map-editor-module.js        map editor (bump ?v= in map.html when it changes)
├── analytics.js                Umami event tracking
├── config.sample.js            frontend config template (copy to config.js)
├── .htaccess                   clean URLs, security headers, path blocks
├── agency-data.json            agency directory agency.html reads
├── assets/  data/  shared/     icons, geo + reference datasets, alert catalog
│
├── backends/
│   ├── node/                   THE backend — see docs/components/backend.md
│   │   ├── src/index.ts        entry point
│   │   ├── src/server.ts       Hono app factory
│   │   ├── src/config.ts       zod-validated env — the only process.env reader
│   │   ├── src/api/            one route module per endpoint group
│   │   ├── src/sources/        one module per polled upstream
│   │   ├── src/services/       pollers, node hub, whisper router, LLM, …
│   │   ├── src/store/          LiveStore (memory) + ArchiveWriter (Postgres)
│   │   ├── src/db/migrations/  119 numbered SQL migrations
│   │   ├── assets/             node-versions.json — the agent/component manifest
│   │   └── scripts/deploy.sh   the deploy. The owner runs this. You do not
│   ├── env.sample              annotated environment template
│   ├── prompts/                LLM prompts for the hourly radio summaries
│   └── ecosystem.config.js     STALE AND UNUSED — never cite it
│
├── discord-bot/                Python bot — see docs/components/discord-bot.md
├── feeder-nodes/               three Go modules, one per node kind
├── workers/wire-embed/         Cloudflare Worker for Wire link previews
├── scripts/                    one-off dataset builders (Python + mjs)
└── docs/                       the documentation above
```

## License

GNU General Public License, version 3. See [`LICENSE`](LICENSE).
