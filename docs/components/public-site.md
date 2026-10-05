# Public site — the repo root

**Entry point: any `*.html` file in the repository root.** There is no single
one. `index.html` is the home page; `map.html` is the product.

Vanilla HTML, CSS and JavaScript with Leaflet. **No build step.** The repo root
**is** the webroot — Apache serves these files directly, and a page you open from
the filesystem is the same page production serves.

Read [`../../AGENTS.md`](../../AGENTS.md) before changing anything here.

---

## The constraint

**Do not add a bundler, a framework, a transpiler, a `package.json` or a
toolchain to the site, and do not document one.**

This is a deliberate constraint, not an oversight. A page is an `.html` file you
can open in a browser. There is no `npm run build`, no `dist/`, and nothing to
compile. A contributor who can read HTML can fix a page.

Consequences you have to work with:

- Logic lives in inline `<script>` blocks and a handful of plain `.js` files at
  the root. No modules bundling, no imports across files beyond `<script src>`.
- Some pages are large. `map.html` is about 960 KB, `staff.html` about 920 KB,
  `logs.html` and `live.html` about 250 KB each. That is the cost of the
  constraint and it is accepted.
- CI parses every classic inline `<script>` in every root `.html` file
  (`.github/workflows/tests.yml`). A syntax error anywhere in a 960 KB page
  fails the build, which is the main thing standing in for a compiler.

## Pages

| URL | File | What it is |
|---|---|---|
| `/` | `index.html` | Home |
| `/map` | `map.html` | The incident map. Every live layer |
| `/live` | `live.html` | Live dashboard — per-source counts, hourly radio summaries, news |
| `/logs` | `logs.html` | Searchable historical log, read from the archive |
| `/wire` | `wire.html` | The Wire — news and media posts |
| `/wire-compose` | `wire-compose.html` | Compose a Wire post |
| `/feeds` | `feeds.html` | Direct Feeds — listen-live radio and pager streams |
| `/data-sources` | `data-sources.html` | What is ingested, and from where |
| `/agency` | `agency.html` | Agency reference — reads `agency-data.json` |
| `/feeder` | `feeder.html` | Run a receiver node: enrolment, installer download |
| `/join` | `join.html` | "Feed the map" — the contributor pitch |
| `/fleet-compose` | `fleet-compose.html` | Fleet composer for The Wire |
| `/staff` | `staff.html` | Role-gated operations surface |
| `/dashboard` | `dashboard.html` | Discord-OAuth management UI for the bot |
| `/login`, `/signup`, `/profile` | … | Accounts |
| `/verify-email`, `/reset-password`, `/change-password` | … | Account flows |
| `/about`, `/contact`, `/links`, `/privacy`, `/terms` | … | Static content |
| `/map-editor` | `map-editor.html` | **A redirect stub.** The editor merged into `map.html`, which enables editor mode automatically when a signed-in user has the role. The stub only keeps old links working |

## Shared files

| File | Purpose |
|---|---|
| `styles.css` | Shared styles |
| `logs.css` | Styles specific to the log browser |
| `auth-common.js` | Supabase auth helpers |
| `map-editor-module.js` | The map editor, loaded by `map.html` |
| `analytics.js` | Umami event tracking |
| `scroll-indicator.js`, `ui-dialogs.js`, `idle-guard.js` | UI helpers |
| `agencies.js`, `fire-vocab.js`, `fleet-vocab.js`, `livetraffic-vocab.js` | Vocabulary and label tables |
| `config.sample.js` | Config template — copy to `config.js` |
| `agency-data.json` | Agency directory `agency.html` reads |
| `assets/`, `data/`, `shared/` | Icons, geo and reference datasets, the alert catalog |

## `config.js`

`config.js` is **git-ignored** so each deployment keeps its own values. Copy the
template and fill it in:

```bash
cp config.sample.js config.js
```

It carries the Supabase project URL and anon key, the API base URL, and the API
key. `.htaccess` explicitly allows `config.js`, `auth-common.js` and
`analytics.js` to be served while blocking most other paths.

## The cache-busting rule

**If `map-editor-module.js` changes, bump the `?v=` on its script tag in
`map.html` in the same commit.** The tag is at `map.html:20689`:

```html
<script src="map-editor-module.js?v=43"></script>
```

`.htaccess` sets `no-cache, must-revalidate` on `.html`, `.css` and `.js`, which
is why that query parameter is still needed: the failure mode is a *cached*
`map-editor-module.js` paired with a *fresh* `map.html`, and the version
parameter is what breaks the pairing. This went wrong once and served a
pre-fix editor build to phones for days.

## `.htaccess`

Apache rules at the repo root. It:

- disables directory browsing and serves `index.html` by default;
- gives **clean URLs** — `/live` serves `live.html`, and `/live.html`
  canonically redirects to `/live`;
- blocks `backends/`, `discord-bot/`, `workers/`, `backup/`, `repo_stuff/`,
  `ref_data/` outright;
- blocks dotfiles, and every `.env`, `.md`, `.py`, `.db`, `.sqlite`, `.json` and
  `.sample` file — with named exceptions at `.htaccess:54` for
  `agency-data.json`, `agency-extended.json` and `lga-regions.json`. Only two of
  those three are real: `agency-data.json` is at the repo root and
  `lga-regions.json` is at `data/boundaries/lga-regions.json` (the rule matches
  on filename, so it applies there too). **`agency-extended.json` no longer
  exists.** The extended agency tables are now served live from the CSV source
  of truth by `GET /api/agency/extended`, which is public-exempt
  (`backends/node/src/services/auth/apiKey.ts:58`) because the agency page needs
  no login; `agencies.js:314` fetches that route and falls back to an empty
  `{ agencies: {} }`, never to a file. The `.htaccess` exception and the
  comments naming the static file in `agencies.js:3`, `:294`, `:312` and
  `agency.html:434` are leftovers;
- blocks `config.sample.js`, `ecosystem.config.js`, `README.md`,
  `package.json`;
- sets `Cache-Control: no-cache, must-revalidate` on `.html`, `.css` and `.js`.

Documentation under `docs/` is therefore **not** reachable over the web — the
`.md` block covers it. That is intentional; docs are for people reading the
repository.

## How a page gets data

Every page talks to the backend over `/api/...` at the API base URL from
`config.js`, authenticating with the key `/api/config` hands out. Nothing on the
public site reads a database directly.

```
map.html  ──►  /api/rfs/incidents        (RFS fires)
          ──►  /api/traffic/*            (seven hazard kinds + cameras)
          ──►  /api/bom/warnings         (weather)
          ──►  /api/endeavour/*, /api/ausgrid/outages, /api/essential/*
          ──►  /api/pager/hits           (pager pins — map.html:6265)
          ──►  /api/firms/hotspots       (satellite hotspots)
          ──►  /api/adsb/aircraft, /api/adsb/trails
          ──►  /api/transport/*          (public transport)
          ──►  /api/radio/grn-sites      (repeater site geography — 18474)
          ──►  /api/radio/monitored-sites  (repeater "Monitoring" badges — 18451)
          ──►  /api/incidents            (community-added)

live.html ──►  per-source counts, /api/summaries/latest, /api/news/rss,
               /api/stats/history, /api/heartbeat
```

**The repeater layer is two endpoints, and neither one carries a reception.**
`/api/radio/grn-sites` is the *geography* — the GRN site list, served from the
`grn_sites` table (`backends/node/src/api/radio-public.ts:246`), seeded once at
boot from `data/nswpsn/NSW GRN Version 1.json` when the table is empty
(`index.ts:204`), cached 30s. `/api/radio/monitored-sites` is the *liveness* —
which of those sites the fleet is hearing right now. A site badge going green
means a receiver is locked to that repeater, not that anything was said on it.

That dataset is also **editable live by the owner**, which is not something a
new contributor would guess: `PATCH /api/radio/grn-sites/:id`
(`radio-public.ts:287`) is gated by `requireRole(isOwner)` and the body is
constrained to the dataset's own 16 keys by `GrnSitePatchSchema`
(`radio-public.ts:268`). `map.html:18602` is that editor, and it authenticates
with the signed-in person's Supabase JWT rather than the shared site key —
the backend owner-gates on verified identity, not on possession of the key.

```
logs.html ──►  /api/data/history, /api/data/history/{filters,sources,stats},
               /api/pager/hits

staff.html ─►  WebSocket /api/node-ws/staff  (auth: first message carrying a
               Supabase JWT — a browser cannot set headers on a WS handshake;
               owner or dev only)
          ──►  /api/node-data/*  (decode health, per-site decode, coverage,
               ADS-B series, pager overview, capcodes, …)
```

`map.html` also still fetches `/api/waze/police-heatmap` and `/api/waze/police`.
**Those routes do not exist.** Waze was removed and its tables dropped
(migrations `071`, `074`). The fetches are leftovers.

## Auth on the site

Public auth is Supabase (`auth-common.js`). A signed-in user's JWT is what the
backend role-checks for editor mode, the staff surface and The Wire's authoring
pages.

The Discord management dashboard (`dashboard.html`) is a separate flow — Discord
OAuth with its own session cookie, which is why the backend's CORS config sets
`credentials: true` and the allowlist is an explicit list of exact origins rather
than a wildcard.

**An account with no roles and no signup request is a normal public user.** Never
write logic that treats either absence as a reason to delete an account. That
heuristic once deleted real people.

## The Wire's link previews

`workers/wire-embed/worker.js` is a Cloudflare Worker on
`nswpsn.forcequit.xyz/wire*` (`wrangler.toml`). The site is static behind
Cloudflare, so a social crawler fetching a shared post URL only ever sees the
generic page — it does not run the JS that renders the post, so links never
unfurl.

The Worker passes real browsers straight through untouched. A crawler asking for
a specific post (`?post=<id>` or `?article=<slug>`) gets the same origin HTML
with its `<head>` rewritten from `/api/wire/og/...`. No secrets are involved —
that endpoint returns only Open Graph fields of already-published posts. If
anything fails it serves the untouched origin response.

The route pattern deliberately over-matches (`/wire`, `/wire-compose`,
`/wire/...`), because everything except a crawler with a post parameter passes
through anyway.

## Changing a page

1. Edit the `.html` file. Match the surrounding style.
2. Confirm the inline scripts still parse — CI runs exactly that check.
3. If you touched `map-editor-module.js`, bump the `?v=` in `map.html` in the
   same commit.
4. Commit on `dev-beta`. Changes are live when the **owner** deploys. You do not
   deploy, and you do not restart anything.

## Vocabulary

Ingested radio traffic is a **reception**, never a "call". That applies to UI
copy as much as to code.

Never imply the site is part of the NSW Public Safety Network or affiliated with
any agency. It is *named after* that network and monitors publicly receivable,
unencrypted traffic. Nothing in the copy may read otherwise.

## See also

- [`../architecture.md`](../architecture.md) — where the data comes from.
- [`backend.md`](backend.md) — the API every page calls.
