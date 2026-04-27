# nswpsn-api-node

Node/TypeScript rewrite of the NSW PSN backend, replacing
`backends/external_api_proxy.py`. Migration is strangler-fig — this
service runs alongside the Python backend on a different port (3001)
and Apache routes per-endpoint to whichever backend currently owns it.

## Status: W1 (scaffolding)

Right now this service answers two endpoints:

- `GET /api/health`
- `GET /api/config`
- (`GET /` returns a smoke-test banner)

Everything else still belongs to the Python backend on its production
port. As more routes get ported (per the W1-W9 plan), they'll land
here and the Apache config will flip a Location at a time.

## Quickstart

```bash
cd backends/node
npm install
npm run dev              # tsx watch on src/index.ts, reloads on save
# new terminal:
curl http://localhost:3001/api/health | jq
curl http://localhost:3001/api/config | jq
```

Run tests:

```bash
npm test
npm run typecheck
```

## Env

Looked up by `src/config.ts` (zod-validated). Defaults match Python's
defaults so a single `.env` can drive both backends.

| Var | Default | Notes |
|---|---|---|
| `PORT` | `3001` | Bind port |
| `NODE_ENV` | `dev` | `dev` \| `production` |
| `NSWPSN_API_KEY` | `nswpsn-live-2024-secure` | Same key Python uses |
| `DATABASE_URL` | _(unset)_ | Required from W2 onward |
| `LOG_LEVEL` | `info` | pino level |

## Capturing contract fixtures

Before starting any port, capture golden JSON from the live Python
backend so the contract test suite has ground truth to diff against:

```bash
API_BASE=https://api.forcequit.xyz \
NSWPSN_API_KEY=xxx \
npx tsx scripts/capture-fixtures.ts
```

Output goes to `test/fixtures/contract/` (gitignored).

## Project layout

```
src/
  index.ts            Entry: server + shutdown hooks
  server.ts           createApp() factory (testable)
  config.ts           zod-validated env
  api/                Route modules (one per endpoint group)
    health.ts
    config.ts
  db/
    pool.ts           pg.Pool wrapper (W2)
  lib/
    log.ts            pino logger
  types/              Shared TypeScript types
test/
  unit/               Fast tests, no DB
scripts/
  capture-fixtures.ts Pull golden JSON from prod
```

## Running under PM2 (production)

The `backends/ecosystem.config.js` (one level up) defines an `nswpsn-api-node`
app. Build first, then start:

```bash
npm run build
pm2 start ../ecosystem.config.js --only nswpsn-api-node
pm2 logs nswpsn-api-node
```

## See also

- `../external_api_proxy.py` — current Python backend (still authoritative)
- `../db.py` — current pg pool wrapper being replaced by `src/db/pool.ts`
- The migration plan that drove this design lives in the project chat
  history (W1 through W9 milestones).
