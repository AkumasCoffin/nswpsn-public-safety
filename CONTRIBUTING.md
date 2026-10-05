# Contributing to AusAware

**Read [`AGENTS.md`](AGENTS.md) first.** It holds the operating rules for this
repository and they are prohibitions, not advice. This file covers how to get
set up and how a change travels; it does not restate the rules.

AusAware is live in production and there is no staging environment. That shapes
everything below.

## The four that matter most

- **Never deploy. Never restart anything.** Your work finishes at the commit on
  `dev-beta`. The owner deploys, on their own timing.
- **`dev-beta` is the working branch.** It is permanent, it is never deleted,
  and it is never merged into `main` by anyone but the owner.
- **No AI attribution in commit messages**, in the first commit or any later
  one.
- **Ingested radio traffic is a *reception*, never a "call".**

## There are two ways to contribute

**Run a receiver.** You do not need to write code. If you can put an RTL-SDR
dongle on a roof somewhere in Australia, you can run a feeder node and the
coverage map gets better where you live. Start at
[`docs/components/radio-node.md`](docs/components/radio-node.md),
[`docs/components/pager-node.md`](docs/components/pager-node.md) or
[`docs/components/aircraft-node.md`](docs/components/aircraft-node.md). If you
already run your own rdio-scanner, [`docs/scanner-feed-setup.md`](docs/scanner-feed-setup.md)
needs nothing installed at all — it is one entry in your rdio admin page.

**Write code.** Read on.

## Getting oriented

1. [`README.md`](README.md) — what AusAware is and what it honestly cannot do.
2. [`docs/architecture.md`](docs/architecture.md) — the whole system, and the
   full trace from a transmission in the air to a pin on the live map.
3. [`docs/components/`](docs/components/) — one short doc per component. Each
   names the file execution starts from.

## Local setup

### Backend

```bash
cd backends/node
npm install
cp ../env.sample .env   # fill in what you need; almost everything is optional
npm run dev             # tsx watch on src/index.ts
curl http://localhost:3000/api/health
```

Environment is validated by zod at startup —
`backends/node/src/config.ts` is the only place `process.env` is read, and it is
the only list of variables you need. Almost every variable is optional: an
unset one turns its feature off (the route answers 503, or the source registers
and returns an empty snapshot) rather than breaking the server. `PORT` defaults
to 3000.

### Public site

There is nothing to build. Point any static file server at the repo root, or
open the `.html` file directly. `config.js` is git-ignored — copy
`config.sample.js` to `config.js` and fill in your Supabase URL, anon key, API
base URL and API key.

**Do not add a bundler, a framework, a transpiler or a `package.json` to the
site.** Buildless is a deliberate constraint, not an oversight.

### Discord bot

```bash
cd discord-bot
cp env.sample .env
pip install -r requirements.txt
python apply_schema_presets.py   # idempotent schema applier
python bot.py
```

### Feeder node agents

```bash
cd feeder-nodes/radio-node   # or pager-node, or aircraft-node
go build ./cmd/nodeagent
```

Go 1.26. The three are **separate modules** with separate `go.mod` files; there
is no workspace tying them together, so build each from its own directory.

## Making a change

1. Branch from `dev-beta`.
2. Make the change. Match the surrounding code's naming, comment density and
   idiom.
3. Run the smallest check that proves it (below).
4. Commit to `dev-beta` in logical commits.
5. Open a PR against `dev-beta`, never against `main`.

CI (`.github/workflows/tests.yml`) runs on every push and PR to `main` and
`dev-beta`:

- **Backend** — `npm run typecheck` then `npx vitest run`, from `backends/node`.
- **Frontend** — every classic inline `<script>` in a root `.html` file is
  parsed. A syntax error anywhere in a 900 KB page fails the build.
- **Discord bot** — `py_compile` over `discord-bot/*.py`, plus ruff
  (informational; it never blocks).

`.github/workflows/security.yml` covers dependency audit, secret hygiene and
CodeQL.

## Which check to run

| You changed | Run |
|---|---|
| Backend TypeScript | `cd backends/node && npx tsc --noEmit && npx vitest run` — both must pass |
| A root `.html` page | Confirm the inline scripts parse; CI does the same pass |
| `map-editor-module.js` | Bump the `?v=` on its script tag in `map.html`, **same commit** (`map.html:20689`) |
| A Go agent | `go build ./cmd/nodeagent` and `go test ./...` from that module's directory |
| Discord bot | `python -m py_compile discord-bot/*.py` |
| Documentation only | Do not run the backend suite. Open each file you cited and confirm the claim |

## Commit messages

Describe what changed and why, in the repo's existing voice — look at
`git log` before writing one. Scope prefixes are used loosely
(`video:`, `staff:`, `notifications:`, `deps:`).

**No AI attribution.** No `Co-Authored-By`, no session links, no "generated
with" lines. Not in the first commit, not added and stripped later — stripping
it rewrites shared history.

## Documentation

Documentation is held to the same bar as code: **every factual sentence must be
traceable to a file that exists in the repository today.** If you cannot point
at the line, do not write the sentence. Plausible prose that does not survive
comparison with the code is worse than a gap, because a gap is visible.

Each component doc names the file execution starts from. That is deliberate: a
doc that names its entry point breaks visibly when the code moves, instead of
going quietly wrong.

Say what the project cannot do. It is partial and unfunded by design.
Documentation that oversells is a defect.

## Reporting a bug you cannot fix

Say where it is (file and line), what you expected, and what happens instead.
If you found it while doing something else, report it and keep going — do not
fold an unrelated fix into your change.

## What needs the owner

- Any deploy, restart, or merge to `main`.
- Anything with a recurring cost: a paid API, paid hosting, a new service.
- Anything that changes what the public site claims about itself.
