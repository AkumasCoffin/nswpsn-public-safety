# Operating rules for this repository

These are the rules for anyone working on AusAware — person or agent. They are
prohibitions, not advice. Several exist because they were broken once already
and cost real damage.

Read this file before your first change. `CLAUDE.md` and `CONTRIBUTING.md` both
point here; this file is the single copy.

The site is **live in production**. There is no staging environment.

---

## 1. Never deploy. Never restart anything.

Your work finishes at the commit on `dev-beta`. Nothing else.

- Never run `backends/node/scripts/deploy.sh`.
- Never run `pm2 restart`, `pm2 reload`, `pm2 start`, `pm2 delete`, or
  `pm2 stop` — against any process, on any host.
- Never restart the Discord bot, a feeder node, a whisper server, or the
  central rdio-scanner.

The owner deploys, on their own timing. When your change needs a deploy to take
effect, say so in your handoff. Do not deploy in order to verify something:
verify what can be verified locally, and say plainly what cannot.

The live API has been restarted more than once by someone who was never told
not to. That is why this is rule 1.

## 2. SSH to the production host is read-only.

If you are given access at all, it is for reading.

**Allowed:** `SELECT`, `pm2 logs`, `pm2 status`, reading files, `ss`/`netstat`,
`df`, `journalctl`.

**Never:** `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `ALTER`, any backfill SQL,
editing a file on the box, `git pull` on the box, `npm install` on the box,
`pm2 restart`.

If a one-off query needs running, write out the exact command and hand it to the
owner. Do not run it yourself.

## 3. `dev-beta` is permanent. Never delete it.

It stays even when it looks stale, even when it looks fully merged. All work
lands there first.

## 4. Never merge `dev-beta` into `main`.

That merge is the release step and it belongs to the owner. Do not open it, do
not fast-forward it, do not do it "because CI is green".

## 5. No AI attribution in commit messages. Any repo, no exceptions.

No `Co-Authored-By`, no session links, no "generated with" lines, no tool
names. Leave it out of the **first** commit — adding it and stripping it later
rewrites shared history.

## 6. Dependency changes get committed, never installed on the server.

`backends/node/scripts/deploy.sh:31` runs
`git checkout -- backends/node/package-lock.json` **before** it pulls. Anything
you `npm install` on the box is discarded at the next deploy, silently.

Change `package.json`, commit the updated `package-lock.json`, and let the
deploy install it.

Do not remove that `git checkout --` line from the deploy script. It is
deliberate: `npm install` rewrites the lockfile on every deploy, and without the
discard the next `git pull --ff-only` aborts with "local changes would be
overwritten". The comment above the line says so.

## 7. `backends/ecosystem.config.js` is stale and unused.

Nothing on the box runs from it. Never cite it as the source of truth for a
port, an environment variable, a `kill_timeout`, or anything else. The real
answers are in `backends/node/src/config.ts` (environment) and
`backends/node/scripts/deploy.sh` (how the process is actually started and
restarted).

## 8. Never propose row-level security on the incident or role tables.

Those tables live in the backend's own PostgreSQL, not in Supabase. RLS is a
Supabase feature and does not apply to them. Supabase holds **authentication and
identity only**.

## 9. The backend is read-only against the rdio-scanner database.

`RDIO_DATABASE_URL` (`backends/node/src/services/rdio.ts`) is a separate
PostgreSQL owned by the central rdio-scanner. AusAware reads it. AusAware never
writes to it. Do not add a write path, a migration, or a backfill against it.

## 10. No account-deletion logic keyed on "no roles" or "no signup request".

An account with neither a role nor a signup request is a **normal public user**.

That heuristic once deleted real people. It does not come back, and no document
in this repo may suggest it.

## 11. Pager system credentials stay server-side.

The pager relay's Pagermon credentials (`PAGERMON_INGEST_API_KEY` and its
per-state variants) live on the backend and are never pushed out to a node
agent. A pager node authenticates with its own node token; the backend
substitutes the Pagermon key when it forwards. Never write code or a document
that implies a node holds those credentials.

The same shape applies to radio: a node never holds
`RDIO_INTERNAL_API_KEY`. See `backends/node/src/api/node-ingest.ts`.

## 12. Never imply affiliation with the NSW Public Safety Network, or any agency.

The site is **named after** that network. It monitors publicly receivable,
unencrypted traffic. There is no affiliation with any agency, government body,
or radio network.

This holds in code comments, UI copy, commit messages, documentation and
anything else that ships. Do not write "official", "partnered with", "on behalf
of", or anything a reader could take that way.

## 13. Ingested radio traffic is a *reception*, never a "call".

Use "reception" consistently, everywhere — docs, UI copy, new code, commit
messages.

The one exception: when you are quoting a third party's own identifier, keep
theirs. rdio-scanner's upload endpoint really is named `call-upload`, its table
really is `calls`, and its admin UI really says "Calls". Naming those exactly is
correct. Describing *our* ingested traffic as a "call" is not.

## 14. If `map-editor-module.js` changes, bump its `?v=` in `map.html`.

Same commit. The tag is at `map.html:20689`:

```html
<script src="map-editor-module.js?v=43"></script>
```

Increment the number. Miss it and phones pin the old build — `.htaccess` sets
`no-cache, must-revalidate` on `.html`/`.css`/`.js`, but a cached
`map-editor-module.js` paired with a fresh `map.html` is exactly the breakage
that policy cannot fix on its own.

## 15. Leave the `.claude/` entries in `.gitignore` alone.

`.gitignore:21` ignores `.claude/`. It is a path that needs ignoring, not an
attribution. Do not remove it, and do not treat it as something to clean up.

## 16. No new paid services, hosting, or paid APIs.

The project runs out of pocket on donations. Anything with a recurring cost
needs the owner's sign-off **before** it is built — not after it works.

## 17. Never commit secrets, credentials, or user data.

If you see any in a diff, stop and escalate. Do not "fix it in the next commit":
a secret in a commit is a leaked secret even if the next commit removes it.

---

## Match the stack. Do not introduce alternatives.

| Part | Stack | Where |
|---|---|---|
| Backend | Node + TypeScript, Hono, `pg`, zod, Vitest | `backends/node` |
| Node agents | Go 1.26, three separate modules | `feeder-nodes/{radio,pager,aircraft}-node` |
| Discord bot | Python, discord.py | `discord-bot` |
| Public site | vanilla HTML/CSS/JS, Leaflet, **no build step** | repo root |
| Database | PostgreSQL, numbered migrations | `backends/node/src/db/migrations` |
| Identity | Supabase — authentication and identity only | — |

The public site is static files served straight off the repo root. **Do not add
a bundler, a framework, a transpiler or a package manifest to it**, and do not
document one. A page is an `.html` file you can open.

There is no Python/Flask backend. It was deleted months ago. If you find a
document that describes one, that document is wrong — fix the document.

## Branch and deploy workflow

1. Branch from `dev-beta` and commit back to `dev-beta`. Never commit to `main`.
2. CI (`.github/workflows/tests.yml`) runs on every push and PR to `main` and
   `dev-beta`: backend `npm run typecheck` + `npx vitest run` from
   `backends/node`, a parse pass over every classic inline `<script>` in the
   root HTML, and `py_compile` + ruff over `discord-bot/`.
3. The **owner** merges `dev-beta` to `main` and runs the deploy. Nobody else,
   ever.

If your change needs a deploy to take effect, finish at the commit and say so.

## Before you commit

- **Touching backend code?** `npx tsc --noEmit` and the full Vitest suite must
  pass from `backends/node`.
- **Touching only docs?** Do not run the backend suite. Run the smallest check
  that proves the work.
- **Changed a Go agent?** It does not reach any node until its version is bumped
  in `backends/node/assets/node-versions.json`. `deploy.sh` skips the Go rebuild
  entirely when the built binary already reports the manifest version.
- **Changed `map-editor-module.js`?** See rule 14.
- **Added a dependency?** Commit the lockfile. See rule 6.

## Where to read next

- [`README.md`](README.md) — what AusAware is, and its honest limits.
- [`docs/architecture.md`](docs/architecture.md) — the whole system, and the
  full trace from a transmission in the air to a pin on the live map.
- [`docs/components/`](docs/components/) — one short doc per component, each
  naming the file execution starts from.
