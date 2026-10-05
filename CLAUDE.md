# CLAUDE.md

The operating rules for this repository are in **[`AGENTS.md`](AGENTS.md)**.
Read that file before making any change. It is the single copy; this file does
not restate it.

They are prohibitions, not advice. The four that get broken most often:

- **Never deploy and never restart anything.** Work finishes at the commit on
  `dev-beta`. The owner deploys.
- **Never merge `dev-beta` into `main`.** That is the owner's release step.
- **No AI attribution in commit messages.** No `Co-Authored-By`, no session
  links, no "generated with" lines — and not in the first commit either.
- **Ingested radio traffic is a *reception*, never a "call".**

Then read [`README.md`](README.md) for what AusAware is and
[`docs/architecture.md`](docs/architecture.md) for how it fits together.

## Fast orientation

| Question | Answer |
|---|---|
| Where is the backend? | `backends/node`, entry point `backends/node/src/index.ts` |
| What is it built with? | Node + TypeScript, Hono, `pg`, zod, Vitest |
| Where is the public site? | The repo root. Static `.html` files, Leaflet, **no build step** |
| Where is the Discord bot? | `discord-bot/bot.py` (Python, discord.py) |
| Where are the feeder nodes? | `feeder-nodes/{radio,pager,aircraft}-node/cmd/nodeagent` (Go 1.26) |
| Which branch? | `dev-beta`. Never `main` |
| Is there a Python backend? | No. It was deleted months ago |

## Smallest useful check

Backend code:

```bash
cd backends/node && npx tsc --noEmit && npx vitest run
```

Docs only: do not run the backend suite. Open the files you cited and confirm
the claim.
