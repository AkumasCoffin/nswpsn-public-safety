# wire-embed — The Wire link-unfurl Worker

Makes shared Wire post links (`nswpsn.forcequit.xyz/wire?tab=media&post=…`)
unfurl with the post's real title / description / cover image on Discord, X,
Facebook, Slack, etc. — without changing the URLs people share.

## How it works

The frontend is a static page behind Cloudflare, so social crawlers (which
don't run JS) never see the client-rendered post and links don't unfurl. This
Worker runs on the `/wire*` route and:

- passes **real browsers** and any non-post URL straight through to the origin;
- for a **crawler** requesting a specific post, fetches the post's public Open
  Graph metadata from `GET https://api.forcequit.xyz/api/wire/og/<type>/<key>`
  and rewrites the origin HTML `<head>` (strips static OG/Twitter tags, injects
  the post's real ones, sets `<title>`).

It fails open: any error → the untouched origin response.

No secrets are needed. The `/api/wire/og/…` endpoint is public and only returns
OG fields (title, short description, cover image, canonical URL) of **published,
non-taken-down** posts, and only once `WIRE_PUBLIC=true` on the backend. During
soft launch it returns 404, so embeds stay dark until the Wire goes public.

## Deploy (one time)

Requires the Cloudflare CLI (`npm i -g wrangler`) and access to the
`forcequit.xyz` zone.

```bash
cd workers/wire-embed
wrangler login          # opens a browser to authorise (do this yourself)
wrangler deploy         # publishes the Worker + attaches the /wire* route
```

That's it — the route in `wrangler.toml` binds the Worker to
`nswpsn.forcequit.xyz/wire*` on deploy. Redeploy with `wrangler deploy` after
any edit to `worker.js`.

## Verify

```bash
# Should return the post's OG tags in the <head>:
curl -s -A "Discordbot/2.0" "https://nswpsn.forcequit.xyz/wire?tab=media&post=<ID>" | grep -i 'og:'

# A normal UA should get the untouched page (no injected og:title):
curl -s -A "Mozilla/5.0" "https://nswpsn.forcequit.xyz/wire?tab=media&post=<ID>" | grep -ic 'og:title'
```

Or paste a post link into Discord / <https://cards-dev.twitter.com/validator> /
the Facebook Sharing Debugger and confirm the preview shows the post.

> Embeds only light up once `WIRE_PUBLIC=true` on the backend. Until then the OG
> endpoint returns 404 by design and links fall back to the plain page.
