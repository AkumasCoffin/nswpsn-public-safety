/**
 * The Wire — link-unfurl / embed injector (Cloudflare Worker).
 *
 * Route: nswpsn.forcequit.xyz/wire*  (see wrangler.toml)
 *
 * The frontend is a static page behind Cloudflare, so social crawlers
 * (Discord, X/Twitter, Facebook, Slack, …) that fetch a shared post URL only
 * ever see the generic static HTML — they don't run the JS that renders the
 * post, so links never unfurl with the post's title/image.
 *
 * This Worker fixes that WITHOUT changing the URLs people share:
 *   - Real browsers are passed straight through to the origin, untouched.
 *   - A crawler requesting a specific post (?post=<id> or ?article=<slug>) gets
 *     the same origin HTML, but with its <head> rewritten: existing Open Graph
 *     / Twitter tags are stripped and replaced with the post's real metadata,
 *     fetched from the public API (/api/wire/og/...). No secrets needed — the
 *     endpoint only returns OG fields of already-published posts.
 *
 * If anything goes wrong (API down, not a post URL, not HTML), we fail open and
 * serve the untouched origin response.
 */

const API_BASE = 'https://api.forcequit.xyz';

// User-agents that fetch a URL purely to build a link preview.
const CRAWLER_UA = /(facebookexternalhit|Facebot|Twitterbot|Slackbot|Discordbot|LinkedInBot|TelegramBot|WhatsApp|Pinterest|redditbot|Applebot|SkypeUriPreview|vkShare|Googlebot|bingbot|embedly|Iframely|Mastodon|Bluesky|Synapse|nitter|Yahoo|DuckDuckBot|Qwantify|ia_archiver|MetaInspector|SummalyBot)/i;

function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const ua = request.headers.get('user-agent') || '';
    const post = url.searchParams.get('post');
    const article = url.searchParams.get('article');

    // Only a crawler on a specific-post URL needs the rewrite; everything else
    // goes straight to origin.
    if (!CRAWLER_UA.test(ua) || (!post && !article)) {
      return fetch(request);
    }

    const ogType = article ? 'article' : 'media';
    const ogKey = article || post;

    let originRes;
    let og = null;
    try {
      const [o, ogRes] = await Promise.all([
        fetch(request),
        fetch(`${API_BASE}/api/wire/og/${ogType}/${encodeURIComponent(ogKey)}`, {
          headers: { accept: 'application/json' },
          cf: { cacheTtl: 120, cacheEverything: true },
        }),
      ]);
      originRes = o;
      if (ogRes.ok) {
        const j = await ogRes.json();
        og = j && j.og;
      }
    } catch (e) {
      // Fail open: if we already have the origin response, serve it; otherwise
      // fall back to a plain fetch.
      return originRes || fetch(request);
    }

    const ct = (originRes.headers.get('content-type') || '').toLowerCase();
    if (!og || !ct.includes('text/html')) {
      return originRes;
    }

    const canonical = og.url || url.toString();
    const fullTitle = og.title ? `${og.title} — NSWPSN The Wire` : 'NSWPSN The Wire';
    const desc = og.description || 'Independent photos & video from NSW emergency-services contributors.';
    const image = og.image || '';
    const tags = [
      `<meta property="og:site_name" content="${esc(og.site_name || 'NSWPSN — The Wire')}">`,
      `<meta property="og:type" content="${og.type === 'article' ? 'article' : 'website'}">`,
      `<meta property="og:title" content="${esc(fullTitle)}">`,
      `<meta property="og:description" content="${esc(desc)}">`,
      `<meta property="og:url" content="${esc(canonical)}">`,
      image ? `<meta property="og:image" content="${esc(image)}">` : '',
      `<meta name="twitter:card" content="${image ? 'summary_large_image' : 'summary'}">`,
      `<meta name="twitter:title" content="${esc(fullTitle)}">`,
      `<meta name="twitter:description" content="${esc(desc)}">`,
      image ? `<meta name="twitter:image" content="${esc(image)}">` : '',
      og.author ? `<meta name="author" content="${esc(og.author)}">` : '',
      `<link rel="canonical" href="${esc(canonical)}">`,
    ].filter(Boolean).join('\n');

    // Strip any static default OG/Twitter tags, then inject the post's real
    // metadata and set the document title.
    return new HTMLRewriter()
      .on("meta[property^='og:']", { element(el) { el.remove(); } })
      .on("meta[name^='twitter:']", { element(el) { el.remove(); } })
      .on("link[rel='canonical']", { element(el) { el.remove(); } })
      .on('title', { element(el) { el.setInnerContent(fullTitle); } })
      .on('head', { element(el) { el.append(tags, { html: true }); } })
      .transform(originRes);
  },
};
