# Scalar API reference — vendored viewer for `/api-docs`

`standalone.js` is `dist/browser/standalone.js` from the npm package
`@scalar/api-reference`, unmodified. It is the viewer the API reference page
(`api-docs.html` at the repo root) loads. It is committed rather than loaded
from a CDN so the page never fetches anything from a third party, and so the
site keeps its no-build rule: the file is served as it is.

| | |
|---|---|
| Package | `@scalar/api-reference` |
| Version | 1.76.0 |
| Licence | MIT (github.com/scalar/scalar) |
| Tarball | https://registry.npmjs.org/@scalar/api-reference/-/api-reference-1.76.0.tgz |
| Tarball integrity | `sha512-pjqwnzmA9YK3ZX+IZSICMsywm4qI6HLSngR2py9H6TvFApHHuJjkTAd+gFJfnqJDzJuro8k7ji6JCtJ2U+Byng==` |
| `standalone.js` sha256 | `dcb027152bf20bf2f6dcd66e8f9fb073c1a9e274cf0b94b939a7d99ab8947060` |

`.gitattributes` marks this directory `-text`: git must never rewrite its line
endings, or the file would stop matching the hash above. The page asks for it
as `standalone.js?v=<version>`, so a new version is a new URL even where a
cached copy would otherwise be kept, and loads it only once there is a
reference to show — a signed-out visitor never downloads it. This README is
not served (the root `.htaccess` refuses `.md`).

## Updating

1. In an empty scratch directory: `npm pack @scalar/api-reference@<version>`,
   and check the tarball against the `dist.integrity` npm reports
   (`npm view @scalar/api-reference@<version> dist.integrity`).
2. Extract `package/dist/browser/standalone.js` over this one.
3. Bump the `?v=` on the `<script src="vendor/scalar/standalone.js?v=…">` in
   `api-docs.html` to the new version, and update the table above.
4. Re-check the options `api-docs.html` passes to `Scalar.createApiReference` —
   telemetry, the request proxy, default fonts, the AI agent, MCP and the
   developer tools must stay off, and "Test Request" and the API client hidden.
   Scalar has changed their defaults between releases.
5. Open `/api-docs` signed in, with the browser's network panel open: nothing
   may be requested from any host but this site, `api.forcequit.xyz` and the
   ones every page already uses.
