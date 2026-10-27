# Scalar API reference — vendored viewer for `/api/docs`

`standalone.js` is `dist/browser/standalone.js` from the npm package
`@scalar/api-reference`, unmodified. It is the viewer the API documentation
page (`src/api/v1/docs.ts`, shell in `assets/docs/`) loads. It is committed
rather than installed so that `npm install` on every deploy does not pull the
whole package and its dependency tree for one file, and so the page never
loads anything from a CDN.

| | |
|---|---|
| Package | `@scalar/api-reference` |
| Version | 1.76.0 |
| Licence | MIT (github.com/scalar/scalar) |
| Tarball | https://registry.npmjs.org/@scalar/api-reference/-/api-reference-1.76.0.tgz |
| Tarball integrity | `sha512-pjqwnzmA9YK3ZX+IZSICMsywm4qI6HLSngR2py9H6TvFApHHuJjkTAd+gFJfnqJDzJuro8k7ji6JCtJ2U+Byng==` |
| `standalone.js` sha256 | `dcb027152bf20bf2f6dcd66e8f9fb073c1a9e274cf0b94b939a7d99ab8947060` |

`standalone.js.br` is the same bytes, brotli quality 11. The server sends it to
any browser that accepts `br` (all current ones), so the 4.9 MB file goes over
the wire as about 1 MB, compressed once here rather than on every request.

`.gitattributes` marks this directory `-text`: git must never rewrite its line
endings, or the two files would stop matching.

## Updating

1. In an empty scratch directory: `npm pack @scalar/api-reference@<version>`,
   and check the tarball against the `dist.integrity` npm reports
   (`npm view @scalar/api-reference@<version> dist.integrity`).
2. Extract `package/dist/browser/standalone.js` over this one.
3. Recompress:
   `node -e "const fs=require('fs'),z=require('zlib');const s=fs.readFileSync('standalone.js');fs.writeFileSync('standalone.js.br',z.brotliCompressSync(s,{params:{[z.constants.BROTLI_PARAM_QUALITY]:11}}))"`
4. Bump `SCALAR_VERSION` in `src/api/v1/docs.ts` (it is the cache-busting
   `?v=` on the script URL — the file is served as immutable) and update the
   table above.
5. Re-check the configuration options `assets/docs/docs.js` sets — telemetry,
   the request proxy, the AI agent, MCP and default fonts must all stay off.
   Scalar has changed their defaults between releases.
6. Open `/api/docs` and check the browser console: the page's
   Content-Security-Policy blocks every host but this one, so anything new the
   viewer tries to reach shows up there as a CSP error.
