/**
 * The caller's address, as seen through Cloudflare.
 *
 * api.forcequit.xyz is fronted by a Cloudflare Tunnel, so the socket peer is
 * always the tunnel. Cloudflare puts the real client in `cf-connecting-ip`;
 * `x-forwarded-for` is the fallback for a direct or locally proxied call.
 */
export function clientIp(c: { req: { header: (k: string) => string | undefined } }): string {
  return (
    c.req.header('cf-connecting-ip') ??
    c.req.header('x-forwarded-for')?.split(',')[0]?.trim() ??
    ''
  );
}

/**
 * The part of an address that stays put for one client across a browsing
 * session. A browser token is bound to this rather than the exact address:
 * mobile carriers rotate the low bits of a v6 address (and some NAT pools the
 * last octet of v4) between requests, and binding to the exact value would
 * turn every such request into a re-mint. /24 for v4, /48 for v6.
 */
export function ipScope(ip: string): string {
  if (!ip) return '';
  if (ip.includes(':')) {
    // v6: keep the first three hextets. Expand nothing — the textual form
    // Cloudflare hands over is already canonical for one client.
    const parts = ip.split(':');
    return parts.slice(0, 3).join(':').toLowerCase() + '::/48';
  }
  const octets = ip.split('.');
  if (octets.length !== 4) return ip;
  return octets.slice(0, 3).join('.') + '.0/24';
}
