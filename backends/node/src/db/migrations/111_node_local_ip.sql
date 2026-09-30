-- The node's LAN address, self-reported by its agent in the WS hello and
-- refreshed on every connect. Stored (not just held on the live socket) so
-- staff still see the LAST KNOWN address on an offline node — which is
-- exactly when someone needs it, to go and find the box. Owners see their
-- own nodes' addresses too; it is never public.
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS local_ip TEXT;
