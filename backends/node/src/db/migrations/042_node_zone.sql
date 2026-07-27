-- Feeder nodes: required NSW RFS zone (coarse area) per node, alongside the
-- optional exact antenna pin (lat/lon from 040). Every node must declare a zone
-- going forward; the exact pin stays optional ("zone only" = zone set, lat/lon
-- null). Nullable here so pre-existing rows keep working until the operator sets
-- one; the API/UI enforce it on create + edit. Zone name matches the NSW RFS
-- zone list (data/Extended/nsw-rural-fire-service/zones-*.csv). Staff/owner only,
-- never on the public map.
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS zone TEXT;
