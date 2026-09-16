-- Map filter preferences, stored against the account so they follow a user
-- between devices.
--
-- Same shape and same reasoning as watermark_default (migration 096): a
-- preference that used to live only in localStorage, which means it was lost on
-- a different browser and on anything that clears site data. The map's filters
-- are fiddly enough to set up — aircraft type pills, label fields, trails — that
-- losing them is a real annoyance rather than a cosmetic one.
--
-- JSONB rather than a column per setting: this is one opaque blob the client
-- owns, versioned inside itself, so adding a filter later needs no migration.
-- The server never reads into it and never validates its contents beyond a size
-- bound; it is storage, not schema.
--
-- NOT exposed by the public GET /api/profiles/:userId. That route runs every row
-- through shapeProfile(), which is a strict allowlist, so a new column cannot
-- leak by being added here. Only the JWT-gated map-prefs routes read it back.

ALTER TABLE user_profiles
  ADD COLUMN IF NOT EXISTS map_prefs JSONB;
