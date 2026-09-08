-- Every user incident carries the State / LGA / suburb its pin sits in,
-- resolved server-side from the coordinates via the boundaries point lookup
-- (boundaryForPoint) on create and re-resolved whenever the pin moves.
-- Nullable: resolution is best-effort and a point can miss (offshore, or a
-- suburb-picked incident whose client supplied the names directly).
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS state  TEXT;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS lga    TEXT;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS suburb TEXT;
