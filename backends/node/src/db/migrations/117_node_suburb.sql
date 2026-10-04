-- Radio nodes adopt the pager location model (state + LGA, suburb optional).
-- Suburb is new for every kind; purely descriptive — nothing routes on it.
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS suburb TEXT;
