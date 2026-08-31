-- 083: administrative boundaries — localities, LGAs, states.
--
-- WHY A TABLE AND NOT STATIC FILES
-- The map's old boundary overlays were two NSW-only GeoJSON files shipped
-- whole to the browser (132 LGAs, 10 planning regions, ~1.6 MB). The
-- national replacements are 15,785 localities, 2,210 LGAs and 12,844
-- state polygons — around 40 MB of geometry. Sending all of it so the
-- browser can draw the dozen shapes on screen is not workable, so the
-- data lives here and the map asks for its current viewport.
--
-- The other reason is server-side: this is the dataset that answers
-- "which LGA is this incident in?", which nothing upstream tells us.
--
-- WHY A BBOX AND NOT POSTGIS
-- PostGIS is not installed. A viewport query only needs "does this
-- shape's bounding box overlap the screen?", which is four numeric
-- comparisons, and the exact point-in-polygon test for lookups is done
-- in application code by the same ray-cast lib/stateMask.ts already uses.
-- So each row carries its own precomputed bbox and the index covers it.
--
-- One table rather than three: the kinds differ only in which upstream
-- field fills `name`, and a single table means one query shape, one
-- index, and one API route.

CREATE TABLE IF NOT EXISTS boundaries (
  -- 'locality' | 'lga' | 'state'
  kind        text NOT NULL,
  -- Upstream's own persistent id (loc_pid / lga_pid / state_pid). Stable
  -- across their releases, so a re-import updates rather than duplicates.
  ext_id      text NOT NULL,
  name        text NOT NULL,
  -- abb_name for LGAs, state_abbrev for states; null for localities.
  short_name  text,
  state       text,
  -- loc_class ('Gazetted Locality' / 'District'); null for the others.
  class       text,
  min_lat     double precision NOT NULL,
  min_lon     double precision NOT NULL,
  max_lat     double precision NOT NULL,
  max_lon     double precision NOT NULL,
  -- GeoJSON geometry: Polygon or MultiPolygon.
  geom        jsonb NOT NULL,
  imported_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (kind, ext_id)
);

-- The viewport query: kind first (always equality), then the bbox
-- overlap. Postgres can only range-scan the first inequality column, but
-- with kind pinned the remaining filter is over a few thousand rows at
-- most and stays cheap.
CREATE INDEX IF NOT EXISTS boundaries_kind_bbox
  ON boundaries (kind, min_lat, max_lat, min_lon, max_lon);

-- "every LGA in Queensland" and similar.
CREATE INDEX IF NOT EXISTS boundaries_kind_state ON boundaries (kind, state);

-- Name lookups for pickers and search.
CREATE INDEX IF NOT EXISTS boundaries_kind_name ON boundaries (kind, lower(name));
