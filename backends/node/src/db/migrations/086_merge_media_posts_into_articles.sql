-- The Wire: media posts merge INTO articles. The two entities were the same
-- shape apart from articles' extras (slug/excerpt/body, draft lifecycle,
-- series) — the split just meant two feeds, two composers and double code.
-- Every media post becomes an article, keeping its id, so every child row
-- (media, views, likes, comments, edit history, takedowns) follows it by a
-- parent_type rewrite and every old deep link keeps resolving by id.
--
-- Mapping choices:
--   slug          derived from the title + the first 8 chars of the id
--                 (uuid prefix), so it's readable AND collision-proof.
--   body          the caption verbatim — plain text is valid Markdown, and
--                 the article body is where the content lives.
--   excerpt       the caption's first 280 chars as the feed-card teaser.
--                 The detail page hides the lede when the body starts with
--                 it, so nothing displays twice.
--   published_at  created_at (media posts published on creation); NULL for
--                 rows still awaiting review, matching article semantics
--                 (reviewPost stamps it on approval).
--
-- The old table is RENAMED, not dropped: the data stays reachable for a
-- manual rollback, and anything still referencing media_posts fails loudly
-- instead of quietly reading a stale copy.

INSERT INTO articles (
  id, author_id, author_name, title, slug, excerpt, body,
  location_type, region, lat, lng, agencies, incident_id, views, status,
  published_at, created_at, updated_at, removed_by, removed_by_name, removed_at,
  license, credit, rights_affirmed, taken_down_at,
  reviewed_by, reviewed_by_name, reviewed_at, review_note,
  incident, co_authors, watermark
)
SELECT
  mp.id, mp.author_id, mp.author_name, mp.title,
  COALESCE(NULLIF(trim(both '-' from lower(regexp_replace(mp.title, '[^a-zA-Z0-9]+', '-', 'g'))), ''), 'post')
    || '-' || substr(mp.id, 1, 8),
  left(mp.caption, 280),
  mp.caption,
  mp.location_type, mp.region, mp.lat, mp.lng, mp.agencies, mp.incident_id, mp.views, mp.status,
  CASE WHEN mp.status IN ('pending', 'rejected') THEN NULL ELSE mp.created_at END,
  mp.created_at, mp.updated_at, mp.removed_by, mp.removed_by_name, mp.removed_at,
  mp.license, mp.credit, mp.rights_affirmed, mp.taken_down_at,
  mp.reviewed_by, mp.reviewed_by_name, mp.reviewed_at, mp.review_note,
  mp.incident, mp.co_authors, mp.watermark
FROM media_posts mp
WHERE NOT EXISTS (SELECT 1 FROM articles a WHERE a.id = mp.id);

-- Children follow the parent. Ids are uuids from separate sequences, so a
-- parent_type rewrite cannot collide with existing article children (the
-- wire_views / wire_likes primary keys included).
UPDATE wire_media     SET parent_type = 'article' WHERE parent_type = 'media_post';
UPDATE wire_views     SET parent_type = 'article' WHERE parent_type = 'media_post';
UPDATE wire_comments  SET parent_type = 'article' WHERE parent_type = 'media_post';
UPDATE wire_likes     SET parent_type = 'article' WHERE parent_type = 'media_post';
UPDATE wire_edits     SET parent_type = 'article' WHERE parent_type = 'media_post';
UPDATE wire_takedowns SET target_type = 'article' WHERE target_type = 'media_post';

-- Articles want exactly one cover. The media composer flagged the first
-- upload, but enforce it for any article that has images and no cover
-- (first image by sort order wins).
UPDATE wire_media wm SET is_cover = true
FROM (
  SELECT DISTINCT ON (parent_id) id
    FROM wire_media
   WHERE parent_type = 'article' AND kind = 'image'
     AND parent_id NOT IN (
       SELECT parent_id FROM wire_media
        WHERE parent_type = 'article' AND kind = 'image' AND is_cover
     )
   ORDER BY parent_id, sort_order, created_at
) pick
WHERE wm.id = pick.id;

ALTER TABLE media_posts RENAME TO media_posts_legacy;
