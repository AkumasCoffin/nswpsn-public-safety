-- Article series: a lead article with up to 5 follow-up parts, for covering a
-- multi-day event as connected updates instead of unrelated posts.
--
-- Flat by design: a part points at its lead and CANNOT itself have parts, so a
-- series is always one level deep and can never form a chain or a cycle. The
-- 5-part cap and the no-nesting rule are enforced in api/wire.ts on write.
--
-- Soft reference (no FK), matching every other Wire relation — deleting a lead
-- leaves its parts intact rather than cascading them away; they simply stop
-- resolving a parent and the reads treat them as standalone again.
ALTER TABLE articles ADD COLUMN IF NOT EXISTS parent_article_id TEXT;

-- Parts are looked up by their lead constantly (series list on the detail page)
-- and excluded from the feed, so both directions want an index.
CREATE INDEX IF NOT EXISTS idx_articles_parent
  ON articles(parent_article_id, published_at) WHERE parent_article_id IS NOT NULL;
