-- Follow-up to 086: migrated captions were written as PLAIN TEXT, but the
-- article body renders as Markdown -- and a caption line of just "-" (a
-- visual divider people type) is Markdown's setext-heading marker, so every
-- paragraph above one rendered as a giant <h2>.
--
-- Translate those separator lines into what the author visually meant: a
-- horizontal rule with proper paragraph breaks around it. Only rows that
-- came from media_posts (still listed in media_posts_legacy) are touched --
-- real articles were written as Markdown on purpose.

UPDATE articles a
   SET body = regexp_replace(a.body, E'\\n[ \\t]*-+[ \\t]*\\n', E'\n\n---\n\n', 'g')
 WHERE EXISTS (SELECT 1 FROM media_posts_legacy l WHERE l.id = a.id)
   AND a.body ~ E'\\n[ \\t]*-+[ \\t]*\\n';
