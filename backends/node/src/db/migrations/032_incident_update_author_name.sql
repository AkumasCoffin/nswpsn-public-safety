-- Denormalised author display name on incident log entries, stamped at
-- creation from the editor's Supabase JWT (username metadata falling
-- back to the email local part). Existing rows stay NULL and render
-- without an author.

ALTER TABLE incident_updates ADD COLUMN IF NOT EXISTS created_by_name TEXT;
