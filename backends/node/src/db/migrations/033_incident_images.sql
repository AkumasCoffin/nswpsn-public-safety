-- Incident photos: up to 4 images per incident, uploaded by editors.
-- Each entry: { id, file, size, content_type, uploaded_by,
--               uploaded_by_name, uploaded_at }
-- The files themselves live on disk under <repo>/uploads/incident-images/
-- (served by Apache on nswpsn.forcequit.xyz; resized via /cdn-cgi/image/).
ALTER TABLE incidents
  ADD COLUMN IF NOT EXISTS images JSONB NOT NULL DEFAULT '[]'::jsonb;
