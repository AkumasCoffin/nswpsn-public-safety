-- Swatches saved from the agency colour wheel, newest first.
--
-- GlobalConfigSchema has accepted and validated `colorPalette` since the
-- feature was added, but there was no column to put it in: saveGlobalConfig
-- never wrote it and rowToConfig never read one, so the schema default ([])
-- won every time. Every save and every reload answered with an empty palette
-- and the staff UI overwrote its local swatches with it, which is to say the
-- feature never worked at all.
--
-- Shape: a JSON array of CSS colour strings, at most 48 of at most 9
-- characters (services/nodes/globalConfig.ts ColorPaletteSchema).
--
-- Deliberately OUTSIDE globalConfigVersion: the palette is display-only, like
-- displayColor itself, and is never pushed to a node — folding it into the
-- version hash would re-sync the whole fleet every time someone picked a
-- colour.
ALTER TABLE feeder_global_config
  ADD COLUMN IF NOT EXISTS color_palette JSONB NOT NULL DEFAULT '[]'::jsonb;
