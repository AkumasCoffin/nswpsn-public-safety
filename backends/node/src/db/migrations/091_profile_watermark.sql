-- Custom media watermark: a transparent PNG a contributor uploads on their
-- profile, stamped onto their Wire/Fleet photos client-side instead of the
-- plain username text. The image lives in R2 (wire/watermarks/); only the
-- key is stored, and it is served back to its OWNER through the API (the
-- compose pages draw it onto a canvas, which needs same-origin bytes).

ALTER TABLE user_profiles ADD COLUMN IF NOT EXISTS watermark_key TEXT;
