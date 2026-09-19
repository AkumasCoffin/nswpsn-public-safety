-- Roles an approval granted, kept on the request itself.
--
-- Approval used to have two shapes. If the request carried a supabase_user_id
-- the roles went straight onto that account; if it did not, the backend
-- created a NEW Supabase account with a generated temporary password and put
-- the roles there. The second shape is dead: signup.html creates the account
-- itself, with a password the person chooses, before the request is ever
-- submitted. Creating another one for the same email now just fails.
--
-- What is left is the case the temp-password branch was hiding. When email
-- confirmation is required there is no session yet, so the request is filed
-- against the email alone and reaches the queue unlinked. Staff approve it,
-- the person confirms and signs in — and nothing connects the two.
--
-- So an approval of an unlinked request records what it granted here, and the
-- first signed-in page load claims it by verified email. Comma-separated, to
-- match request_type and tech_experience in the same table.
ALTER TABLE editor_requests ADD COLUMN IF NOT EXISTS approved_roles TEXT;

-- The claim runs on sign-in, so it has to be cheap. Partial, because the rows
-- it covers are only those approved and still waiting for their account: near
-- empty in the steady state, and the lookup is lower(email) to match how
-- addresses are compared everywhere else in this codebase.
CREATE INDEX IF NOT EXISTS idx_editor_requests_unclaimed
  ON editor_requests (lower(email))
  WHERE status = 'approved' AND supabase_user_id IS NULL;
