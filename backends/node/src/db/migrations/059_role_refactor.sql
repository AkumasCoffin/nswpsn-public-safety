-- Role refactor: main role (grouping) + subrole (grants perms), stored as one
-- namespaced "main:sub" string in the existing user_roles.role column.
--
--   owner               : unchanged (top-level, everything)
--   staff               : was team_member — user/role management + request review
--   feeder:radio        : was radio_contributor
--   feeder:pager        : was pager_contributor
--   feeder:agency_data  : was data_feeder
--   feeder:monitor      : was node_monitor (view-only Data/Nodes)
--   feeder:manager      : NEW — feeder node/config management
--   wire:contributor    : was media_feeder
--   wire:manager        : NEW — Wire approvals + takedowns
--   map:editor          : was map_editor
--   map:manager         : NEW — map-editor oversight
--   authed              : NEW base role EVERY account gets (granted on login by
--                         POST /api/profiles/sync). Staff "Users" tab = only
--                         authed; "Members" = authed + at least one real role.
--
-- The 'dev' role is REMOVED entirely — its powers (Dev tab, node management)
-- move to owner + the new feeder:manager.
--
-- Only the FULL string grants permission; the part before ':' is a grouping
-- label for the UI. user_roles.role is free-form TEXT (no enum/CHECK), so these
-- are plain data rewrites. The NOT EXISTS guard avoids colliding with the
-- UNIQUE(user_id, role) constraint when a user somehow already holds the new
-- name; the follow-up DELETE clears any old-name row the guard skipped.

UPDATE user_roles SET role = 'staff'
  WHERE role = 'team_member'
    AND NOT EXISTS (SELECT 1 FROM user_roles t WHERE t.user_id = user_roles.user_id AND t.role = 'staff');
UPDATE user_roles SET role = 'feeder:radio'
  WHERE role = 'radio_contributor'
    AND NOT EXISTS (SELECT 1 FROM user_roles t WHERE t.user_id = user_roles.user_id AND t.role = 'feeder:radio');
UPDATE user_roles SET role = 'feeder:pager'
  WHERE role = 'pager_contributor'
    AND NOT EXISTS (SELECT 1 FROM user_roles t WHERE t.user_id = user_roles.user_id AND t.role = 'feeder:pager');
UPDATE user_roles SET role = 'feeder:agency_data'
  WHERE role = 'data_feeder'
    AND NOT EXISTS (SELECT 1 FROM user_roles t WHERE t.user_id = user_roles.user_id AND t.role = 'feeder:agency_data');
UPDATE user_roles SET role = 'feeder:monitor'
  WHERE role = 'node_monitor'
    AND NOT EXISTS (SELECT 1 FROM user_roles t WHERE t.user_id = user_roles.user_id AND t.role = 'feeder:monitor');
UPDATE user_roles SET role = 'wire:contributor'
  WHERE role = 'media_feeder'
    AND NOT EXISTS (SELECT 1 FROM user_roles t WHERE t.user_id = user_roles.user_id AND t.role = 'wire:contributor');
UPDATE user_roles SET role = 'map:editor'
  WHERE role = 'map_editor'
    AND NOT EXISTS (SELECT 1 FROM user_roles t WHERE t.user_id = user_roles.user_id AND t.role = 'map:editor');

-- Clear old-name rows the guarded UPDATEs skipped (user already had the new
-- name), and drop 'dev' entirely.
DELETE FROM user_roles
  WHERE role IN ('team_member', 'radio_contributor', 'pager_contributor',
                 'data_feeder', 'node_monitor', 'media_feeder', 'map_editor', 'dev');

-- Seed the base 'authed' role for every account we already know about (anyone
-- holding a role, or with a profile row). New/returning users get it on their
-- next authenticated page load via POST /api/profiles/sync.
INSERT INTO user_roles (user_id, role, granted_by)
  SELECT DISTINCT user_id, 'authed', 'migration-059' FROM user_roles
  ON CONFLICT (user_id, role) DO NOTHING;
INSERT INTO user_roles (user_id, role, granted_by)
  SELECT user_id, 'authed', 'migration-059' FROM user_profiles
  ON CONFLICT (user_id, role) DO NOTHING;
