-- 078: the talkgroups a patched transmission was actually carried on.
--
-- A patched call reaches us with the PATCH GROUP as its talkgroup — a
-- supergroup nobody scans (10128 alone is ~17,000 events a day here, by far
-- the biggest single "talkgroup" in the table, and it is not a talkgroup at
-- all). The member talkgroups are what the conversation really went out on.
--
-- vce records them per event in activity_event_talkgroup_member and now
-- reports them on /activity/events as `patchMembers` (ControlActivityLookup);
-- the agent forwards the field and recordActivityEvents stores it here.
--
-- Nullable on purpose, and NOT defaulted to an empty array: null means "this
-- reception came from a control server that does not report members", which is
-- a different fact from "this call had none". The read path treats both as
-- no-members, but the distinction is what tells us whether a node is running
-- new enough vce.
--
-- Only ever set on CALL_PATCH_GROUP receptions — an ordinary call has no
-- members — so the column is null on the overwhelming majority of rows and
-- costs nothing on those.

ALTER TABLE node_radio_events
  ADD COLUMN IF NOT EXISTS patch_members INTEGER[];
