package rdioctl

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// forkSchema is the rdio-scanner v6.13.1 fork's CREATE TABLE set for the config
// tables, copied from server/database.go's migrations in their FINAL post-
// migration form: the v6.1.0 normalisation (talkgroups/units in their own tables
// keyed by the system's logical id), plus the later delay/alert columns, and
// WITHOUT the transcribe/transcriptionPrompt columns (migrated out into the
// transcripts plugin's own tables and dropped from systems/talkgroups). This is
// exactly the shape the operator's DB has, so the writer is tested against it.
var forkSchema = []string{
	"create table `rdioScannerSystems` (`_id` integer primary key autoincrement, `autoPopulate` tinyint(1) default 0, `blacklists` text not null, `id` integer not null unique, `label` varchar(255) not null, `led` varchar(255), `order` integer, `delay` integer not null default 0, `alert` varchar(64) not null default '')",
	"create table `rdioScannerTalkgroups` (`_id` integer primary key autoincrement, `frequency` integer, `groupId` integer not null, `id` integer not null, `label` varchar(255) not null, `led` varchar(255), `name` varchar(255) not null, `order` integer, `systemId` integer not null, `tagId` integer not null, `delay` integer not null default 0, `alert` varchar(64) not null default '')",
	"create unique index `rdio_scanner_talkgroups_system_id_id` on `rdioScannerTalkgroups` (`systemId`, `id`)",
	"create table `rdioScannerUnits` (`_id` integer primary key autoincrement, `id` integer not null, `label` varchar(255) not null, `order` integer, `systemId` integer not null)",
	"create unique index `rdio_scanner_units_system_id_id` on `rdioScannerUnits` (`systemId`, `id`)",
	"create table `rdioScannerApiKeys` (`_id` integer primary key autoincrement, `disabled` tinyint(1) default 0, `ident` varchar(255), `key` varchar(255) not null unique, `order` integer, `systems` text not null)",
	"create table `rdioScannerGroups` (`_id` integer primary key autoincrement, `label` varchar(255) not null)",
	"create table `rdioScannerTags` (`_id` integer primary key autoincrement, `label` varchar(255) not null)",
	"create table `rdioScannerDownstreams` (`_id` integer primary key autoincrement, `apiKey` varchar(255) not null, `disabled` tinyint(1) default 0, `order` integer, `systems` text not null, `url` varchar(255) not null)",
}

func newRdioDB(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "rdio-scanner.db")
	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	for _, q := range forkSchema {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("ddl %q: %v", q, err)
		}
	}
	// rdio seeds default groups/tags on a fresh DB; simulate a couple so orphan
	// deletion (config replaces the seed) is exercised.
	if _, err := db.Exec("insert into `rdioScannerGroups` (`_id`, `label`) values (900, 'SeedGroup')"); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	if _, err := db.Exec("insert into `rdioScannerTags` (`_id`, `label`) values (901, 'SeedTag')"); err != nil {
		t.Fatalf("seed tag: %v", err)
	}
	return dbPath
}

// cfgFromJSON decodes a JSON literal into the map[string]any shape WriteConfigDB
// receives at runtime (numbers become float64, exactly as the payload arrives).
func cfgFromJSON(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("cfg json: %v", err)
	}
	return m
}

const twoSystemCfg = `{
  "groups": [
    {"_id": 10, "label": "Fire"},
    {"_id": 20, "label": "Ambulance"}
  ],
  "tags": [
    {"_id": 13, "label": "Dispatch"},
    {"_id": 23, "label": "Tactical"}
  ],
  "systems": [
    {
      "_id": 1, "id": 1, "label": "RFS", "led": "orange", "order": 1,
      "autoPopulate": false, "blacklists": "", "delay": 0, "alert": "",
      "talkgroups": [
        {"id": 30015, "groupId": 10, "tagId": 13, "label": "SWS A", "name": "South Western Slopes A", "order": 1, "frequency": null, "led": null},
        {"id": 30016, "groupId": 10, "tagId": 23, "label": "SWS B", "name": "South Western Slopes B", "order": 2, "frequency": null, "led": null}
      ],
      "units": [
        {"id": 2014294, "label": "State Mitigation", "order": 1}
      ]
    },
    {
      "_id": 2, "id": 2, "label": "ASNSW", "led": "red", "order": 2,
      "autoPopulate": false, "blacklists": "", "delay": 0, "alert": "",
      "talkgroups": [
        {"id": 5001, "groupId": 20, "tagId": 13, "label": "AMB 1", "name": "Ambulance 1", "order": 1, "frequency": null, "led": null}
      ],
      "units": [
        {"id": 700, "label": "Control", "order": 1}
      ]
    }
  ],
  "apiKeys": [
    {"_id": 1, "disabled": false, "ident": "RFS", "key": "key-aaa", "order": 1, "systems": [{"id": 1, "talkgroups": "*"}]},
    {"_id": 2, "disabled": false, "ident": "ASNSW", "key": "key-bbb", "order": 2, "systems": [{"id": 2, "talkgroups": "*"}]}
  ],
  "downstreams": [
    {"_id": 1, "apiKey": "key-aaa", "disabled": false, "order": null, "systems": "*", "url": "http://127.0.0.1:17390"}
  ]
}`

func count(t *testing.T, db *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", q, err)
	}
	return n
}

func TestWriteConfigDB_Idempotent(t *testing.T) {
	dbPath := newRdioDB(t)
	cfg := cfgFromJSON(t, twoSystemCfg)

	// Apply TWICE — the second must not error (proves the old INSERT-collides-on-
	// UNIQUE bug is gone: reconcile upserts by natural key).
	if err := WriteConfigDB(dbPath, cfg); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := WriteConfigDB(dbPath, cfg); err != nil {
		t.Fatalf("second apply (idempotency): %v", err)
	}

	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("open verify: %v", err)
	}
	defer db.Close()

	if n := count(t, db, "select count(*) from `rdioScannerSystems`"); n != 2 {
		t.Errorf("systems = %d, want 2", n)
	}
	if n := count(t, db, "select count(*) from `rdioScannerApiKeys`"); n != 2 {
		t.Errorf("apiKeys = %d, want 2", n)
	}
	if n := count(t, db, "select count(*) from `rdioScannerGroups`"); n != 2 {
		t.Errorf("groups = %d, want 2 (seed group must be deleted)", n)
	}
	if n := count(t, db, "select count(*) from `rdioScannerTags`"); n != 2 {
		t.Errorf("tags = %d, want 2 (seed tag must be deleted)", n)
	}
	if n := count(t, db, "select count(*) from `rdioScannerTalkgroups`"); n != 3 {
		t.Errorf("talkgroups = %d, want 3", n)
	}
	if n := count(t, db, "select count(*) from `rdioScannerUnits`"); n != 2 {
		t.Errorf("units = %d, want 2", n)
	}
	if n := count(t, db, "select count(*) from `rdioScannerDownstreams`"); n != 1 {
		t.Errorf("downstreams = %d, want 1", n)
	}

	// group/tag reference representation: a talkgroup stores the INTEGER groupId/
	// tagId (the groups/tags _id), NOT a label. Verify the round-trip.
	var groupID, tagID int
	if err := db.QueryRow(
		"select `groupId`, `tagId` from `rdioScannerTalkgroups` where `systemId` = 1 and `id` = 30016",
	).Scan(&groupID, &tagID); err != nil {
		t.Fatalf("talkgroup ref scan: %v", err)
	}
	if groupID != 10 || tagID != 23 {
		t.Errorf("talkgroup 30016 refs groupId=%d tagId=%d, want 10/23", groupID, tagID)
	}
	// And those ids resolve to the expected group/tag labels.
	var glabel, tlabel string
	if err := db.QueryRow("select `label` from `rdioScannerGroups` where `_id` = ?", groupID).Scan(&glabel); err != nil {
		t.Fatalf("group label: %v", err)
	}
	if err := db.QueryRow("select `label` from `rdioScannerTags` where `_id` = ?", tagID).Scan(&tlabel); err != nil {
		t.Fatalf("tag label: %v", err)
	}
	if glabel != "Fire" || tlabel != "Tactical" {
		t.Errorf("resolved labels = %q/%q, want Fire/Tactical", glabel, tlabel)
	}

	// blacklists serialised as '[]' when empty, systems grant stored as `"*"`.
	var bl string
	if err := db.QueryRow("select `blacklists` from `rdioScannerSystems` where `id` = 1").Scan(&bl); err != nil {
		t.Fatalf("blacklists: %v", err)
	}
	if bl != "[]" {
		t.Errorf("blacklists = %q, want []", bl)
	}
	var dsSystems string
	if err := db.QueryRow("select `systems` from `rdioScannerDownstreams` where `_id` = 1").Scan(&dsSystems); err != nil {
		t.Fatalf("downstream systems: %v", err)
	}
	if dsSystems != `"*"` {
		t.Errorf("downstream systems = %q, want \"*\"", dsSystems)
	}
}

func TestWriteConfigDB_RemovesOrphanSystem(t *testing.T) {
	dbPath := newRdioDB(t)
	if err := WriteConfigDB(dbPath, cfgFromJSON(t, twoSystemCfg)); err != nil {
		t.Fatalf("seed apply: %v", err)
	}

	// A later apply with only system 1 must delete system 2 and its children.
	const oneSystemCfg = `{
      "groups": [{"_id": 10, "label": "Fire"}, {"_id": 20, "label": "Ambulance"}],
      "tags": [{"_id": 13, "label": "Dispatch"}, {"_id": 23, "label": "Tactical"}],
      "systems": [
        {"_id": 1, "id": 1, "label": "RFS", "led": "orange", "order": 1,
         "autoPopulate": false, "blacklists": "", "delay": 0, "alert": "",
         "talkgroups": [
           {"id": 30015, "groupId": 10, "tagId": 13, "label": "SWS A", "name": "South Western Slopes A", "order": 1}
         ],
         "units": [{"id": 2014294, "label": "State Mitigation", "order": 1}]}
      ],
      "apiKeys": [{"_id": 1, "disabled": false, "ident": "RFS", "key": "key-aaa", "order": 1, "systems": [{"id": 1, "talkgroups": "*"}]}],
      "downstreams": [{"_id": 1, "apiKey": "key-aaa", "disabled": false, "order": null, "systems": "*", "url": "http://127.0.0.1:17390"}]
    }`
	if err := WriteConfigDB(dbPath, cfgFromJSON(t, oneSystemCfg)); err != nil {
		t.Fatalf("shrink apply: %v", err)
	}

	db, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("open verify: %v", err)
	}
	defer db.Close()

	if n := count(t, db, "select count(*) from `rdioScannerSystems`"); n != 1 {
		t.Errorf("systems = %d, want 1", n)
	}
	if n := count(t, db, "select count(*) from `rdioScannerSystems` where `id` = 2"); n != 0 {
		t.Errorf("system 2 survived removal")
	}
	// Removed system's talkgroups/units must be gone; system 1 keeps 1 talkgroup.
	if n := count(t, db, "select count(*) from `rdioScannerTalkgroups` where `systemId` = 2"); n != 0 {
		t.Errorf("system 2 talkgroups survived: %d", n)
	}
	if n := count(t, db, "select count(*) from `rdioScannerUnits` where `systemId` = 2"); n != 0 {
		t.Errorf("system 2 units survived: %d", n)
	}
	if n := count(t, db, "select count(*) from `rdioScannerTalkgroups`"); n != 1 {
		t.Errorf("total talkgroups = %d, want 1", n)
	}
	// A talkgroup removed from a still-present system is pruned too (system 1 had
	// 2 talkgroups, now 1).
	if n := count(t, db, "select count(*) from `rdioScannerTalkgroups` where `systemId` = 1 and `id` = 30016"); n != 0 {
		t.Errorf("removed talkgroup 30016 survived")
	}
	if n := count(t, db, "select count(*) from `rdioScannerApiKeys`"); n != 1 {
		t.Errorf("apiKeys = %d, want 1 (key-bbb removed)", n)
	}
}
