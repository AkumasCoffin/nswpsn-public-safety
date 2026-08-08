// dbstore.go writes an rdio-scanner configuration document DIRECTLY into
// rdio-scanner's own SQLite database, replacing the flaky HTTP admin PUT
// (/api/admin/config) that hangs/EOFs/times out on the operator's node when
// applying the ~120KB config. It is the SQLite sink for the SAME desired
// document that PutConfig used (built by resolveRdioConfig + applyRdioKeys):
// nothing about the document shape changes, only where it lands.
//
// The on-disk representation is mirrored EXACTLY from the checked-out fork
// (D:\working-dir\rdio-scanner\server). Contrary to the "inline talkgroups JSON"
// assumption, this fork uses the NORMALISED v6.1.0+ schema:
//
//   - rdioScannerSystems(_id, autoPopulate, blacklists, id UNIQUE, label, led,
//     order, delay, alert) — talkgroups/units are NOT inline; they live in their
//     own tables keyed by the system's LOGICAL id (not its _id rowid).
//   - rdioScannerTalkgroups(_id, frequency, groupId, id, label, led, name, order,
//     systemId, tagId, delay, alert) UNIQUE(systemId, id). A talkgroup references
//     its group/tag by the INTEGER groupId/tagId, which are the rdioScannerGroups
//     / rdioScannerTags rows' _id — NOT by label. (The client-facing config uses
//     labels; the admin round-trip — GetConfig marshalling *Talkgroup — emits
//     groupId/tagId, and that is the document we receive.)
//   - rdioScannerUnits(_id, id, label, order, systemId) UNIQUE(systemId, id).
//   - rdioScannerApiKeys(_id, disabled, ident, key UNIQUE, order, systems).
//   - rdioScannerGroups(_id, label), rdioScannerTags(_id, label).
//   - rdioScannerDownstreams(_id, apiKey, disabled, order, systems, url).
//
// Because groups/tags are referenced by their _id, and the desired document
// carries explicit, self-consistent _id / groupId / tagId integers, groups and
// tags are reconciled BY _id (preserving it) and no label->id rewriting is
// needed. The transcribe / transcriptionPrompt fields the document may carry are
// deliberately NOT written: in this fork those columns have been migrated out of
// systems/talkgroups into the transcripts plugin's own tables, so rdio's own
// core admin Write does not touch them either.
package rdioctl

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"
)

// WriteConfigDB persists cfg (systems/talkgroups/units, apiKeys, groups, tags,
// downstreams) into rdio-scanner's SQLite database at dbPath, in a single
// transaction, reconciling each table (upsert by natural key + delete orphans)
// so a re-apply is idempotent and never trips a UNIQUE constraint. rdio may hold
// the DB open while running; WAL + busy_timeout let these config-table writes
// proceed alongside rdio's runtime writes (which only touch calls/logs). The
// caller bounces rdio afterwards so it reloads the config at boot.
func WriteConfigDB(dbPath string, cfg map[string]any) (err error) {
	// busy_timeout waits out rdio's own writer instead of failing SQLITE_BUSY;
	// WAL lets a reader (rdio) and our writer coexist; foreign_keys is harmless
	// here (the schema declares no FK constraints) but matches rdio's intent.
	// _txlock=immediate makes database/sql's Begin issue "BEGIN IMMEDIATE", so
	// the write lock (and the busy_timeout wait) is taken up front rather than on
	// a mid-transaction lock upgrade.
	dsn := "file:" + dbPath +
		"?_pragma=busy_timeout%3d5000&_pragma=journal_mode%3dWAL&_pragma=foreign_keys%3don&_txlock=immediate"

	db, oerr := sql.Open("sqlite", dsn)
	if oerr != nil {
		return fmt.Errorf("open rdio db: %w", oerr)
	}
	defer db.Close()

	tx, berr := db.Begin()
	if berr != nil {
		return fmt.Errorf("begin immediate: %w", berr)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = writeGroups(tx, cfg["groups"]); err != nil {
		return fmt.Errorf("groups: %w", err)
	}
	if err = writeTags(tx, cfg["tags"]); err != nil {
		return fmt.Errorf("tags: %w", err)
	}
	if err = writeSystems(tx, cfg["systems"]); err != nil {
		return fmt.Errorf("systems: %w", err)
	}
	if err = writeApiKeys(tx, cfg["apiKeys"]); err != nil {
		return fmt.Errorf("apiKeys: %w", err)
	}
	if err = writeDownstreams(tx, cfg["downstreams"]); err != nil {
		return fmt.Errorf("downstreams: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// writeGroups reconciles rdioScannerGroups by _id: the desired document carries
// explicit _id values that talkgroups reference via groupId, so the _id is
// preserved (upsert ON CONFLICT(_id)) and rows whose _id is not desired are
// deleted. rdio seeds default groups on a fresh DB; this replaces them with the
// operator's set.
func writeGroups(tx *sql.Tx, v any) error {
	rows := asArrayOfMaps(v)
	if rows == nil {
		return nil // section absent → leave the table untouched (matches admin PUT)
	}
	keep := make([]int, 0, len(rows))
	for _, r := range rows {
		id, ok := asInt(r["_id"])
		if !ok {
			continue
		}
		keep = append(keep, id)
	}
	if err := deleteOrphans(tx, "rdioScannerGroups", "_id", keep); err != nil {
		return err
	}
	for _, r := range rows {
		id, ok := asInt(r["_id"])
		if !ok {
			// No _id: let autoincrement assign one (cannot be referenced anyway).
			if _, err := tx.Exec("INSERT INTO `rdioScannerGroups` (`label`) VALUES (?)", asStr(r["label"])); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.Exec(
			"INSERT INTO `rdioScannerGroups` (`_id`, `label`) VALUES (?, ?) "+
				"ON CONFLICT(`_id`) DO UPDATE SET `label` = excluded.`label`",
			id, asStr(r["label"]),
		); err != nil {
			return err
		}
	}
	return nil
}

// writeTags mirrors writeGroups for rdioScannerTags (referenced by talkgroup
// tagId).
func writeTags(tx *sql.Tx, v any) error {
	rows := asArrayOfMaps(v)
	if rows == nil {
		return nil
	}
	keep := make([]int, 0, len(rows))
	for _, r := range rows {
		id, ok := asInt(r["_id"])
		if !ok {
			continue
		}
		keep = append(keep, id)
	}
	if err := deleteOrphans(tx, "rdioScannerTags", "_id", keep); err != nil {
		return err
	}
	for _, r := range rows {
		id, ok := asInt(r["_id"])
		if !ok {
			if _, err := tx.Exec("INSERT INTO `rdioScannerTags` (`label`) VALUES (?)", asStr(r["label"])); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.Exec(
			"INSERT INTO `rdioScannerTags` (`_id`, `label`) VALUES (?, ?) "+
				"ON CONFLICT(`_id`) DO UPDATE SET `label` = excluded.`label`",
			id, asStr(r["label"]),
		); err != nil {
			return err
		}
	}
	return nil
}

// writeSystems reconciles rdioScannerSystems by the LOGICAL `id` (its UNIQUE
// key, and the key talkgroups/units reference as systemId) and cascades into the
// talkgroups/units tables. _id is intentionally left to autoincrement: nothing
// references a system's rowid, and omitting it removes any risk of a primary-key
// collision when the operator's config assigns _id values that clash with rows
// already in the DB. Removed systems (and their talkgroups/units) are deleted.
func writeSystems(tx *sql.Tx, v any) error {
	rows := asArrayOfMaps(v)
	if rows == nil {
		return nil
	}
	keep := make([]int, 0, len(rows))
	for _, r := range rows {
		id, ok := asInt(r["id"])
		if !ok {
			continue
		}
		keep = append(keep, id)
	}
	// Delete talkgroups/units of removed systems first, then the systems.
	if err := deleteOrphans(tx, "rdioScannerTalkgroups", "systemId", keep); err != nil {
		return err
	}
	if err := deleteOrphans(tx, "rdioScannerUnits", "systemId", keep); err != nil {
		return err
	}
	if err := deleteOrphans(tx, "rdioScannerSystems", "id", keep); err != nil {
		return err
	}

	for _, r := range rows {
		id, ok := asInt(r["id"])
		if !ok {
			continue // a system without a logical id is unusable; skip it
		}
		// blacklists is stored bracket-wrapped ("[...]"), '[]' when empty —
		// exactly as rdio's Systems.Write serialises the Blacklists string.
		bl := asStr(r["blacklists"])
		if bl != "" {
			bl = "[" + bl + "]"
		} else {
			bl = "[]"
		}
		if _, err := tx.Exec(
			"INSERT INTO `rdioScannerSystems` "+
				"(`autoPopulate`, `blacklists`, `id`, `label`, `led`, `order`, `delay`, `alert`) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?) "+
				"ON CONFLICT(`id`) DO UPDATE SET "+
				"`autoPopulate` = excluded.`autoPopulate`, `blacklists` = excluded.`blacklists`, "+
				"`label` = excluded.`label`, `led` = excluded.`led`, `order` = excluded.`order`, "+
				"`delay` = excluded.`delay`, `alert` = excluded.`alert`",
			asBoolInt(r["autoPopulate"]), bl, id, asStr(r["label"]), asNullStr(r["led"]),
			asIntDefault(r["order"], 0), asIntDefault(r["delay"], 0), asStr(r["alert"]),
		); err != nil {
			return err
		}
		if err := writeTalkgroups(tx, id, r["talkgroups"]); err != nil {
			return err
		}
		if err := writeUnits(tx, id, r["units"]); err != nil {
			return err
		}
	}
	return nil
}

// writeTalkgroups reconciles one system's talkgroups by (systemId, id). group /
// tag references are written straight from the integer groupId / tagId in the
// document (which point at rdioScannerGroups._id / rdioScannerTags._id); no
// label lookup is involved, matching rdio's Talkgroups.Write.
func writeTalkgroups(tx *sql.Tx, systemID int, v any) error {
	rows := asArrayOfMaps(v)
	if rows == nil {
		// A system row with no talkgroups key: clear its talkgroups so the DB
		// matches the desired (empty) set.
		rows = []map[string]any{}
	}
	keep := make([]int, 0, len(rows))
	for _, r := range rows {
		id, ok := asInt(r["id"])
		if !ok {
			continue
		}
		keep = append(keep, id)
	}
	if err := deleteChildOrphans(tx, "rdioScannerTalkgroups", systemID, keep); err != nil {
		return err
	}
	for _, r := range rows {
		id, ok := asInt(r["id"])
		if !ok {
			continue
		}
		if _, err := tx.Exec(
			"INSERT INTO `rdioScannerTalkgroups` "+
				"(`frequency`, `groupId`, `id`, `label`, `led`, `name`, `order`, `systemId`, `tagId`, `delay`, `alert`) "+
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "+
				"ON CONFLICT(`systemId`, `id`) DO UPDATE SET "+
				"`frequency` = excluded.`frequency`, `groupId` = excluded.`groupId`, "+
				"`label` = excluded.`label`, `led` = excluded.`led`, `name` = excluded.`name`, "+
				"`order` = excluded.`order`, `tagId` = excluded.`tagId`, "+
				"`delay` = excluded.`delay`, `alert` = excluded.`alert`",
			asNullInt(r["frequency"]), asIntDefault(r["groupId"], 0), id, asStr(r["label"]),
			asNullStr(r["led"]), asStr(r["name"]), asIntDefault(r["order"], 0), systemID,
			asIntDefault(r["tagId"], 0), asIntDefault(r["delay"], 0), asStr(r["alert"]),
		); err != nil {
			return err
		}
	}
	return nil
}

// writeUnits reconciles one system's units by (systemId, id).
func writeUnits(tx *sql.Tx, systemID int, v any) error {
	rows := asArrayOfMaps(v)
	if rows == nil {
		rows = []map[string]any{}
	}
	keep := make([]int, 0, len(rows))
	for _, r := range rows {
		id, ok := asInt(r["id"])
		if !ok {
			continue
		}
		keep = append(keep, id)
	}
	if err := deleteChildOrphans(tx, "rdioScannerUnits", systemID, keep); err != nil {
		return err
	}
	for _, r := range rows {
		id, ok := asInt(r["id"])
		if !ok {
			continue
		}
		if _, err := tx.Exec(
			"INSERT INTO `rdioScannerUnits` (`id`, `label`, `order`, `systemId`) VALUES (?, ?, ?, ?) "+
				"ON CONFLICT(`systemId`, `id`) DO UPDATE SET "+
				"`label` = excluded.`label`, `order` = excluded.`order`",
			id, asStr(r["label"]), asIntDefault(r["order"], 0), systemID,
		); err != nil {
			return err
		}
	}
	return nil
}

// writeApiKeys reconciles rdioScannerApiKeys by `key` (its UNIQUE column, and
// the natural identity of an API key). _id autoincrements — nothing references
// it. The `systems` grant is stored as JSON text, with the "*" wildcard stored
// as the JSON string `"*"`, exactly as rdio's Apikeys.Write does.
func writeApiKeys(tx *sql.Tx, v any) error {
	rows := asArrayOfMaps(v)
	if rows == nil {
		return nil
	}
	keep := make([]string, 0, len(rows))
	for _, r := range rows {
		k := asStr(r["key"])
		if k == "" {
			continue
		}
		keep = append(keep, k)
	}
	if err := deleteOrphansStr(tx, "rdioScannerApiKeys", "key", keep); err != nil {
		return err
	}
	for _, r := range rows {
		k := asStr(r["key"])
		if k == "" {
			continue // a keyless apiKey cannot be uploaded against; skip it
		}
		if _, err := tx.Exec(
			"INSERT INTO `rdioScannerApiKeys` (`disabled`, `ident`, `key`, `order`, `systems`) "+
				"VALUES (?, ?, ?, ?, ?) "+
				"ON CONFLICT(`key`) DO UPDATE SET "+
				"`disabled` = excluded.`disabled`, `ident` = excluded.`ident`, "+
				"`order` = excluded.`order`, `systems` = excluded.`systems`",
			asBoolInt(r["disabled"]), asStr(r["ident"]), k, asNullInt(r["order"]),
			systemsJSON(r["systems"]),
		); err != nil {
			return err
		}
	}
	return nil
}

// writeDownstreams reconciles rdioScannerDownstreams by _id. The apply layer
// emits a single downstream (_id=1) pointing at the agent relay; apiKey is no
// longer UNIQUE (removed in a v6.0.2 migration) so _id is the reconcile key,
// matching rdio's Downstreams.Write.
func writeDownstreams(tx *sql.Tx, v any) error {
	rows := asArrayOfMaps(v)
	if rows == nil {
		return nil
	}
	keep := make([]int, 0, len(rows))
	for _, r := range rows {
		id, ok := asInt(r["_id"])
		if !ok {
			continue
		}
		keep = append(keep, id)
	}
	if err := deleteOrphans(tx, "rdioScannerDownstreams", "_id", keep); err != nil {
		return err
	}
	for _, r := range rows {
		id, ok := asInt(r["_id"])
		if !ok {
			if _, err := tx.Exec(
				"INSERT INTO `rdioScannerDownstreams` (`apiKey`, `disabled`, `order`, `systems`, `url`) "+
					"VALUES (?, ?, ?, ?, ?)",
				asStr(r["apiKey"]), asBoolInt(r["disabled"]), asNullInt(r["order"]),
				systemsJSON(r["systems"]), asStr(r["url"]),
			); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.Exec(
			"INSERT INTO `rdioScannerDownstreams` (`_id`, `apiKey`, `disabled`, `order`, `systems`, `url`) "+
				"VALUES (?, ?, ?, ?, ?, ?) "+
				"ON CONFLICT(`_id`) DO UPDATE SET "+
				"`apiKey` = excluded.`apiKey`, `disabled` = excluded.`disabled`, "+
				"`order` = excluded.`order`, `systems` = excluded.`systems`, `url` = excluded.`url`",
			id, asStr(r["apiKey"]), asBoolInt(r["disabled"]), asNullInt(r["order"]),
			systemsJSON(r["systems"]), asStr(r["url"]),
		); err != nil {
			return err
		}
	}
	return nil
}

// deleteOrphans removes rows of table whose integer keyCol is not in keep. An
// empty keep set clears the whole table (the desired state has no such rows).
func deleteOrphans(tx *sql.Tx, table, keyCol string, keep []int) error {
	if len(keep) == 0 {
		_, err := tx.Exec("DELETE FROM `" + table + "`")
		return err
	}
	ph, args := intPlaceholders(keep)
	q := "DELETE FROM `" + table + "` WHERE `" + keyCol + "` NOT IN (" + ph + ")"
	_, err := tx.Exec(q, args...)
	return err
}

// deleteOrphansStr is deleteOrphans for a string key column (apiKeys.key).
func deleteOrphansStr(tx *sql.Tx, table, keyCol string, keep []string) error {
	if len(keep) == 0 {
		_, err := tx.Exec("DELETE FROM `" + table + "`")
		return err
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(keep)), ",")
	args := make([]any, len(keep))
	for i, s := range keep {
		args[i] = s
	}
	q := "DELETE FROM `" + table + "` WHERE `" + keyCol + "` NOT IN (" + ph + ")"
	_, err := tx.Exec(q, args...)
	return err
}

// deleteChildOrphans removes rows for one systemId whose `id` is not in keep,
// used to prune talkgroups/units removed from a system that still exists. When
// keep is empty every row for that system is removed.
func deleteChildOrphans(tx *sql.Tx, table string, systemID int, keep []int) error {
	if len(keep) == 0 {
		_, err := tx.Exec("DELETE FROM `"+table+"` WHERE `systemId` = ?", systemID)
		return err
	}
	ph, args := intPlaceholders(keep)
	args = append(args, systemID)
	q := "DELETE FROM `" + table + "` WHERE `id` NOT IN (" + ph + ") AND `systemId` = ?"
	_, err := tx.Exec(q, args...)
	return err
}

func intPlaceholders(vals []int) (string, []any) {
	ph := strings.TrimSuffix(strings.Repeat("?,", len(vals)), ",")
	args := make([]any, len(vals))
	for i, v := range vals {
		args[i] = v
	}
	return ph, args
}

// systemsJSON serialises an apiKey/downstream `systems` grant to the text rdio
// stores: the wildcard string "*" becomes the JSON string `"*"`, an array is
// marshalled as-is, and anything else falls back to `"*"` (grant-all) rather
// than an empty grant that would silently drop every call.
func systemsJSON(v any) string {
	switch t := v.(type) {
	case string:
		if t == "*" {
			return `"*"`
		}
		return t
	case []any:
		if b, err := json.Marshal(t); err == nil {
			return string(b)
		}
	}
	return `"*"`
}

// --- value coercion helpers (JSON-decoded map values) ---

func asArrayOfMaps(v any) []map[string]any {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(arr))
	for _, e := range arr {
		if m, ok := e.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(n))
		return i, err == nil
	default:
		return 0, false
	}
}

func asIntDefault(v any, d int) int {
	if i, ok := asInt(v); ok {
		return i
	}
	return d
}

// asNullInt returns an *int (nil → SQL NULL) for nullable integer columns
// (frequency, apiKey/downstream order).
func asNullInt(v any) any {
	if i, ok := asInt(v); ok {
		return i
	}
	return nil
}

func asBoolInt(v any) int {
	switch b := v.(type) {
	case bool:
		if b {
			return 1
		}
		return 0
	case float64:
		if b != 0 {
			return 1
		}
		return 0
	case string:
		if strings.EqualFold(strings.TrimSpace(b), "true") || b == "1" {
			return 1
		}
	}
	return 0
}

func asStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// asNullStr returns the string for a nullable text column, or nil (SQL NULL)
// when the value is absent/non-string — matches rdio storing led as NULL rather
// than an empty string when unset.
func asNullStr(v any) any {
	if s, ok := v.(string); ok && s != "" {
		return s
	}
	return nil
}
