# SDR-Trunk alias + rdio-scanner systems/groups/tags reference (verified)

For the "Global settings" editor. Sources: D:\working-dir\sdrtrunk (aliases) and
D:\working-dir\rdio-scanner (systems/groups/tags).

## SDR-Trunk alias
Alias root `<alias>` attributes: `name` (str), `list` (str, the alias-list),
`group` (str), `color` (ARGB **int**, shown via full RGB colour picker, hex
`#%06X`), `iconName` (str, named-icon dropdown), `stream_talkgroup_alias` (int).
Children: `id` (list of AliasID), `action` (list, rarely edited).
Priority / record / broadcast-channel are stored as `id` entries but surfaced as
top-of-form controls (Listen toggle + Priority combo / Record toggle / Streaming).

Editor layout (mirror): top grid — **Name** (text), **Listen** (toggle) +
**Priority** (dropdown: Default, or 1–99; disabled unless Listen on), **Colour**
(RGB picker), **Group** (text w/ autocomplete), **Record** (toggle), **Icon**
(dropdown). Then an **Identifiers** list with a protocol-grouped "Add identifier"
menu + a per-type sub-editor; a **Streaming** panel (broadcast channels + "stream
as talkgroup" int); Actions (optional).

### AliasID types (type= wire value → fields → control)
- `talkgroup`: value(int, text w/ per-protocol formatter), protocol(label)
- `talkgroupRange`: min,max(int), protocol(label)
- `radio`: value(int), protocol {APCO25,DMR,PASSPORT}
- `radioRange`: min,max(int), protocol
- `p25FullyQualifiedTalkgroup`: wacn,system,value (3 number fields)
- `p25FullyQualifiedRadio`: wacn,system,value
- `dcs`: code (dropdown, DCSCode)
- `esn`: esn (text, `*` wildcard)
- `loJackFunctionAndID`: function(dropdown), id(text, 5 char)
- `statusID`: status(int 0–255); `unitStatusID`: status(int 0–255)
- `tones`: tone sequence (complex; keep generic)
- `priority`: priority(int; -1=no-monitor, 1–99, 100=default) — top toggle+combo
- `record`: (no fields) — top toggle
- `broadcastChannel`: channel(str) — streaming panel
Protocol picker is protocol-FIRST (choose protocol → valid id types); protocol is
fixed on an id once created (shown as a label).
TALKGROUP protocols: AM,APCO25,DMR,FLEETSYNC,LTR,LTR_NET,MDC1200,MPT1327,NBFM,PASSPORT.

The renderer stores each id generically as {type, attrs:{...}} which round-trips
any type; the editor should show proper inputs for the common ones above and fall
back to generic key/value for the rest. Alias schema should also carry `iconName`
and `streamTalkgroupAlias`.

## rdio-scanner (Go server structs + Angular admin)
Relationship: **Systems** own nested `talkgroups[]` + `units[]`. **Groups** and
**Tags** are GLOBAL flat lists {_id, label}. A talkgroup references them by numeric
`groupId`/`tagId` (= group/tag `_id`). LED = named-colour **dropdown** (NOT a
picker): Default(null) + blue,cyan,green,magenta,orange,red,white,yellow.

**System**: `id`(uint, number), `label`(text), `led`(dropdown), `order`(drag,
hidden), `autoPopulate`(toggle), `transcribe`(toggle, default true),
`transcriptionPrompt`(textarea), `blacklists`(textarea, comma-sep ids),
`delay`(number), `alert`(dropdown None+alert1..9), `talkgroups[]`, `units[]`.
**Talkgroup**: `id`(number), `label`(text), `name`(text), `groupId`(dropdown of
global groups, value=_id), `tagId`(dropdown of global tags), `led`(dropdown),
`frequency`(number), `transcribe`(toggle), `delay`(number), `alert`(dropdown).
**Unit**: `id`(number), `label`(text).
**Group**: `_id`(hidden), `label`(text).  **Tag**: `_id`, `label` (same as Group).

Admin layout: accordion of per-system panels (label/control rows) with nested
Talkgroups + Units accordions (one expandable panel per row, drag-reorder, New/
Delete). Groups & Tags tabs = flat list, each row a label text input + delete;
new _id = max+1; auto-sorted by label. No led/alert on Group or Tag. No colour
picker anywhere in rdio — led is always a named-colour select.
