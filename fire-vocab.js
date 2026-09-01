/**
 * Shared fire-agency vocabulary.
 *
 * Six feeds describe the same handful of real things in their own words.
 * A vegetation fire is "Bush Fire" to the RFS, "Bushfire" to the NT and
 * WA, and "FIRE VEGETATION" to Queensland. A fire nobody has contained
 * yet is "Not yet controlled", "Going", "Active" or "Responding"
 * depending on who is publishing. Left alone that produced twenty type
 * pills for eight kinds of fire, and four colours for one state.
 *
 * This is the single definition map.html and logs.html both read, the
 * same way livetraffic-vocab.js is the single definition of the traffic
 * feeds. Three axes, each with its own job:
 *
 *   TYPE    what kind of thing it is      -> the icon
 *   STATUS  how far from controlled       -> the colour
 *   LEVEL   what the public is told       -> the AWS triangle
 *
 * Colour and shape never encode the same thing: the icon says what it
 * is, the colour says how it is going, and a published warning level
 * replaces the pin entirely with the standard triangle.
 *
 * NOTHING HERE DISCARDS WHAT AN AGENCY SAID. Every canon function
 * returns a canonical key for filtering, icons and colour, and the raw
 * string is kept for the detail panel — `describe()` returns both, so a
 * panel can show "Being contained" and note that QFD published it as
 * "Going". Unrecognised wording keeps itself rather than being filed
 * under Other, so a new agency shows up as itself and can be added here
 * deliberately.
 *
 * Load order matters: this file must come before the page's own script.
 */
(function () {
  'use strict';

  const clean = (v) => String(v == null ? '' : v).replace(/\s+/g, ' ').trim();
  const low = (v) => clean(v).toLowerCase();

  // ------------------------------------------------------------------
  // TYPE — what kind of thing it is. Drives the icon and the type pills.
  //
  // Only wordings are folded, never distinctions: a grass fire stays
  // separate from a bush fire because the agencies mean different things
  // by them. The deliberate burns are the one judgement call — whether a
  // burn is a landholder's or the agency's own matters less, on a live
  // incident map, than that it was lit on purpose.
  //
  // The tail of non-fire types is not an accident. The RFS feed carries
  // the rescues, hazmat and storm jobs its brigades attend, and they
  // arrive on the Fires layer because that is the feed they come in on.
  // Naming them is better than letting each agency's wording through as
  // its own pill.
  // ------------------------------------------------------------------
  const TYPES = [
    { id: 'bush', label: 'Bush Fire', icon: 'fa-tree', fire: true,
      match: /^(bush\s*fire|fire vegetation|vegetation fire|forest fire)$/ },
    { id: 'grass', label: 'Grass Fire', icon: 'fa-seedling', fire: true,
      match: /^(grass(\s*(and|&|\/)\s*scrub)?\s*fire|scrub fire|grass and scrub fire)$/ },
    { id: 'structure', label: 'Structure Fire', icon: 'fa-house-fire', fire: true,
      match: /^(structure fire|fire structure|building fire|house fire)$/ },
    { id: 'vehicle', label: 'Vehicle Fire', icon: 'fa-car-burst', fire: true,
      match: /^(vehicle fire|fire vehicle|car fire|truck fire|vehicle\/equipment fire|equipment fire)$/ },
    { id: 'planned', label: 'Planned Burn', icon: 'fa-fire-flame-simple', fire: true,
      match: /^(planned burn|hazard reduction|burn ?off|fire permitted burn|permit(ted)? burn)$/ },
    { id: 'alarm', label: 'Fire Alarm', icon: 'fa-bell', fire: true,
      match: /^(automatic fire alarm|fire alarm( \(afa\))?|afa|active alarm|alarm bells ringing)$/ },
    { id: 'other_fire', label: 'Other Fire', icon: 'fa-fire', fire: true,
      match: /^(fire|fire other|other fire|non structure fire|rubbish fire|haystack fire|smoke complaint\/illegal burn|fire had occurred)$/ },
    // Not fires. They ride in on a fire agency's feed.
    { id: 'rescue', label: 'Rescue & Crash', icon: 'fa-helmet-safety', fire: false,
      match: /^(mva\/transport|road crash|rescue road crash|rescue technical|search\/rescue|rescue flood|medical|rescue)$/ },
    { id: 'hazmat', label: 'Hazmat', icon: 'fa-flask', fire: false,
      match: /^(hazmat|hazmat all|hazmat incident|hazardous materials|chemical)$/ },
    { id: 'storm', label: 'Storm & Flood', icon: 'fa-water', fire: false,
      match: /^(flood\/storm\/tree down|riverine flood|flood|storm|tree down)$/ },
    { id: 'other', label: 'Other Incident', icon: 'fa-circle-info', fire: false,
      match: /^(other|incident|planned event|assist other agency|weather|earthquake|power \/ gas.*)$/ },
  ];

  const TYPE_BY_ID = {};
  TYPES.forEach((t) => { TYPE_BY_ID[t.id] = t; });

  /**
   * Canonical type label for an agency's own wording.
   *
   * Unrecognised wording keeps itself — a new agency shows up as itself
   * rather than being silently filed under Other — but Queensland sends
   * its types in caps, which would otherwise sit oddly beside the rest.
   */
  function canonType(raw) {
    const t = clean(raw);
    if (!t) return 'Other Fire';
    const l = t.toLowerCase();
    for (const type of TYPES) if (type.match.test(l)) return type.label;
    return t === t.toUpperCase() && /[A-Z]{2,}/.test(t)
      ? t.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase())
      : t;
  }

  function typeEntry(raw) {
    const label = canonType(raw);
    return TYPES.find((t) => t.label === label) || null;
  }

  function typeIcon(raw) {
    const e = typeEntry(raw);
    return e ? e.icon : 'fa-fire';
  }

  // ------------------------------------------------------------------
  // STATUS — how far from controlled. Drives the colour.
  //
  // ORDER IS LOAD-BEARING. "Not Yet Under Control" contains "under
  // control" and "Out of control" contains "out", so the worst cases
  // have to be tested before the calmer ones they read like.
  // ------------------------------------------------------------------
  const STATUSES = [
    { id: 'uncontained', label: 'Out of control', ring: '#f87171', a: '#ff6b6b', b: '#b91c1c',
      match: /out of control|not yet (under )?control|^going$|^active$|responding|spreading|on scene|^warning$/ },
    { id: 'containing', label: 'Being contained', ring: '#fdba74', a: '#fb923c', b: '#c2410c',
      match: /being controlled|contained|in progress|under investigation/ },
    { id: 'controlled', label: 'Under control', ring: '#86efac', a: '#4ade80', b: '#15803d',
      match: /under control|patrolled|monitoring|^safe$|^out$|extinguish/ },
    { id: 'closed', label: 'Closed', ring: '#cbd5e1', a: '#94a3b8', b: '#475569',
      match: /closed|complete|finalis|inactive/ },
  ];
  const STATUS_UNKNOWN = {
    id: 'unknown', label: 'Not published', ring: '#cbd5e1', a: '#94a3b8', b: '#475569',
  };

  /**
   * A deliberate burn that is alight is doing what it was lit to do.
   *
   * WA marks all 35 of its burn-offs "Active" and Queensland marks its
   * permitted burns "Going" — the same words those agencies use for a
   * bushfire nobody has contained — so a state full of routine hazard
   * reduction was drawn as a state on fire. The NSW RFS publishes the
   * same thing as "Under control", which is the reading that matches
   * reality, so a planned burn under way takes the controlled colour
   * and says plainly what it is.
   *
   * Only the label is new. The bucket and the palette are the existing
   * controlled ones, so nothing downstream has to learn a fifth state.
   */
  const BURNING_AS_PLANNED = /^(active|going|in progress|alight|burning|underway|under way)$/;
  const STATUS_PLANNED = {
    id: 'controlled', label: 'Burning as planned',
    ring: '#86efac', a: '#4ade80', b: '#15803d',
  };

  /**
   * Words that appear in a status field but are not a containment state.
   *
   * Queensland files its warnings' CallToAction as their status, so a
   * fire that is very much burning arrives labelled "Avoid Smoke" or
   * "Stay Informed" — instructions to the public, not a state. Victoria
   * sometimes sends a severity ("Minor") and the NT sometimes sends the
   * incident TYPE ("Planned Burn"). All three used to be matched by the
   * closed bucket or fall through to unknown, which drew a live fire in
   * finished grey. They are recognised here so the UI can say the status
   * was not published rather than repeat a misleading word.
   *
   * The real fix for the Queensland half is one line in qldFire.ts —
   * warnings should carry no status, the way the WA source does — but
   * that is a backend change and this keeps the display honest either way.
   */
  const NOT_A_STATUS = /^(avoid smoke.*|stay informed|leave immediately|watch and act|advice|minor|moderate|major|severe|extreme|planned burn|hazard reduction|burn ?off)$/;

  function isStatus(raw) {
    const t = low(raw);
    return !!t && !NOT_A_STATUS.test(t);
  }

  function statusEntry(raw, type) {
    const t = low(raw);
    if (!t || NOT_A_STATUS.test(t)) return STATUS_UNKNOWN;
    // Status is read in the context of what is burning: see
    // BURNING_AS_PLANNED. Tested first, because the words it matches are
    // the same ones that mean "out of control" on an unplanned fire.
    if (type && canonType(type) === 'Planned Burn' && BURNING_AS_PLANNED.test(t)) {
      return STATUS_PLANNED;
    }
    for (const s of STATUSES) if (s.match.test(t)) return s;
    return STATUS_UNKNOWN;
  }

  const statusBucket = (raw, type) => statusEntry(raw, type).id;
  const statusColor = (raw, type) => statusEntry(raw, type);
  const statusLabel = (raw, type) => statusEntry(raw, type).label;

  // ------------------------------------------------------------------
  // LEVEL — the Australian Warning System level, when one is published.
  //
  // Every agency publishes the same three levels in slightly different
  // words: QFD says "Leave Immediately" where the RFS says "Emergency
  // Warning", and a CFA warning's call to action reads "Stay Informed"
  // for an Advice.
  // ------------------------------------------------------------------
  const LEVELS = [
    { id: 'emergency', label: 'Emergency Warning', icon: 'assets/rfs-emergency.svg', color: '#D32027',
      match: /emergency warning|leave immediately|evacuate/ },
    { id: 'watchact', label: 'Watch and Act', icon: 'assets/rfs-watch-and-act.svg', color: '#F17131',
      match: /watch and act|watch & act|prepare to leave|^prepare/ },
    { id: 'advice', label: 'Advice', icon: 'assets/rfs-advice.svg', color: '#FFD44F',
      match: /advice|stay informed/ },
  ];
  const LEVEL_NONE = { id: 'other', label: 'None published', icon: null, color: '#94a3b8' };

  /**
   * Values that sit in an alert-level field without being a level.
   *
   * The RFS writes "Not Applicable" on 21 records and the incident TYPE
   * — "Planned Burn" — on 11 more. Both are the absence of a warning,
   * not a warning, so a panel should show a dash rather than announce
   * "WARNING LEVEL: Planned Burn" as though it meant something.
   *
   * Tested before the level patterns so "Planned Burn Advice", which the
   * NT does publish as a genuine Advice for a planned burn, still reads
   * as an Advice.
   */
  const NOT_A_LEVEL = /^(not applicable|n\/?a|none|nil|planned burn|hazard reduction|burn ?off|-+)$/;

  function isLevel(raw) {
    const t = low(raw);
    return !!t && !NOT_A_LEVEL.test(t) && LEVELS.some((l) => l.match.test(t));
  }

  function levelEntry(raw) {
    const t = low(raw);
    if (!t || NOT_A_LEVEL.test(t)) return LEVEL_NONE;
    for (const l of LEVELS) if (l.match.test(t)) return l;
    return LEVEL_NONE;
  }

  const levelBucket = (raw) => levelEntry(raw).id;
  const levelIcon = (raw) => levelEntry(raw).icon;
  const levelColor = (raw) => levelEntry(raw).color;
  const levelLabel = (raw) => levelEntry(raw).label;

  // ------------------------------------------------------------------
  // AGENCIES — one label per short code, so a pill means the same agency
  // wherever its records happened to land. Routing is by event, so a CFA
  // hazmat call is on Hazards and an SES flood warning is on Floods.
  // ------------------------------------------------------------------
  const AGENCIES = {
    rfs: { label: 'NSW RFS', color: '#ef4444' },
    nt_fire: { label: 'NT Fire & Rescue', color: '#f97316' },
    NTFRS: { label: 'NT Fire & Rescue', color: '#f97316' },
    qld: { label: 'QLD Fire Dept', color: '#dc2626' },
    QFD: { label: 'QLD Fire Dept', color: '#dc2626' },
    dfes: { label: 'DFES (WA)', color: '#e11d48' },
    DFES: { label: 'DFES (WA)', color: '#e11d48' },
    CFA: { label: 'CFA (Vic)', color: '#6366f1' },
    DEECA: { label: 'DEECA (Vic)', color: '#84cc16' },
    EMV: { label: 'EMV (Vic)', color: '#818cf8' },
    // Not a fire service. Victoria's 000 call-taking and dispatch
    // agency, which the feed names as the source on jobs no
    // responding brigade is attributed to — so the label has to say
    // what it is, or it reads as a phone number sitting between CFA
    // and the SES.
    ESTA: { label: 'Triple Zero Vic (dispatch)', color: '#a78bfa' },
    SES: { label: 'SES', color: '#f59e0b' },
  };
  const agencyLabel = (key) => (AGENCIES[key] && AGENCIES[key].label) || String(key || '');
  const agencyColor = (key) => (AGENCIES[key] && AGENCIES[key].color) || '#94a3b8';

  /**
   * Everything about one record's three axes, canonical and raw.
   *
   * `published` is set only where the agency's own wording differs from
   * the canonical label, which is exactly when a detail panel should
   * show both.
   */
  function describe(raw) {
    const r = raw || {};
    const type = canonType(r.type);
    const st = statusEntry(r.status, r.type);
    const lv = levelEntry(r.level);
    const rawStatus = clean(r.status);
    const rawType = clean(r.type);
    const rawLevel = clean(r.level);
    return {
      type: { label: type, icon: typeIcon(r.type), raw: rawType,
        published: rawType && rawType !== type ? rawType : null },
      status: { id: st.id, label: st.label, ring: st.ring, a: st.a, b: st.b, raw: rawStatus,
        real: isStatus(r.status),
        published: rawStatus && rawStatus !== st.label ? rawStatus : null },
      level: { id: lv.id, label: lv.label, icon: lv.icon, color: lv.color, raw: rawLevel,
        real: isLevel(r.level),
        published: rawLevel && rawLevel !== lv.label ? rawLevel : null },
    };
  }

  window.FireVocab = {
    TYPES, STATUSES, LEVELS, AGENCIES,
    canonType, typeEntry, typeIcon,
    statusBucket, statusColor, statusLabel, statusEntry, isStatus,
    levelBucket, levelIcon, levelColor, levelLabel, levelEntry, isLevel,
    agencyLabel, agencyColor,
    describe,
  };
})();
