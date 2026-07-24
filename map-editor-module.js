/* ==========================================================================
   NSW PSN — Map Editor Module
   ==========================================================================
   Extracted from map-editor.html so the public map (map.html) and the
   editor are ONE page. Loaded on map.html for everyone, but stays dormant
   until a signed-in user passes the server-side role check
   (/api/check-editor -> has_access). Editors then get the floating editor
   panel, pin create/edit/delete, suggestions, logs and grammar tools on
   top of the normal public map.

   Depends on globals provided by map.html + auth-common.js:
     map, L, sb, PROXY_BASE, API_BASE_URL, API_KEY, escapeHtml,
     haversineMeters, buildUserIncidentTooltip, isWithinMinutes,
     marked, DOMPurify, userLayer (public user-incident layer).
   All editor internals stay private to this IIFE; only the functions
   referenced by inline on* handlers are exported onto window.
   ========================================================================== */
(function () {
  'use strict';

  // --- Editor state (was global on map-editor.html) ---
  let addMode = false;
  let selectedId = null;
  let currentIncident = null;
  let currentUserId = null;
  let currentIsAdmin = false;
  let currentUserEmail = null;

    // Curated from each agency's documented taxonomies: RFS CAP-feed
    // IncidentTypes (Bush Fire / Grass Fire / Hazard Reduction / Pile
    // Burn / AFA...), FRNSW eAIRS classes (hazmat, rescue/USAR, special
    // service, storm), NSW Ambulance AMPDS chief complaints (condensed
    // to map-useful buckets), SES statutory + specialist categories,
    // and police operations. Grouped for the picker; the flat list
    // keeps every existing consumer working.
    const INCIDENT_TYPE_GROUPS = [
      { label: 'Fire', types: [
        "Bush Fire", "Grass Fire", "Structure Fire", "Vehicle Fire",
        "Hazard Reduction", "Pile Burn", "Fire Alarm (AFA)"
      ] },
      { label: 'Rescue', types: [
        "MVA / Transport", "Road Crash Rescue", "Search & Rescue",
        "Vertical Rescue", "Land Rescue", "Flood Rescue", "Animal Rescue"
      ] },
      { label: 'Medical', types: [
        "Cardiac Arrest", "Medical Emergency", "Overdose / Poisoning",
        "Mental Health / Self-harm"
      ] },
      { label: 'Hazard / Weather', types: [
        "Hazmat", "Storm Damage", "Flood", "Tree Down / Powerlines"
      ] },
      { label: 'Other', types: [
        "Police Operation", "Assist Other Agency", "Resupply",
        "Planned Event", "Other"
      ] }
    ];
    const INCIDENT_TYPES = INCIDENT_TYPE_GROUPS.flatMap((g) => g.types);
    const AGENCY_COLORS = {
      'RFS': '#ff6600',
      'FRNSW': '#ef4444',
      'NSWAS': '#3b82f6',
      'SES': '#eab308',
      'POLICE': '#3b82f6',
      'VRA': '#ffffff',
      'AVIATION': '#a855f7'
    };
    const TYPE_COLORS = {
      'FIRE': '#ef4444',
      'RESCUE': '#ff6600',
      'HAZARD': '#eab308',
      'POLICE': '#3b82f6',
      'MEDICAL': '#3b82f6',
      'DEFAULT': '#000000'
    };
    // --- Responding units (callsigns) -----------------------------------
    // Editors attach unit callsigns to a call. Every saved callsign goes
    // into a persistent dictionary (backend `callsigns` table) so anyone
    // typing the same one later can Tab-complete it. Enter adds.
    let currentUnits = [];
    let _callsignDict = [];
    // Non-user contexts (RFS incidents, pager clusters) have no Save
    // button — their units persist immediately against a shared stub
    // incident row (same stub mechanism the RFS log flow uses).
    let _unitsStub = null; // { id, title, lat, lng, tag } | null
    async function ensureStubIncident(stub) {
      try {
        const check = await apiFetch(`${PROXY_BASE}/api/incidents/${stub.id}`);
        if (check.ok) return true;
      } catch (e) { /* fall through to create */ }
      const expireTime = new Date(Date.now() + 48 * 3600000);
      const res = await apiFetch(`${PROXY_BASE}/api/incidents`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: stub.id,
          title: stub.title,
          lat: stub.lat || 0,
          lng: stub.lng || 0,
          is_rfs_stub: true,
          type: [stub.tag],
          status: 'Monitoring',
          description: 'Auto-created stub for ' + stub.tag + ' log.',
          expires_at: expireTime
        })
      });
      return res.ok;
    }
    async function persistStubUnits() {
      if (!_unitsStub) return;
      const stub = _unitsStub;
      const ok = await ensureStubIncident(stub);
      if (!ok || _unitsStub !== stub) { if (!ok) showToast('Failed to save units.', 'error'); return; }
      const res = await apiFetch(`${PROXY_BASE}/api/incidents/${stub.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ units: currentUnits, updated_at: new Date() })
      });
      if (!res.ok) showToast('Failed to save units.', 'error');
    }
    // In the RFS/pager read-only views the units input reads best just
    // above the Incident Logs (under the read-only details block); user
    // pins keep it in the form between agencies and description. Moving
    // the same node preserves its input listeners.
    function placeUnitsGroup(beforeLogs) {
      const g = document.getElementById('units-group');
      if (!g) return;
      if (beforeLogs) {
        const logs = document.getElementById('log-section');
        if (logs && logs.parentNode) logs.parentNode.insertBefore(g, logs);
      } else {
        const desc = document.getElementById('desc-group');
        if (desc && desc.parentNode) desc.parentNode.insertBefore(g, desc);
      }
      // Photos travel with the units group so the two stay together in
      // every context (user form, RFS block, pager cluster).
      const p = document.getElementById('photos-group');
      if (p && g.parentNode) g.parentNode.insertBefore(p, g.nextSibling);
    }
    async function loadStubUnits(stub) {
      try {
        const res = await apiFetch(`${PROXY_BASE}/api/incidents/${stub.id}`);
        if (!res.ok) return;
        const j = await res.json();
        if (_unitsStub && _unitsStub.id === stub.id) {
          currentUnits = Array.isArray(j.units) ? j.units : [];
          renderUnitChips();
          currentImages = Array.isArray(j.images) ? j.images : [];
          renderPhotoTiles();
        }
      } catch (e) { /* stays empty */ }
    }
    async function loadCallsignDict() {
      try {
        const res = await apiFetch(`${PROXY_BASE}/api/incidents/callsigns`);
        if (res.ok) {
          const d = await res.json();
          _callsignDict = Array.isArray(d.callsigns) ? d.callsigns : [];
        }
      } catch (e) { /* completion just stays empty */ }
    }
    // --- Incident photos ------------------------------------------------
    // Up to 4 images per incident, 50MB each. Uploads commit immediately
    // against the incident row (they are NOT part of the Save payload), so
    // RFS/pager contexts create their stub first, exactly like units do.
    // Files are stored on the webroot and resized by Cloudflare on demand.
    const MAX_INCIDENT_IMAGES = 4;
    const MAX_IMAGE_BYTES = 50 * 1024 * 1024;
    const IMAGE_ORIGIN = 'https://nswpsn.forcequit.xyz';
    let currentImages = [];
    let _photoBusy = false;

    // Cloudflare Image Transformations URL. Falls back (via onerror on the
    // <img>) to the original file if transformations aren't enabled.
    function cfImg(filePath, width) {
      return IMAGE_ORIGIN + '/cdn-cgi/image/width=' + width +
        ',quality=78,format=auto' + filePath;
    }

    // Which incident do photos attach to? User pins use the selected id;
    // RFS/pager views attach to their shared stub row (created on demand).
    async function _photoTargetId() {
      if (_unitsStub) {
        const ok = await ensureStubIncident(_unitsStub);
        return ok ? _unitsStub.id : null;
      }
      return selectedId || null;
    }

    function renderPhotoTiles() {
      const box = document.getElementById('photo-tiles');
      const countEl = document.getElementById('photo-count');
      if (!box) return;
      if (countEl) {
        countEl.textContent = currentImages.length
          ? '(' + currentImages.length + '/' + MAX_INCIDENT_IMAGES + ')'
          : '';
      }
      box.innerHTML = '';

      currentImages.forEach((img) => {
        const tile = document.createElement('div');
        tile.style.cssText = 'position:relative; width:64px; height:64px; border-radius:6px; overflow:hidden; border:1px solid rgba(148,163,184,0.35); background:rgba(0,0,0,0.3);';
        const a = document.createElement('a');
        a.href = IMAGE_ORIGIN + img.file;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        a.title = 'Open full size';
        const el = document.createElement('img');
        el.src = cfImg(img.file, 128);
        el.alt = 'Incident photo';
        el.loading = 'lazy';
        el.style.cssText = 'width:100%; height:100%; object-fit:cover; display:block;';
        el.onerror = () => { el.onerror = null; el.src = IMAGE_ORIGIN + img.file; };
        a.appendChild(el);
        tile.appendChild(a);

        // Only the uploader (or an admin) may remove a photo.
        const mine = !!(img.uploaded_by && img.uploaded_by === currentUserId) || currentIsAdmin;
        if (mine) {
          const x = document.createElement('button');
          x.type = 'button';
          x.textContent = '\u00d7';
          x.title = 'Remove photo';
          x.style.cssText = 'position:absolute; top:2px; right:2px; width:18px; height:18px; padding:0; line-height:1; border:0; border-radius:4px; background:rgba(15,23,42,0.85); color:#fca5a5; cursor:pointer; font-size:0.85rem;';
          x.onclick = (ev) => { ev.preventDefault(); deleteIncidentPhoto(img.id); };
          tile.appendChild(x);
        }
        box.appendChild(tile);
      });

      if (_photoBusy) {
        const busy = document.createElement('div');
        busy.style.cssText = 'width:64px; height:64px; border-radius:6px; border:1px dashed rgba(148,163,184,0.4); display:flex; align-items:center; justify-content:center; color:#94a3b8;';
        busy.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
        box.appendChild(busy);
      } else if (currentImages.length < MAX_INCIDENT_IMAGES) {
        const add = document.createElement('button');
        add.type = 'button';
        add.title = 'Add a photo';
        add.style.cssText = 'width:64px; height:64px; border-radius:6px; border:1px dashed rgba(148,163,184,0.4); background:rgba(255,255,255,0.03); color:#94a3b8; cursor:pointer; font-size:1.1rem;';
        add.innerHTML = '<i class="fas fa-plus"></i>';
        add.onclick = () => {
          const input = document.getElementById('photo-input');
          if (input) { input.value = ''; input.click(); }
        };
        box.appendChild(add);
      }
    }

    async function uploadIncidentPhoto(file) {
      if (!file) return;
      if (currentImages.length >= MAX_INCIDENT_IMAGES) {
        showToast('Up to ' + MAX_INCIDENT_IMAGES + ' photos per incident.', 'error');
        return;
      }
      const allowed = ['image/jpeg', 'image/png', 'image/webp', 'image/gif'];
      if (allowed.indexOf(file.type) === -1) {
        showToast('Photos must be JPEG, PNG, WebP or GIF.', 'error');
        return;
      }
      if (file.size > MAX_IMAGE_BYTES) {
        showToast('That photo is over the 50MB limit.', 'error');
        return;
      }
      const targetId = await _photoTargetId();
      if (!targetId) {
        showToast('Select or save the incident before adding photos.', 'error');
        return;
      }
      _photoBusy = true;
      renderPhotoTiles();
      try {
        const res = await apiFetch(`${PROXY_BASE}/api/incidents/${targetId}/images`, {
          method: 'POST',
          headers: { 'Content-Type': file.type },
          body: file
        });
        if (!res.ok) {
          let msg = 'Failed to upload photo.';
          try { const j = await res.json(); if (j && j.error) msg = j.error; } catch (e) {}
          showToast(msg, 'error');
          return;
        }
        const data = await res.json();
        if (data && data.image) {
          currentImages.push(data.image);
          showToast('Photo added.', 'success');
        }
      } catch (e) {
        showToast('Failed to upload photo.', 'error');
      } finally {
        _photoBusy = false;
        renderPhotoTiles();
      }
    }

    async function deleteIncidentPhoto(imageId) {
      const ok = await askConfirm('Remove this photo? This cannot be undone.', {
        confirmLabel: 'Remove', danger: true,
      });
      if (!ok) return;
      const targetId = _unitsStub ? _unitsStub.id : selectedId;
      if (!targetId) return;
      try {
        const res = await apiFetch(
          `${PROXY_BASE}/api/incidents/${targetId}/images/${imageId}`,
          { method: 'DELETE' }
        );
        if (!res.ok) {
          let msg = 'Failed to remove photo.';
          try { const j = await res.json(); if (j && j.error) msg = j.error; } catch (e) {}
          showToast(msg, 'error');
          return;
        }
        currentImages = currentImages.filter((i) => i.id !== imageId);
        renderPhotoTiles();
      } catch (e) {
        showToast('Failed to remove photo.', 'error');
      }
    }

    function wirePhotoInput() {
      const input = document.getElementById('photo-input');
      if (!input || input._wired) return;
      input._wired = true;
      input.addEventListener('change', () => {
        const file = input.files && input.files[0];
        input.value = '';
        if (file) uploadIncidentPhoto(file);
      });
    }

    function renderUnitChips() {
      const box = document.getElementById('unit-chips');
      if (!box) return;
      box.innerHTML = '';
      currentUnits.forEach((u, i) => {
        const chip = document.createElement('span');
        chip.style.cssText = 'display:inline-flex; align-items:center; gap:0.35rem; background:rgba(34,197,94,0.15); border:1px solid rgba(34,197,94,0.5); color:#bbf7d0; font-size:0.72rem; font-weight:600; padding:0.2rem 0.5rem; border-radius:999px;';
        chip.appendChild(document.createTextNode(u));
        const x = document.createElement('button');
        x.type = 'button';
        x.textContent = '×';
        x.title = 'Remove unit';
        x.style.cssText = 'background:none; border:0; color:#86efac; cursor:pointer; font-size:0.85rem; padding:0; line-height:1;';
        x.onclick = () => {
          currentUnits.splice(i, 1);
          renderUnitChips();
          if (_unitsStub) persistStubUnits();
        };
        chip.appendChild(x);
        box.appendChild(chip);
      });
    }
    function addUnitFromInput() {
      const input = document.getElementById('unit-input');
      if (!input) return;
      const val = input.value.trim().toUpperCase();
      if (!val) return;
      if (!currentUnits.includes(val)) currentUnits.push(val);
      if (!_callsignDict.includes(val)) _callsignDict.unshift(val);
      input.value = '';
      renderUnitChips();
      if (_unitsStub) persistStubUnits();
    }
    function wireUnitInput() {
      const input = document.getElementById('unit-input');
      if (!input) return;
      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          addUnitFromInput();
        } else if (e.key === 'Tab') {
          const prefix = input.value.trim().toUpperCase();
          if (!prefix) return; // empty input: let Tab move focus normally
          const hit = _callsignDict.find((cs) =>
            cs.toUpperCase().startsWith(prefix) && !currentUnits.includes(cs.toUpperCase()));
          if (hit) { e.preventDefault(); input.value = hit; }
        }
      });
    }

    function renderTypeCheckboxes() {
      const container = document.getElementById('type-checkboxes');
      // Toggle pills (hidden checkbox drives the checked state so the
      // existing `#type-checkboxes input:checked` read + selectIncident's
      // per-value restore keep working).
      container.innerHTML = INCIDENT_TYPE_GROUPS.map(g => `
        <div class="type-group-label">${g.label}</div>
        ${g.types.map(t => `
        <label class="type-pill"><input type="checkbox" value="${t}"> ${t}</label>`).join('')}
      `).join('');
    }

    // --- Auto-remove slider -------------------------------------------
    // Discrete stops from 20 min to 12 hours. Index-based so the thumb
    // snaps to sensible values instead of arbitrary minute counts.
    const EXPIRY_STOPS_MIN = [20, 30, 45, 60, 90, 120, 180, 240, 360, 480, 720];
    const EXPIRY_DEFAULT_INDEX = 5; // 120 min = 2 hours
    function _fmtExpiry(min) {
      if (min < 60) return min + ' min';
      const h = Math.floor(min / 60), m = min % 60;
      if (!m) return h + (h === 1 ? ' hour' : ' hours');
      return h + 'h ' + m + 'm';
    }
    function _expiryIndexForMinutes(min) {
      let best = EXPIRY_DEFAULT_INDEX, bd = Infinity;
      EXPIRY_STOPS_MIN.forEach((s, i) => {
        const d = Math.abs(s - min);
        if (d < bd) { bd = d; best = i; }
      });
      return best;
    }
    function updateExpiryLabel() {
      const r = document.getElementById('edit-expiry-range');
      const lbl = document.getElementById('edit-expiry-label');
      if (r && lbl) {
        lbl.textContent = _fmtExpiry(EXPIRY_STOPS_MIN[parseInt(r.value, 10) || 0]);
        // Fill the track up to the thumb (matches the pager-hours slider).
        const pct = (parseInt(r.value, 10) / (EXPIRY_STOPS_MIN.length - 1)) * 100;
        r.style.setProperty('--percent', pct + '%');
      }
    }
    function setExpirySlider(minutes) {
      const r = document.getElementById('edit-expiry-range');
      if (!r) return;
      r.value = String(Number.isFinite(minutes) ? _expiryIndexForMinutes(minutes) : EXPIRY_DEFAULT_INDEX);
      updateExpiryLabel();
    }
    function getExpiryMinutes() {
      const r = document.getElementById('edit-expiry-range');
      const idx = r ? parseInt(r.value, 10) : EXPIRY_DEFAULT_INDEX;
      return EXPIRY_STOPS_MIN[idx] ?? EXPIRY_STOPS_MIN[EXPIRY_DEFAULT_INDEX];
    }
    // Minutes left before an incident's stored expiry, or null.
    function _minutesUntilExpiry(expiresAt) {
      if (!expiresAt) return null;
      const t = new Date(expiresAt).getTime();
      if (!Number.isFinite(t)) return null;
      return Math.round((t - Date.now()) / 60000);
    }
    async function apiFetch(url, options = {}) {
      const method = (options.method || 'GET').toUpperCase();
      const isMutating = method === 'PUT' || method === 'POST' || method === 'DELETE' || method === 'PATCH';
      // options.auth === true forces the Supabase JWT even on a GET (used by
      // owner-only reads such as the suggestions review list).
      const needsJwt = isMutating || options.auth === true;
      let bearer = API_KEY;
      if (needsJwt) {
        const { data } = await sb.auth.getSession();
        const token = data?.session?.access_token;
        if (!token) {
          // Suppress the alert for auth-forced reads (e.g. suggestion review);
          // the caller handles the rejection quietly.
          if (isMutating) {
            alert('You need to be logged in as an editor to make changes. Please log in and try again.');
          }
          return Promise.reject(new Error('apiFetch blocked: no editor session'));
        }
        bearer = token;
      } else if (!API_KEY) {
        // Reads before /api/config loads — send unauthenticated and let
        // the caller's error handling deal with the 401.
        bearer = null;
      }
      const headers = {
        ...options.headers,
        ...(bearer ? { 'Authorization': `Bearer ${bearer}` } : {})
      };
      return fetch(url, { ...options, headers });
    }
    // Rendering stays with the public unified renderer (loadAllData) so
    // user/RFS/pager pins keep merging; the editor just re-triggers it
    // after mutations.
    function reloadIncidents() {
      try { if (typeof loadAllData === 'function') loadAllData(); } catch (e) { /* non-fatal */ }
    }

    function selectIncident(inc, pagerDetails) {
      const RFS_BLOCK = document.getElementById('rfs-read-only-data');
      if (RFS_BLOCK) RFS_BLOCK.remove();

      selectedId = inc.id;
      currentIncident = inc;
      // Reset the shared header (other views retitle/recolour it).
      const userHeader = document.querySelector('#edit-header h4');
      if (userHeader) { userHeader.textContent = 'Edit Incident'; userHeader.style.color = '#7dd3fc'; }
      document.getElementById('selection-editor').style.display = 'block';
      document.getElementById('instruction-text').style.display = 'none';
      document.getElementById('edit-id').value = inc.id;
      document.getElementById('edit-id-display').textContent = "ID: " + inc.id.split('-')[0];
      document.getElementById('edit-title').value = inc.title || '';
      document.getElementById('edit-desc').value = inc.description || '';
      // Load the slider at the incident's remaining time (snapped to the
      // nearest stop, clamped into the 20m-12h range); default 2h when it
      // has no future expiry.
      {
        const left = _minutesUntilExpiry(inc.expires_at);
        setExpirySlider(left != null && left > 0 ? left : EXPIRY_STOPS_MIN[EXPIRY_DEFAULT_INDEX]);
      }
      document.getElementById('edit-status').value = inc.status || 'Going';
      document.getElementById('edit-size').value = inc.size || '';
      
      // Display location (read-only, auto-generated from coordinates)
      const locationText = inc.location || '';
      document.getElementById('edit-location-text').textContent = locationText || 'No location data';
      document.getElementById('location-group').style.display = locationText ? 'block' : 'none';

      document.getElementById('title-group').style.display = 'block';
      document.getElementById('status-size-group').style.display = 'grid';
      document.getElementById('expiry-group').style.display = 'block';
      document.getElementById('type-group').style.display = 'block';
      document.getElementById('agency-group').style.display = 'block';
      document.getElementById('desc-group').style.display = 'block';
      document.getElementById('save-delete-group').style.display = 'grid';
      document.getElementById('log-section').style.display = 'block';

      // Fresh render drops any legacy-type entries appended for the
      // previously selected pin.
      renderTypeCheckboxes();
      const typeBox = document.getElementById('type-checkboxes');
      const incTypes = Array.isArray(inc.type) ? inc.type : (inc.type ? [inc.type] : []);
      incTypes.forEach(t => {
        const match = Array.from(typeBox.querySelectorAll('input')).find(i => i.value === t);
        if (match) { match.checked = true; return; }
        // Type saved before the taxonomy cleanup — keep it selectable
        // so saving doesn't silently drop it.
        const label = document.createElement('label');
        label.style.cssText = 'display:flex; align-items:center; gap:0.4rem; font-size:0.75rem; width:45%; color:#94a3b8;';
        const cb = document.createElement('input');
        cb.type = 'checkbox';
        cb.value = t;
        cb.checked = true;
        label.appendChild(cb);
        label.appendChild(document.createTextNode(' ' + t));
        typeBox.appendChild(label);
      });

      _unitsStub = null; // user pins persist units via Save
      currentUnits = Array.isArray(inc.units) ? inc.units.slice() : [];
      renderUnitChips();
      // Photos are already persisted server-side; just mirror the row.
      currentImages = Array.isArray(inc.images) ? inc.images.slice() : [];
      renderPhotoTiles();
      placeUnitsGroup(false); // back to its form position
      const unitsGroup = document.getElementById('units-group');
      if (unitsGroup) unitsGroup.style.display = 'block';
      const photosGroup = document.getElementById('photos-group');
      if (photosGroup) photosGroup.style.display = 'block';

      const agencyInputs = document.querySelectorAll('#agency-checkboxes input');
      agencyInputs.forEach(cb => cb.checked = false);
      if (inc.responding_agencies) {
        inc.responding_agencies.forEach(agency => {
          const match = Array.from(agencyInputs).find(i => i.value === agency);
          if (match) match.checked = true;
        });
      }

      // --- Ownership-aware UI ---
      // A site admin (owner/team_member) may edit/delete any pin; otherwise
      // only the creator may. Legacy pins (created_by null) are admin-only,
      // so non-admins fall through to the suggest UI (backend enforces 403).
      const canModify = currentIsAdmin || (!!inc.created_by && inc.created_by === currentUserId);
      applyOwnershipUI(inc, canModify);

      loadIncidentLogs(inc.id);
      if (canModify) {
        loadIncidentSuggestions(inc.id);
      }
      map.panTo([inc.lat, inc.lng]);
      renderPagerSection(pagerDetails);
      updateDragGhost(inc, canModify);

      // Archive is staff/owner-only (backend enforces canManageUsers too).
      const archiveBtn = document.getElementById('btn-archive');
      if (archiveBtn) archiveBtn.style.display = currentIsAdmin ? 'block' : 'none';
    }

    // Toggle the direct-edit path vs. the suggestion path based on ownership,
    // and (re)build the injected panels fresh so no state leaks between pins.
    function applyOwnershipUI(inc, canModify) {
      const notice = document.getElementById('ownership-notice');
      const suggestPanel = document.getElementById('suggest-panel');
      const reviewPanel = document.getElementById('suggestions-review-panel');
      const editGroupIds = ['title-group', 'status-size-group', 'expiry-group', 'type-group', 'agency-group', 'desc-group'];

      // Always reset injected panels between selections.
      clearSuggestMove();
      notice.style.display = 'none';
      notice.innerHTML = '';
      suggestPanel.style.display = 'none';
      suggestPanel.innerHTML = '';
      reviewPanel.style.display = 'none';
      reviewPanel.innerHTML = '';

      if (canModify) {
        // Full edit path (existing behavior).
        editGroupIds.forEach(id => {
          const el = document.getElementById(id);
          if (el) el.style.display = (id === 'status-size-group') ? 'grid' : 'block';
        });
        document.getElementById('save-delete-group').style.display = 'grid';
        // Review panel is populated asynchronously by loadIncidentSuggestions.
      } else {
        // Non-owner: hide the direct edit form + save/delete, show suggest UI.
        editGroupIds.forEach(id => {
          const el = document.getElementById(id);
          if (el) el.style.display = 'none';
        });
        document.getElementById('save-delete-group').style.display = 'none';
        notice.style.display = 'block';
        notice.innerHTML = `<i class="fa-solid fa-circle-info" style="margin-right:0.4rem;"></i>You're not the owner of this pin — suggest changes below.`;
        buildSuggestPanel(inc);
      }
    }

    // ---- SUGGESTION WORKFLOW (non-owner path) ----
    // Build a compact "suggest an edit" + "suggest a note" panel pre-filled
    // from the incident. Fields are namespaced (sugg-*) so they never collide
    // with the real editor inputs.
    function buildSuggestPanel(inc) {
      const panel = document.getElementById('suggest-panel');
      const statusOptions = ['Going','In Route','On Scene','Out of Control','Being Controlled','Emergency Warning','Watch and Act','Advice','Under Control','Pending','Investigation','Monitor','Patrol','Off Scene','Safe'];
      const curStatus = inc.status || 'Going';
      const statusHtml = statusOptions.map(s =>
        `<option value="${escapeHtml(s)}"${s === curStatus ? ' selected' : ''}>${escapeHtml(s)}</option>`
      ).join('');

      const incTypes = Array.isArray(inc.type) ? inc.type : (inc.type ? [inc.type] : []);
      const typeHtml = INCIDENT_TYPES.map(t => {
        const checked = incTypes.includes(t) ? ' checked' : '';
        return `<label class="pill-checkbox"><input type="checkbox" class="sugg-type" value="${escapeHtml(t)}"${checked}> ${escapeHtml(t)}</label>`;
      }).join('');

      const AGENCIES = ['RFS','FRNSW','NSWAS','SES','Police','VRA'];
      const incAgencies = Array.isArray(inc.responding_agencies) ? inc.responding_agencies : [];
      const agencyHtml = AGENCIES.map(a => {
        const checked = incAgencies.includes(a) ? ' checked' : '';
        return `<label class="pill-checkbox"><input type="checkbox" class="sugg-agency" value="${escapeHtml(a)}"${checked}> ${escapeHtml(a)}</label>`;
      }).join('');

      clearSuggestMove();
      panel.style.display = 'block';
      panel.innerHTML = `
        <div style="border:1px solid rgba(168,85,247,0.35); background:rgba(168,85,247,0.06); border-radius:8px; padding:0.9rem;">
          <h4 style="margin:0 0 0.8rem; color:#c084fc; font-size:0.95rem;"><i class="fa-solid fa-lightbulb"></i> Suggest an edit</h4>
          <div class="form-group">
            <label class="form-label" for="sugg-title">Title</label>
            <input type="text" id="sugg-title" class="form-input" value="${escapeHtml(inc.title || '')}">
          </div>
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:0.5rem;">
            <div class="form-group">
              <label class="form-label" for="sugg-status">Status</label>
              <select id="sugg-status" class="form-select">${statusHtml}</select>
            </div>
            <div class="form-group">
              <label class="form-label" for="sugg-size">Size</label>
              <input type="text" id="sugg-size" class="form-input" value="${escapeHtml(inc.size || '')}" placeholder="e.g. 5 ha">
            </div>
          </div>
          <div class="form-group">
            <div class="form-label">Incident Type(s)</div>
            <div style="display:flex; flex-wrap:wrap; gap:0.4rem; max-height:120px; overflow-y:auto; background:rgba(0,0,0,0.2); padding:0.5rem; border-radius:6px; border:1px solid var(--border-subtle);">${typeHtml}</div>
          </div>
          <div class="form-group">
            <div class="form-label">Responding Agencies</div>
            <div style="display:flex; flex-wrap:wrap; gap:0.5rem;">${agencyHtml}</div>
          </div>
          <div class="form-group">
            <div class="form-label">Location</div>
            <div style="display:flex; gap:0.5rem; align-items:center;">
              <button type="button" class="btn btn-secondary" style="padding:0.35rem 0.7rem; font-size:0.75rem;" onclick="startSuggestMove()"><i class="fa-solid fa-arrows-up-down-left-right"></i> Suggest a move</button>
              <button type="button" class="btn btn-secondary" id="sugg-move-clear" style="display:none; padding:0.35rem 0.7rem; font-size:0.75rem;" onclick="clearSuggestMove()">Clear</button>
            </div>
            <div id="sugg-move-display" style="font-size:0.75rem; color:#94a3b8; margin-top:0.4rem; min-height:1em;"></div>
          </div>
          <div class="form-group">
            <label class="form-label" for="sugg-desc">Description (Markdown)</label>
            <textarea id="sugg-desc" class="form-textarea" placeholder="Use **bold**, *italics*, or lists...">${escapeHtml(inc.description || '')}</textarea>
          </div>
          <button class="btn btn-primary btn-block" style="padding:0.5rem;" onclick="submitSuggestionEdit()"><i class="fa-solid fa-paper-plane"></i> Submit suggestion</button>
          <div style="border-top:1px solid rgba(255,255,255,0.1); margin:1rem 0 0.8rem;"></div>
          <div class="form-group" style="margin-bottom:0.5rem;">
            <label class="form-label" for="sugg-note">Suggest a note to the owner</label>
            <textarea id="sugg-note" class="form-textarea" placeholder="e.g. This pin looks like a duplicate of #1234" style="min-height:50px; font-size:0.85rem;"></textarea>
          </div>
          <button class="btn btn-secondary btn-block" style="padding:0.5rem;" onclick="submitSuggestionNote()"><i class="fa-solid fa-comment"></i> Suggest a note</button>
        </div>
      `;
    }

    // Collect only the fields that differ from the loaded incident so the
    // owner sees a focused diff.
    function collectSuggestionChanges(inc) {
      const changes = {};
      const title = document.getElementById('sugg-title').value;
      if (title !== (inc.title || '')) changes.title = title;

      const desc = document.getElementById('sugg-desc').value;
      if (desc !== (inc.description || '')) changes.description = desc;

      const status = document.getElementById('sugg-status').value;
      if (status !== (inc.status || 'Going')) changes.status = status;

      const size = document.getElementById('sugg-size').value;
      if (size !== (inc.size || '')) changes.size = size;

      const newTypes = Array.from(document.querySelectorAll('.sugg-type:checked')).map(cb => cb.value);
      const oldTypes = Array.isArray(inc.type) ? inc.type : (inc.type ? [inc.type] : []);
      if (!arraysEqualUnordered(newTypes, oldTypes)) changes.type = newTypes;

      const newAgencies = Array.from(document.querySelectorAll('.sugg-agency:checked')).map(cb => cb.value);
      const oldAgencies = Array.isArray(inc.responding_agencies) ? inc.responding_agencies : [];
      if (!arraysEqualUnordered(newAgencies, oldAgencies)) changes.responding_agencies = newAgencies;

      // Proposed pin move (set by dragging the blue suggest-ghost).
      if (suggestedMove) {
        changes.lat = suggestedMove.lat;
        changes.lng = suggestedMove.lng;
        if (suggestedMove.location) changes.location = suggestedMove.location;
      }

      return changes;
    }

    function arraysEqualUnordered(a, b) {
      if (a.length !== b.length) return false;
      const sa = [...a].sort();
      const sb = [...b].sort();
      return sa.every((v, i) => v === sb[i]);
    }

    async function submitSuggestionEdit() {
      if (!selectedId || !currentIncident) return;
      const changes = collectSuggestionChanges(currentIncident);
      if (Object.keys(changes).length === 0) {
        alert('No changes to suggest — edit a field first.');
        return;
      }
      const body = { kind: 'edit', changes };
      if (currentUserEmail) body.suggested_by_name = currentUserEmail;
      try {
        const res = await apiFetch(`${PROXY_BASE}/api/incidents/${selectedId}/suggestions`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body)
        });
        if (res.ok) {
          clearSuggestMove();
          alert('Suggestion submitted — the owner will review it.');
        } else {
          alert('Failed to submit suggestion.');
        }
      } catch (e) {
        console.warn('Suggestion submit failed:', e);
      }
    }

    async function submitSuggestionNote() {
      if (!selectedId) return;
      const msg = document.getElementById('sugg-note').value.trim();
      if (!msg) {
        alert('Type a note first.');
        return;
      }
      const body = { kind: 'note', message: msg };
      if (currentUserEmail) body.suggested_by_name = currentUserEmail;
      try {
        const res = await apiFetch(`${PROXY_BASE}/api/incidents/${selectedId}/suggestions`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body)
        });
        if (res.ok) {
          document.getElementById('sugg-note').value = '';
          alert('Note submitted — the owner will review it.');
        } else {
          alert('Failed to submit note.');
        }
      } catch (e) {
        console.warn('Note submit failed:', e);
      }
    }

    // ---- SUGGESTION REVIEW (owner/admin path) ----
    // GET the pending suggestions with the reviewer's JWT and render each as
    // an approve/reject card. Owner-only endpoint; 401/403 render nothing.
    async function loadIncidentSuggestions(id) {
      const panel = document.getElementById('suggestions-review-panel');
      if (!panel) return;
      let list = [];
      try {
        const res = await apiFetch(`${PROXY_BASE}/api/incidents/${id}/suggestions?status=pending`, { auth: true });
        if (!res.ok) {
          // 401/403 (not an owner/admin for this pin) or other error: show nothing.
          panel.style.display = 'none';
          panel.innerHTML = '';
          return;
        }
        list = await res.json();
      } catch (e) {
        panel.style.display = 'none';
        panel.innerHTML = '';
        return;
      }
      if (!Array.isArray(list) || list.length === 0) {
        panel.style.display = 'block';
        panel.innerHTML = `<div style="font-size:0.75rem; color:var(--text-soft); font-style:italic; padding:0.3rem 0;">No pending suggestions.</div>`;
        return;
      }

      const cards = list.map(s => renderSuggestionCard(id, s)).join('');
      panel.style.display = 'block';
      panel.innerHTML = `
        <div style="border:1px solid rgba(34,197,94,0.35); background:rgba(34,197,94,0.05); border-radius:8px; padding:0.9rem;">
          <h4 style="margin:0 0 0.8rem; color:#4ade80; font-size:0.95rem;">
            <i class="fa-solid fa-inbox"></i> Pending suggestions
            <span style="background:#22c55e; color:#052e16; font-size:0.7rem; font-weight:700; padding:1px 7px; border-radius:10px; margin-left:0.3rem;">${list.length}</span>
          </h4>
          ${cards}
        </div>
      `;
    }

    function renderSuggestionCard(incidentId, s) {
      const who = escapeHtml(s.suggested_by_name || s.suggested_by || 'Unknown');
      const when = s.created_at ? new Date(s.created_at).toLocaleString() : '';
      let bodyHtml = '';
      if (s.kind === 'edit' && s.changes && typeof s.changes === 'object') {
        const rows = Object.keys(s.changes).map(k => {
          const oldVal = currentIncident ? currentIncident[k] : undefined;
          const oldStr = formatFieldValue(oldVal);
          const newStr = formatFieldValue(s.changes[k]);
          return `<div style="font-size:0.78rem; margin-bottom:0.25rem;">
            <span style="color:#c084fc; font-weight:600;">${escapeHtml(k)}</span>:
            <span style="color:#94a3b8; text-decoration:line-through;">${escapeHtml(oldStr)}</span>
            <span style="color:#64748b;"> → </span>
            <span style="color:#e2e8f0;">${escapeHtml(newStr)}</span>
          </div>`;
        }).join('');
        bodyHtml = rows || `<div style="font-size:0.78rem; color:var(--text-soft);">(no field changes)</div>`;
      } else if (s.kind === 'note') {
        bodyHtml = `<div style="font-size:0.82rem; color:#e2e8f0; white-space:pre-wrap;">${escapeHtml(s.message || '')}</div>`;
      } else {
        bodyHtml = `<div style="font-size:0.78rem; color:var(--text-soft);">(unrecognized suggestion)</div>`;
      }

      const kindLabel = s.kind === 'edit' ? 'EDIT' : (s.kind === 'note' ? 'NOTE' : escapeHtml(String(s.kind || '')));
      const sid = encodeURIComponent(s.id);
      const iid = encodeURIComponent(incidentId);
      return `
        <div style="border:1px solid rgba(255,255,255,0.1); border-radius:6px; padding:0.6rem 0.7rem; margin-bottom:0.6rem; background:rgba(0,0,0,0.2);">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.4rem; font-size:0.7rem; color:var(--text-soft);">
            <span><span style="background:rgba(168,85,247,0.2); color:#c084fc; padding:1px 6px; border-radius:4px; font-weight:700; letter-spacing:0.3px;">${kindLabel}</span> by ${who}</span>
            <span>${escapeHtml(when)}</span>
          </div>
          ${bodyHtml}
          <div style="display:flex; gap:0.5rem; margin-top:0.6rem;">
            <button class="btn btn-primary" style="padding:0.3rem 0.7rem; font-size:0.75rem;" onclick="approveSuggestion('${iid}','${sid}')">Approve</button>
            <button class="btn btn-danger" style="padding:0.3rem 0.7rem; font-size:0.75rem;" onclick="rejectSuggestion('${iid}','${sid}')">Reject</button>
          </div>
        </div>
      `;
    }

    function formatFieldValue(v) {
      if (v === null || v === undefined) return '—';
      if (Array.isArray(v)) return v.length ? v.join(', ') : '—';
      const s = String(v);
      return s.length ? s : '—';
    }

    async function approveSuggestion(incidentId, sid) {
      try {
        const res = await apiFetch(`${PROXY_BASE}/api/incidents/${incidentId}/suggestions/${sid}/approve`, { method: 'POST' });
        if (!res.ok) { alert('Failed to approve suggestion.'); return; }
        // Re-fetch the incident so the editor + review panel reflect the applied change.
        await refreshSelectedIncident(incidentId);
      } catch (e) {
        console.warn('Approve failed:', e);
      }
    }

    async function rejectSuggestion(incidentId, sid) {
      try {
        const res = await apiFetch(`${PROXY_BASE}/api/incidents/${incidentId}/suggestions/${sid}/reject`, { method: 'POST' });
        if (!res.ok) { alert('Failed to reject suggestion.'); return; }
        loadIncidentSuggestions(incidentId);
      } catch (e) {
        console.warn('Reject failed:', e);
      }
    }

    // Re-fetch a single incident and re-open it in the editor (used after an
    // approval auto-applies a change). Falls back to a logs+suggestions refresh.
    async function refreshSelectedIncident(incidentId) {
      try {
        const res = await apiFetch(`${PROXY_BASE}/api/incidents/${incidentId}`);
        if (res.ok) {
          const fresh = await res.json();
          if (fresh && fresh.id) {
            selectIncident(fresh);
            reloadIncidents();
            return;
          }
        }
      } catch (e) {
        console.warn('Incident refresh failed:', e);
      }
      // Fallback: at least refresh logs + suggestions in place.
      loadIncidentLogs(incidentId);
      loadIncidentSuggestions(incidentId);
    }
    // RFS incident (read-only) selection for logging
    async function selectRfsIncident(data, pagerDetails) {
      document.getElementById('title-group').style.display = 'none';
      document.getElementById('status-size-group').style.display = 'none';
      document.getElementById('expiry-group').style.display = 'none';
      document.getElementById('type-group').style.display = 'none';
      document.getElementById('agency-group').style.display = 'none';
      document.getElementById('desc-group').style.display = 'none';
      document.getElementById('save-delete-group').style.display = 'none';
      const _ab1 = document.getElementById('btn-archive');
      if (_ab1) _ab1.style.display = 'none';

      // Units ARE editable on RFS incidents — they persist immediately
      // against the shared RFS stub row (no Save button in this view).
      {
        const pt = (data.point || '').split(' ').map(Number);
        _unitsStub = {
          id: data.id,
          title: 'RFS Incident Log',
          lat: Number.isFinite(pt[0]) ? pt[0] : 0,
          lng: Number.isFinite(pt[1]) ? pt[1] : 0,
          tag: 'RFS'
        };
        currentUnits = [];
        renderUnitChips();
        currentImages = [];
        renderPhotoTiles();
        document.getElementById('units-group').style.display = 'block';
        document.getElementById('photos-group').style.display = 'block';
        loadStubUnits(_unitsStub);
      }

      // Clear any ownership-aware panels left over from a user-pin selection.
      const _on = document.getElementById('ownership-notice');
      if (_on) { _on.style.display = 'none'; _on.innerHTML = ''; }
      const _sp = document.getElementById('suggest-panel');
      if (_sp) { _sp.style.display = 'none'; _sp.innerHTML = ''; }
      const _rp = document.getElementById('suggestions-review-panel');
      if (_rp) { _rp.style.display = 'none'; _rp.innerHTML = ''; }
      currentIncident = null;

      selectedId = data.id;
      document.getElementById('selection-editor').style.display = 'block';
      document.getElementById('instruction-text').style.display = 'none';
      document.getElementById('edit-id').value = data.id;
      document.getElementById('edit-id-display').textContent = "RFS Log ID: " + data.id.slice(0, 8);
      // Header is shared across the user/RFS/pager views — always set
      // it, or the previous view's title (e.g. "Pager hits at this
      // location") leaks into this one.
      const rfsHeader = document.querySelector('#edit-header h4');
      if (rfsHeader) { rfsHeader.textContent = 'RFS Incident'; rfsHeader.style.color = '#ef4444'; }

      const customRFSBlock = document.createElement('div');
      customRFSBlock.id = 'rfs-read-only-data';
      customRFSBlock.dataset.point = data.point;

      customRFSBlock.innerHTML = `
        <div style="background:rgba(148,163,184,0.07); border:1px solid rgba(148,163,184,0.25); padding:0.8rem; border-radius:6px; margin-bottom:1rem; font-size:0.85rem;">
          <h5 style="margin-top:0.3rem; margin-bottom:0.5rem; color:#ef4444;">RFS Incident Details (Read Only)</h5>
          <strong>Title:</strong> ${escapeHtml(data.title || 'N/A')}<br>
          <strong>Status:</strong> ${escapeHtml(data.STATUS || 'Unknown')}<br>
          <strong>Location:</strong> ${escapeHtml(data.LOCATION || 'N/A')}<br>
          <a href="${data.link}" target="_blank" style="font-size:0.8rem; margin-top:0.5rem; display:inline-block; color:#7dd3fc;">View on Fires Near Me →</a>
        </div>
      `;
      const editorPanel = document.getElementById('selection-editor');
      let existingRFSBlock = document.getElementById('rfs-read-only-data');
      if (existingRFSBlock) existingRFSBlock.remove();
      
      const logSection = document.getElementById('log-section');
      editorPanel.insertBefore(customRFSBlock, logSection);
      // Units sit between the read-only details and the logs.
      placeUnitsGroup(true);

      logSection.style.display = 'block';
      document.getElementById('new-update-msg').value = '';

      loadIncidentLogs(data.id);
      const [lat, lng] = data.point.split(' ').map(Number);
      if (Number.isFinite(lat) && Number.isFinite(lng)) {
        map.panTo([lat, lng]);
      }
      renderPagerSection(pagerDetails);
      removeDragGhost();
    }

    function getPinStyle(inc) {
      let colors = [];
      if (inc.responding_agencies && inc.responding_agencies.length > 0) {
        inc.responding_agencies.forEach(a => {
          const c = AGENCY_COLORS[a.toUpperCase()];
          if (c) colors.push(c);
        });
      }
      if (colors.length === 0) {
        const t = (Array.isArray(inc.type) ? inc.type.join(' ') : inc.type || '').toUpperCase();
        if (t.includes('FIRE')) colors.push(TYPE_COLORS.FIRE);
        else if (t.includes('RESCUE') || t.includes('MVA')) colors.push(TYPE_COLORS.RESCUE);
        else if (t.includes('HAZARD')) colors.push(TYPE_COLORS.HAZARD);
        else if (t.includes('POLICE')) colors.push(TYPE_COLORS.POLICE);
        else if (t.includes('CARDIAC')) colors.push(TYPE_COLORS.MEDICAL);
        else colors.push(TYPE_COLORS.DEFAULT);
      }
      let background = colors[0];
      if (colors.length > 1) {
        const segment = 360 / colors.length;
        let grad = 'conic-gradient(';
        colors.forEach((c, i) => {
          grad += `${c} ${i * segment}deg ${(i + 1) * segment}deg${i < colors.length - 1 ? ', ' : ''}`;
        });
        grad += ')';
        background = grad;
      }
      return { background, glow: colors[0] };
    }

    function resetEditor() {
      selectedId = null;
      currentIncident = null;
      document.getElementById('selection-editor').style.display = 'none';
      document.getElementById('instruction-text').style.display = 'block';

      document.getElementById('title-group').style.display = 'block';
      document.getElementById('location-group').style.display = 'none';
      document.getElementById('status-size-group').style.display = 'grid';
      document.getElementById('expiry-group').style.display = 'block';
      document.getElementById('type-group').style.display = 'block';
      document.getElementById('agency-group').style.display = 'block';
      document.getElementById('units-group').style.display = 'block';
      document.getElementById('photos-group').style.display = 'block';
      document.getElementById('desc-group').style.display = 'block';
      document.getElementById('save-delete-group').style.display = 'grid';
      document.getElementById('log-section').style.display = 'block';
      currentUnits = [];
      _unitsStub = null;
      renderUnitChips();
      currentImages = [];
      renderPhotoTiles();
      placeUnitsGroup(false);

      // Clear ownership-aware injected panels so state never leaks between pins.
      const ownershipNotice = document.getElementById('ownership-notice');
      if (ownershipNotice) { ownershipNotice.style.display = 'none'; ownershipNotice.innerHTML = ''; }
      const suggestPanel = document.getElementById('suggest-panel');
      if (suggestPanel) { suggestPanel.style.display = 'none'; suggestPanel.innerHTML = ''; }
      const reviewPanel = document.getElementById('suggestions-review-panel');
      if (reviewPanel) { reviewPanel.style.display = 'none'; reviewPanel.innerHTML = ''; }

      const RFS_BLOCK = document.getElementById('rfs-read-only-data');
      if (RFS_BLOCK) RFS_BLOCK.remove();
      const PAGER_BLOCK = document.getElementById('pager-read-only-data');
      if (PAGER_BLOCK) PAGER_BLOCK.remove();
      removeDragGhost();
      clearSuggestMove();
      const _ab3 = document.getElementById('btn-archive');
      if (_ab3) _ab3.style.display = 'none';
    }

    // --- LOG MANAGEMENT ---
    async function loadIncidentLogs(id) {
      const container = document.getElementById('editor-logs-container');
      container.innerHTML = '<div style="padding:1rem; color:var(--text-soft); font-size:0.8rem;">Loading logs...</div>';
      
      const res = await apiFetch(`${PROXY_BASE}/api/incidents/${id}/updates`);
      const data = res.ok ? await res.json() : [];
      const error = res.ok ? null : { message: 'Failed to load logs' };

      if (error) {
        container.innerHTML = `<div style="color:#ef4444; padding:0.5rem; font-size:0.8rem;">Error loading logs: ${escapeHtml(error.message)}</div>`;
        return;
      }
      if (!data || data.length === 0) {
        container.innerHTML = `<div style="padding:1rem; color:var(--text-soft); font-size:0.8rem; font-style:italic;">No logs found.</div>`;
        return;
      }

      container.innerHTML = data.map(log => {
        // Edit/Delete are AUTHOR-ONLY (admins get a moderation
        // override) — matching the backend gate, so no dead buttons.
        const mine = !!(log.created_by && log.created_by === currentUserId) || currentIsAdmin;
        const actions = mine ? `
            <div style="display:flex; gap:0.5rem;">
              <span style="cursor:pointer; color:var(--accent);" onclick="startEditLog('${log.id}')">Edit</span>
              <span style="cursor:pointer; color:#8b5cf6;" onclick="checkGrammar('${log.id}')" title="Check grammar">✓ Grammar</span>
              <span style="cursor:pointer; color:#ef4444;" onclick="deleteLog('${log.id}')">Delete</span>
            </div>` : '';
        return `
        <div id="log-item-${log.id}" style="border-bottom:1px solid rgba(255,255,255,0.1); padding:0.6rem 0.4rem;" data-original-message="${escapeHtml(log.message).replace(/"/g, '&quot;')}">
          <div style="font-size:0.7rem; color:var(--text-soft); display:flex; justify-content:space-between; margin-bottom:0.3rem;">
            <span>${new Date(log.created_at).toLocaleString()}${log.created_by_name ? ` · <strong style="color:#cbd5e1;">${escapeHtml(log.created_by_name)}</strong>` : ''}</span>
            ${actions}
          </div>
          <div class="log-content markdown-body" id="log-text-${log.id}" style="font-size:0.85rem; color:#e2e8f0; line-height:1.4;">
            ${DOMPurify.sanitize(marked.parse(log.message))}
          </div>
          <textarea id="log-edit-input-${log.id}" class="form-textarea" style="display:none; font-size:0.85rem; min-height:60px; margin-bottom:0.5rem;">${escapeHtml(log.message)}</textarea>
          <div id="log-edit-actions-${log.id}" style="display:none; gap:0.5rem; margin-bottom:0.5rem;">
            <button class="btn btn-primary" style="padding:0.3rem 0.6rem; font-size:0.75rem;" onclick="saveEditLog('${log.id}')">Save</button>
            <button class="btn btn-secondary" style="padding:0.3rem 0.6rem; font-size:0.75rem;" onclick="checkGrammarForEdit('${log.id}')" title="Check grammar">✓ Grammar</button>
            <button class="btn btn-secondary" style="padding:0.3rem 0.6rem; font-size:0.75rem;" onclick="cancelEditLog('${log.id}')">Cancel</button>
          </div>
          <div id="log-grammar-results-${log.id}" style="display:none; margin-top:0.5rem;"></div>
        </div>
      `; }).join('');
    }

    function startEditLog(id) {
      document.getElementById(`log-text-${id}`).style.display = 'none';
      document.getElementById(`log-edit-input-${id}`).style.display = 'block';
      document.getElementById(`log-edit-actions-${id}`).style.display = 'flex';
      // Update the stored original message when editing starts
      const logItem = document.getElementById(`log-item-${id}`);
      const editInput = document.getElementById(`log-edit-input-${id}`);
      if (logItem && editInput) {
        logItem.dataset.originalMessage = editInput.value;
      }
    }

    function cancelEditLog(id) {
      document.getElementById(`log-text-${id}`).style.display = 'block';
      document.getElementById(`log-edit-input-${id}`).style.display = 'none';
      document.getElementById(`log-edit-actions-${id}`).style.display = 'none';
      // Hide grammar results when canceling
      const resultsDiv = document.getElementById(`log-grammar-results-${id}`);
      if (resultsDiv) resultsDiv.style.display = 'none';
    }

    async function saveEditLog(id) {
      const newText = document.getElementById(`log-edit-input-${id}`).value;
      const res = await apiFetch(`${PROXY_BASE}/api/incidents/updates/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: newText })
      });
      const error = res.ok ? null : { message: 'Failed to save log' };
      if (error) alert(error.message);
      else {
        // Hide grammar results when saving
        const resultsDiv = document.getElementById(`log-grammar-results-${id}`);
        if (resultsDiv) resultsDiv.style.display = 'none';
        loadIncidentLogs(selectedId);
      }
    }

    async function deleteLog(id) {
      const ok = await askConfirm(
        'Delete this log entry? This cannot be undone.',
        { confirmLabel: 'Delete', danger: true },
      );
      if (!ok) return;
      const res = await apiFetch(`${PROXY_BASE}/api/incidents/updates/${id}`, { method: 'DELETE' });
      if (!res.ok) showToast('Failed to delete log entry.', 'error');
      else loadIncidentLogs(selectedId);
    }

    // --- GRAMMAR CHECKING (LanguageTool API) ---
    async function checkGrammar(id) {
      const resultsDiv = document.getElementById(`log-grammar-results-${id}`);
      if (!resultsDiv) return;
      
      // Get text from the log item - try edit input first (if editing), then the stored original message
      const editInput = document.getElementById(`log-edit-input-${id}`);
      const logItem = document.getElementById(`log-item-${id}`);
      const text = editInput && editInput.style.display !== 'none' 
        ? editInput.value 
        : (logItem ? logItem.dataset.originalMessage || '' : '');
      
      if (!text.trim()) {
        alert('No text to check.');
        return;
      }
      
      resultsDiv.style.display = 'block';
      resultsDiv.innerHTML = '<div style="color:#94a3b8; font-size:0.75rem;"><i class="fa-solid fa-spinner fa-spin"></i> Checking grammar...</div>';
      
      try {
        const formData = new URLSearchParams();
        formData.append('text', text);
        formData.append('language', 'en-US');
        formData.append('enabledOnly', 'false');
        
        const response = await fetch('https://api.languagetool.org/v2/check', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
          },
          body: formData.toString()
        });
        
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        displayGrammarResults(id, data, text, false);
        
        // Store field type for apply function
        const resultsDiv = document.getElementById(`log-grammar-results-${id}`);
        if (resultsDiv) {
          resultsDiv.dataset.fieldType = 'log';
        }
      } catch (error) {
        resultsDiv.innerHTML = `<div style="color:#ef4444; font-size:0.75rem;">Error checking grammar: ${escapeHtml(error.message)}</div>`;
      }
    }

    async function checkGrammarForEdit(id) {
      const textarea = document.getElementById(`log-edit-input-${id}`);
      if (!textarea) return;
      const text = textarea.value;
      if (!text.trim()) {
        alert('Please enter some text to check.');
        return;
      }
      await checkGrammar(id, text);
    }

    async function checkGrammarForNew() {
      const textarea = document.getElementById('new-update-msg');
      if (!textarea) return;
      const text = textarea.value;
      if (!text.trim()) {
        alert('Please enter some text to check.');
        return;
      }
      
      const resultsDiv = document.getElementById('new-log-grammar-results');
      if (!resultsDiv) return;
      
      resultsDiv.style.display = 'block';
      resultsDiv.innerHTML = '<div style="color:#94a3b8; font-size:0.75rem;"><i class="fa-solid fa-spinner fa-spin"></i> Checking grammar...</div>';
      
      try {
        const formData = new URLSearchParams();
        formData.append('text', text);
        formData.append('language', 'en-US');
        formData.append('enabledOnly', 'false');
        
        const response = await fetch('https://api.languagetool.org/v2/check', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
          },
          body: formData.toString()
        });
        
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        displayGrammarResults('new', data, text, true);
        
        // Store field type for apply function
        const resultsDiv = document.getElementById('new-log-grammar-results');
        if (resultsDiv) {
          resultsDiv.dataset.fieldType = 'new-log';
        }
      } catch (error) {
        resultsDiv.innerHTML = `<div style="color:#ef4444; font-size:0.75rem;">Error checking grammar: ${escapeHtml(error.message)}</div>`;
      }
    }

    function displayGrammarResults(id, data, originalText, isNewLog) {
      let resultsDiv;
      if (isNewLog) {
        resultsDiv = document.getElementById('new-log-grammar-results');
      } else if (id === 'title') {
        resultsDiv = document.getElementById('title-grammar-results');
      } else if (id === 'description') {
        resultsDiv = document.getElementById('description-grammar-results');
      } else {
        resultsDiv = document.getElementById(`log-grammar-results-${id}`);
      }
      
      if (!resultsDiv) {
        console.error('Grammar results div not found for id:', id, 'isNewLog:', isNewLog);
        return;
      }
      
      // Store the original text and matches for applying fixes
      resultsDiv.dataset.originalText = originalText;
      resultsDiv.dataset.matches = JSON.stringify(data.matches);
      resultsDiv.dataset.fieldId = id;
      resultsDiv.dataset.isNewLog = isNewLog;
      
      if (!data.matches || data.matches.length === 0) {
        resultsDiv.innerHTML = `
          <div style="background:rgba(34,197,94,0.1); border:1px solid #22c55e; border-radius:6px; padding:0.6rem; font-size:0.75rem; color:#22c55e; display:flex; justify-content:space-between; align-items:center;">
            <span><i class="fa-solid fa-check-circle"></i> No grammar or spelling issues found!</span>
            <button onclick="this.parentElement.parentElement.style.display='none'" style="background:none; border:none; color:#22c55e; cursor:pointer; font-size:0.7rem; padding:0.2rem 0.4rem;" title="Dismiss">✕</button>
          </div>
        `;
        return;
      }
      
      let html = `
        <div style="background:rgba(249,115,22,0.1); border:1px solid #f97316; border-radius:6px; padding:0.6rem; font-size:0.75rem;">
          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.5rem;">
            <div style="color:#f97316; font-weight:600;">
              <i class="fa-solid fa-exclamation-triangle"></i> Found ${data.matches.length} issue${data.matches.length !== 1 ? 's' : ''}
            </div>
            <div style="display:flex; gap:0.3rem;">
              <button onclick="selectAllGrammarFixes('${id}', ${isNewLog ? 'true' : 'false'})" style="background:rgba(59,130,246,0.2); border:1px solid rgba(59,130,246,0.4); color:#60a5fa; padding:0.2rem 0.4rem; border-radius:4px; font-size:0.65rem; cursor:pointer;" title="Select all">All</button>
              <button onclick="deselectAllGrammarFixes('${id}', ${isNewLog ? 'true' : 'false'})" style="background:rgba(148,163,184,0.2); border:1px solid rgba(148,163,184,0.4); color:#94a3b8; padding:0.2rem 0.4rem; border-radius:4px; font-size:0.65rem; cursor:pointer;" title="Deselect all">None</button>
            </div>
          </div>
      `;
      
      data.matches.forEach((match, idx) => {
        const start = match.offset;
        const end = match.offset + match.length;
        const before = escapeHtml(originalText.substring(0, start));
        const error = escapeHtml(originalText.substring(start, end));
        const after = escapeHtml(originalText.substring(end));
        
        const firstSuggestion = match.replacements && match.replacements.length > 0 ? match.replacements[0].value : null;
        const otherSuggestions = match.replacements && match.replacements.length > 1 
          ? match.replacements.slice(1, 4).map(r => r.value) 
          : [];
        
        let suggestionsHtml = '';
        if (firstSuggestion) {
          suggestionsHtml = `
            <span style="display:inline-flex; align-items:center; gap:0.3rem; background:linear-gradient(135deg, rgba(34,197,94,0.15), rgba(34,197,94,0.25)); color:#4ade80; padding:0.2rem 0.5rem; border-radius:4px; font-weight:600; border:1px solid rgba(34,197,94,0.5); box-shadow:0 0 8px rgba(34,197,94,0.2);">
              <i class="fa-solid fa-check-circle" style="font-size:0.7rem;"></i>
              ${escapeHtml(firstSuggestion)}
            </span>
          `;
          if (otherSuggestions.length > 0) {
            suggestionsHtml += `
              <span style="color:#64748b; margin:0 0.4rem; font-size:0.65rem;">or</span>
              <span style="color:#94a3b8;">${otherSuggestions.map(s => escapeHtml(s)).join(', ')}</span>
            `;
          }
        } else {
          suggestionsHtml = '<span style="color:#94a3b8; font-style:italic;">No suggestions available</span>';
        }
        
        const hasReplacement = match.replacements && match.replacements.length > 0;
        const matchId = `grammar-fix-${id}-${idx}`;
        
        html += `
          <div style="margin-bottom:0.6rem; padding:0.6rem; background:rgba(0,0,0,0.25); border-radius:6px; border-left:3px solid #f97316; display:flex; gap:0.6rem; transition:background 0.2s;">
            <div style="flex-shrink:0; padding-top:0.3rem;">
              <input type="checkbox" id="${matchId}" class="grammar-fix-checkbox" data-match-index="${idx}" ${hasReplacement ? 'checked' : 'disabled'} onchange="updateGrammarPreviewFromCheckbox(this)" style="cursor:pointer; width:16px; height:16px; accent-color:#f97316;">
            </div>
            <div style="flex:1; min-width:0;">
              <div style="color:#e2e8f0; margin-bottom:0.4rem; font-weight:500;">
                <span style="color:#f97316; font-weight:700; margin-right:0.3rem;">${idx + 1}.</span>
                <span>${escapeHtml(match.message || 'Grammar issue')}</span>
              </div>
              <div style="color:#94a3b8; font-size:0.75rem; margin-bottom:0.4rem; font-family:'Courier New', monospace; padding:0.4rem; background:rgba(0,0,0,0.3); border-radius:4px; line-height:1.4;">
                "${before}<span style="background:rgba(239,68,68,0.4); color:#fca5a5; padding:0.1rem 0.2rem; border-radius:2px; font-weight:600; text-decoration:underline;">${error}</span>${after}"
              </div>
              <div style="color:#cbd5e1; font-size:0.7rem; display:flex; flex-wrap:wrap; align-items:center; gap:0.3rem;">
                <strong style="color:#cbd5e1; margin-right:0.3rem;">Suggestions:</strong>
                ${suggestionsHtml}
              </div>
              ${firstSuggestion ? `
                <div style="color:#64748b; font-size:0.65rem; margin-top:0.2rem; font-style:italic; display:flex; align-items:center; gap:0.3rem;">
                  <i class="fa-solid fa-arrow-right" style="font-size:0.6rem;"></i>
                  <span>Will apply: <strong style="color:#4ade80;">${escapeHtml(firstSuggestion)}</strong></span>
                </div>
              ` : ''}
              ${match.rule && match.rule.category ? `
                <div style="color:#64748b; font-size:0.65rem; margin-top:0.3rem; padding-top:0.3rem; border-top:1px solid rgba(148,163,184,0.1);">
                  <i class="fa-solid fa-tag" style="font-size:0.6rem; margin-right:0.3rem;"></i>
                  ${escapeHtml(match.rule.category.name || '')}
                </div>
              ` : ''}
              ${!hasReplacement ? `
                <div style="color:#fca5a5; font-size:0.65rem; margin-top:0.3rem; font-style:italic; display:flex; align-items:center; gap:0.3rem;">
                  <i class="fa-solid fa-exclamation-circle" style="font-size:0.6rem;"></i>
                  <span>No replacement available</span>
                </div>
              ` : ''}
            </div>
          </div>
        `;
      });
      
      // Store matches data for preview updates
      resultsDiv.dataset.allMatches = JSON.stringify(data.matches);
      
      // Initial preview calculation (all checked by default)
      const previewData = calculateGrammarPreview(id, isNewLog, originalText, data.matches);
      const correctedDiffHtml = generateCorrectedDiffPreview(originalText, previewData.correctedText, previewData.changesList);
      
      // Add preview section
      html += `
          <div id="grammar-preview-${id}" style="margin-top:0.8rem; padding:0.6rem; background:rgba(59,130,246,0.1); border:1px solid rgba(59,130,246,0.3); border-radius:6px;">
            <div style="color:#60a5fa; font-weight:600; font-size:0.7rem; margin-bottom:0.4rem; text-transform:uppercase; letter-spacing:0.05em;">
              <i class="fa-solid fa-eye"></i> Preview Changes
            </div>
            <div style="margin-bottom:0.5rem;">
              <div style="color:#94a3b8; font-size:0.65rem; margin-bottom:0.2rem;">Original:</div>
              <div style="color:#cbd5e1; font-size:0.75rem; padding:0.4rem; background:rgba(0,0,0,0.3); border-radius:4px; font-family:monospace; white-space:pre-wrap; word-break:break-word;">${escapeHtml(originalText)}</div>
            </div>
            <div style="margin-bottom:0.5rem;">
              <div style="color:#94a3b8; font-size:0.65rem; margin-bottom:0.2rem;">Corrected:</div>
              <div id="grammar-preview-corrected-${id}" style="font-size:0.75rem; padding:0.4rem; background:rgba(0,0,0,0.3); border-radius:4px; font-family:monospace; white-space:pre-wrap; word-break:break-word; line-height:1.5;">${correctedDiffHtml}</div>
            </div>
            <div style="margin-top:0.5rem; padding-top:0.5rem; border-top:1px solid rgba(148,163,184,0.2);">
              <div style="color:#94a3b8; font-size:0.65rem; margin-bottom:0.3rem;">Changes to be applied (<span id="grammar-preview-count-${id}">${previewData.changesList.length}</span>):</div>
              <div id="grammar-preview-changes-${id}" style="max-height:100px; overflow-y:auto;">
      `;
      
      previewData.changesList.forEach(change => {
        html += `
          <div style="font-size:0.7rem; color:#cbd5e1; margin-bottom:0.2rem; padding:0.2rem 0.4rem; background:rgba(0,0,0,0.2); border-radius:3px;">
            <span style="color:#f97316;">${change.index}.</span> 
            <span style="color:#fca5a5; text-decoration:line-through;">"${escapeHtml(change.original)}"</span> 
            <span style="color:#94a3b8;">→</span> 
            <span style="color:#4ade80;">"${escapeHtml(change.replacement)}"</span>
          </div>
        `;
      });
      
      html += `
              </div>
            </div>
          </div>
          <div style="margin-top:0.5rem; padding-top:0.5rem; border-top:1px solid rgba(148,163,184,0.2); display:flex; justify-content:space-between; align-items:center; gap:0.5rem;">
            <div style="font-size:0.65rem; color:#64748b;">
              Powered by <a href="https://languagetool.org" target="_blank" rel="noopener" style="color:#8b5cf6; text-decoration:none;">LanguageTool</a>
            </div>
            <button onclick="applyGrammarFixes('${id}', ${isNewLog ? 'true' : 'false'})" class="btn btn-primary" style="padding:0.3rem 0.6rem; font-size:0.7rem;" title="Apply all suggested fixes">Apply Fixes</button>
            <button onclick="this.parentElement.parentElement.style.display='none'" style="background:none; border:none; color:#94a3b8; cursor:pointer; font-size:0.7rem; padding:0.2rem 0.4rem;" title="Dismiss">✕</button>
          </div>
        </div>
      `;
      
      resultsDiv.innerHTML = html;
      
      // Recalculate preview after HTML is inserted to use actual checkbox states
      setTimeout(() => {
        updateGrammarPreview(id, isNewLog);
      }, 0);
    }

    function calculateGrammarPreview(id, isNewLog, originalText, allMatches) {
      const resultsDiv = isNewLog 
        ? document.getElementById('new-log-grammar-results')
        : (id === 'title' ? document.getElementById('title-grammar-results')
          : id === 'description' ? document.getElementById('description-grammar-results')
          : document.getElementById(`log-grammar-results-${id}`));
      
      if (!resultsDiv) {
        return { correctedText: originalText, changesList: [] };
      }
      
      // Get which fixes are checked
      const checkedIndices = [];
      const checkboxes = resultsDiv.querySelectorAll('.grammar-fix-checkbox:checked');
      
      if (checkboxes.length > 0) {
        // Checkboxes exist in DOM - use their checked state
        checkboxes.forEach(cb => {
          const index = parseInt(cb.dataset.matchIndex);
          if (!isNaN(index)) checkedIndices.push(index);
        });
      } else {
        // Checkboxes don't exist yet (initial render) - default to all matches with replacements
        allMatches.forEach((match, idx) => {
          if (match.replacements && match.replacements.length > 0) {
            checkedIndices.push(idx);
          }
        });
      }
      
      // Apply only checked fixes
      let correctedText = originalText;
      const changesList = [];
      const sortedMatches = [...allMatches]
        .map((match, idx) => ({ ...match, originalIndex: idx }))
        .filter((match, idx) => checkedIndices.includes(idx))
        .sort((a, b) => b.offset - a.offset);
      
      sortedMatches.forEach(match => {
        if (match.replacements && match.replacements.length > 0) {
          const replacement = match.replacements[0].value;
          const start = match.offset;
          const end = match.offset + match.length;
          const original = originalText.substring(start, end);
          
          changesList.push({
            original: original,
            replacement: replacement,
            message: match.message || 'Grammar issue',
            index: match.originalIndex + 1,
            originalOffset: start,
            originalLength: match.length
          });
          
          correctedText = correctedText.substring(0, start) + replacement + correctedText.substring(end);
        }
      });
      
      return { correctedText, changesList };
    }

    function generateCorrectedDiffPreview(originalText, correctedText, changesList) {
      if (!changesList || changesList.length === 0) {
        // No changes - show all text in red
        return `<span style="color:#fca5a5;">${escapeHtml(correctedText)}</span>`;
      }
      
      // Build a map of changed regions in the corrected text
      // Use offset information if available, otherwise fall back to indexOf
      const changedRegions = [];
      
      // Track offset shifts as we apply changes from start to end
      let offsetShift = 0;
      const sortedChanges = [...changesList].sort((a, b) => {
        // Use originalOffset if available, otherwise find position
        const aPos = a.originalOffset !== undefined ? a.originalOffset : originalText.indexOf(a.original);
        const bPos = b.originalOffset !== undefined ? b.originalOffset : originalText.indexOf(b.original);
        return aPos - bPos;
      });
      
      sortedChanges.forEach(change => {
        const originalPos = change.originalOffset !== undefined ? change.originalOffset : originalText.indexOf(change.original);
        if (originalPos === -1) return;
        
        // Position in corrected text (accounting for previous changes)
        const correctedPos = originalPos + offsetShift;
        const correctedEnd = correctedPos + change.replacement.length;
        
        changedRegions.push({
          start: correctedPos,
          end: correctedEnd,
          text: change.replacement
        });
        
        // Update offset shift for next change
        const originalLength = change.originalLength !== undefined ? change.originalLength : change.original.length;
        offsetShift += change.replacement.length - originalLength;
      });
      
      // Sort regions by start position
      changedRegions.sort((a, b) => a.start - b.start);
      
      // Build HTML: unchanged parts in red, changed parts in green
      let html = '';
      let lastIndex = 0;
      
      changedRegions.forEach(region => {
        // Add unchanged text before this change (in red)
        if (region.start > lastIndex) {
          html += `<span style="color:#fca5a5;">${escapeHtml(correctedText.substring(lastIndex, region.start))}</span>`;
        }
        
        // Add changed text (in green)
        html += `<span style="color:#4ade80; background:rgba(34,197,94,0.2); padding:0.1rem 0.2rem; border-radius:2px; font-weight:600;">${escapeHtml(region.text)}</span>`;
        
        lastIndex = region.end;
      });
      
      // Add remaining unchanged text (in red)
      if (lastIndex < correctedText.length) {
        html += `<span style="color:#fca5a5;">${escapeHtml(correctedText.substring(lastIndex))}</span>`;
      }
      
      return html || `<span style="color:#fca5a5;">${escapeHtml(correctedText)}</span>`;
    }

    function selectAllGrammarFixes(id, isNewLog) {
      const resultsDiv = isNewLog 
        ? document.getElementById('new-log-grammar-results')
        : (id === 'title' ? document.getElementById('title-grammar-results')
          : id === 'description' ? document.getElementById('description-grammar-results')
          : document.getElementById(`log-grammar-results-${id}`));
      
      if (!resultsDiv) return;
      
      const checkboxes = resultsDiv.querySelectorAll('.grammar-fix-checkbox:not(:disabled)');
      checkboxes.forEach(cb => cb.checked = true);
      updateGrammarPreview(id, isNewLog);
    }

    function deselectAllGrammarFixes(id, isNewLog) {
      const resultsDiv = isNewLog 
        ? document.getElementById('new-log-grammar-results')
        : (id === 'title' ? document.getElementById('title-grammar-results')
          : id === 'description' ? document.getElementById('description-grammar-results')
          : document.getElementById(`log-grammar-results-${id}`));
      
      if (!resultsDiv) return;
      
      const checkboxes = resultsDiv.querySelectorAll('.grammar-fix-checkbox');
      checkboxes.forEach(cb => cb.checked = false);
      updateGrammarPreview(id, isNewLog);
    }

    function updateGrammarPreviewFromCheckbox(checkbox) {
      const resultsDiv = checkbox.closest('[id$="-grammar-results"]');
      if (!resultsDiv || !resultsDiv.dataset.fieldId) return;
      const id = resultsDiv.dataset.fieldId;
      const isNewLog = resultsDiv.dataset.isNewLog === 'true';
      updateGrammarPreview(id, isNewLog);
    }

    function updateGrammarPreview(id, isNewLog) {
      const resultsDiv = isNewLog 
        ? document.getElementById('new-log-grammar-results')
        : (id === 'title' ? document.getElementById('title-grammar-results')
          : id === 'description' ? document.getElementById('description-grammar-results')
          : document.getElementById(`log-grammar-results-${id}`));
      
      if (!resultsDiv || !resultsDiv.dataset.originalText || !resultsDiv.dataset.allMatches) return;
      
      const originalText = resultsDiv.dataset.originalText;
      const allMatches = JSON.parse(resultsDiv.dataset.allMatches);
      
      const previewData = calculateGrammarPreview(id, isNewLog, originalText, allMatches);
      
      // Update preview display
      const correctedDiv = document.getElementById(`grammar-preview-corrected-${id}`);
      const countSpan = document.getElementById(`grammar-preview-count-${id}`);
      const changesDiv = document.getElementById(`grammar-preview-changes-${id}`);
      
      if (correctedDiv) {
        const correctedDiffHtml = generateCorrectedDiffPreview(originalText, previewData.correctedText, previewData.changesList);
        correctedDiv.innerHTML = correctedDiffHtml;
      }
      if (countSpan) {
        countSpan.textContent = previewData.changesList.length;
      }
      if (changesDiv) {
        if (previewData.changesList.length === 0) {
          changesDiv.innerHTML = '<div style="color:#94a3b8; font-size:0.7rem; font-style:italic; padding:0.4rem;">No changes selected</div>';
        } else {
          changesDiv.innerHTML = previewData.changesList.map(change => `
            <div style="font-size:0.7rem; color:#cbd5e1; margin-bottom:0.2rem; padding:0.2rem 0.4rem; background:rgba(0,0,0,0.2); border-radius:3px;">
              <span style="color:#f97316;">${change.index}.</span> 
              <span style="color:#fca5a5; text-decoration:line-through;">"${escapeHtml(change.original)}"</span> 
              <span style="color:#94a3b8;">→</span> 
              <span style="color:#4ade80;">"${escapeHtml(change.replacement)}"</span>
            </div>
          `).join('');
        }
      }
    }

    function applyGrammarFixes(id, isNewLog) {
      const resultsDiv = isNewLog 
        ? document.getElementById('new-log-grammar-results')
        : (id === 'title' ? document.getElementById('title-grammar-results')
          : id === 'description' ? document.getElementById('description-grammar-results')
          : document.getElementById(`log-grammar-results-${id}`));
      
      if (!resultsDiv || !resultsDiv.dataset.originalText || !resultsDiv.dataset.allMatches) {
        alert('No fixes available to apply.');
        return;
      }
      
      // Get which fixes are checked
      const checkedIndices = [];
      const checkboxes = resultsDiv.querySelectorAll('.grammar-fix-checkbox:checked');
      checkboxes.forEach(cb => {
        const index = parseInt(cb.dataset.matchIndex);
        if (!isNaN(index)) checkedIndices.push(index);
      });
      
      if (checkedIndices.length === 0) {
        alert('Please select at least one fix to apply.');
        return;
      }
      
      let correctedText = resultsDiv.dataset.originalText;
      const allMatches = JSON.parse(resultsDiv.dataset.allMatches);
      
      // Apply only checked fixes, sorted by offset in reverse order
      const sortedMatches = [...allMatches]
        .map((match, idx) => ({ ...match, originalIndex: idx }))
        .filter((match, idx) => checkedIndices.includes(idx))
        .sort((a, b) => b.offset - a.offset);
      
      sortedMatches.forEach(match => {
        if (match.replacements && match.replacements.length > 0) {
          // Use the first (best) suggestion
          const replacement = match.replacements[0].value;
          const start = match.offset;
          const end = match.offset + match.length;
          
          // Apply the replacement
          correctedText = correctedText.substring(0, start) + replacement + correctedText.substring(end);
        }
      });
      
      // Update the appropriate field based on the field type
      const fieldType = resultsDiv.dataset.fieldType || (isNewLog ? 'new-log' : (id === 'title' ? 'title' : (id === 'description' ? 'description' : 'log')));
      
      if (fieldType === 'new-log') {
        const textarea = document.getElementById('new-update-msg');
        if (textarea) {
          textarea.value = correctedText;
          resultsDiv.style.display = 'none';
        }
      } else if (fieldType === 'title') {
        const input = document.getElementById('edit-title');
        if (input) {
          input.value = correctedText;
          resultsDiv.style.display = 'none';
        }
      } else if (fieldType === 'description') {
        const textarea = document.getElementById('edit-desc');
        if (textarea) {
          textarea.value = correctedText;
          resultsDiv.style.display = 'none';
        }
      } else {
        // It's a log entry
        const editInput = document.getElementById(`log-edit-input-${id}`);
        const logItem = document.getElementById(`log-item-${id}`);
        if (editInput && editInput.style.display !== 'none') {
          // If in edit mode, update the textarea
          editInput.value = correctedText;
          // Update stored original message
          if (logItem) logItem.dataset.originalMessage = correctedText;
        } else if (logItem) {
          // If not in edit mode, we need to start edit mode first
          logItem.dataset.originalMessage = correctedText;
          startEditLog(id);
          const editInputAfter = document.getElementById(`log-edit-input-${id}`);
          if (editInputAfter) {
            editInputAfter.value = correctedText;
          }
        }
        resultsDiv.style.display = 'none';
      }
    }

    async function checkGrammarForTitle() {
      const input = document.getElementById('edit-title');
      if (!input) return;
      const text = input.value;
      if (!text.trim()) {
        alert('Please enter a title to check.');
        return;
      }
      
      const resultsDiv = document.getElementById('title-grammar-results');
      if (!resultsDiv) return;
      
      resultsDiv.style.display = 'block';
      resultsDiv.innerHTML = '<div style="color:#94a3b8; font-size:0.75rem;"><i class="fa-solid fa-spinner fa-spin"></i> Checking grammar...</div>';
      
      try {
        const formData = new URLSearchParams();
        formData.append('text', text);
        formData.append('language', 'en-US');
        formData.append('enabledOnly', 'false');
        
        const response = await fetch('https://api.languagetool.org/v2/check', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
          },
          body: formData.toString()
        });
        
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        displayGrammarResults('title', data, text, false);
      } catch (error) {
        resultsDiv.innerHTML = `<div style="color:#ef4444; font-size:0.75rem;">Error checking grammar: ${escapeHtml(error.message)}</div>`;
      }
    }

    async function checkGrammarForDescription() {
      const textarea = document.getElementById('edit-desc');
      if (!textarea) {
        console.error('Description textarea not found');
        alert('Description field not found.');
        return;
      }
      const text = textarea.value;
      if (!text.trim()) {
        alert('Please enter a description to check.');
        return;
      }
      
      const resultsDiv = document.getElementById('description-grammar-results');
      if (!resultsDiv) {
        console.error('Description grammar results div not found');
        alert('Grammar results container not found. Please refresh the page.');
        return;
      }
      
      resultsDiv.style.display = 'block';
      resultsDiv.innerHTML = '<div style="color:#94a3b8; font-size:0.75rem;"><i class="fa-solid fa-spinner fa-spin"></i> Checking grammar...</div>';
      
      try {
        const formData = new URLSearchParams();
        formData.append('text', text);
        formData.append('language', 'en-US');
        formData.append('enabledOnly', 'false');
        
        const response = await fetch('https://api.languagetool.org/v2/check', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
          },
          body: formData.toString()
        });
        
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        displayGrammarResults('description', data, text, false);
        
        // Store field type for apply function (use existing resultsDiv variable)
        resultsDiv.dataset.fieldType = 'description';
      } catch (error) {
        console.error('Grammar check error:', error);
        resultsDiv.innerHTML = `<div style="color:#ef4444; font-size:0.75rem;">Error checking grammar: ${escapeHtml(error.message)}</div>`;
      }
    }

    async function addUpdate() {
      if (!selectedId) return;
      const msg = document.getElementById('new-update-msg').value;
      if (!msg) return;

      // RFS incidents and pager clusters store logs against a shared
      // stub row — make sure it exists before the first entry.
      if (_unitsStub && _unitsStub.id === selectedId) {
        const ok = await ensureStubIncident(_unitsStub);
        if (!ok) {
          alert('Error creating stub for log.');
          return;
        }
      }

      const logRes = await apiFetch(`${PROXY_BASE}/api/incidents/${selectedId}/updates`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg })
      });
      const error = logRes.ok ? null : { message: 'Failed to save log' };
      
      if (error) {
        alert(`Error saving log: ${error.message}`); 
      } else {
        document.getElementById('new-update-msg').value = ''; 
        loadIncidentLogs(selectedId); 
      }
    }

    function toggleAddMode() {
      addMode = !addMode;
      const btn = document.getElementById('btn-add-mode');
      if (addMode) {
        btn.innerText = "Click Map to Place Pin";
        btn.style.background = "#22c55e";
        btn.style.color = "#fff";
        document.getElementById('map').style.cursor = "crosshair";
      } else {
        btn.innerText = "+ Add Pin";
        btn.style.background = "";
        btn.style.color = "";
        document.getElementById('map').style.cursor = "";
      }
    }
    
    // Reverse geocode coordinates to get address
    async function reverseGeocode(lat, lng) {
      try {
        const res = await fetch(`https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lng}&zoom=18&addressdetails=1`);
        const data = await res.json();
        if (data && data.address) {
          const addr = data.address;
          // Build a readable address string
          const parts = [];
          if (addr.road) parts.push(addr.road);
          if (addr.house_number) parts.unshift(addr.house_number);
          if (addr.suburb || addr.village || addr.town) parts.push(addr.suburb || addr.village || addr.town);
          if (addr.state) parts.push(addr.state);
          if (addr.postcode) parts.push(addr.postcode);
          return parts.length > 0 ? parts.join(', ') : data.display_name || '';
        }
        return data.display_name || '';
      } catch (e) {
        console.error('Reverse geocoding error:', e);
        return '';
      }
    }

    // Lightweight in-page toast (bottom-center) so incident flows don't rely
    // on native browser dialogs. textContent → no HTML injection.
    function showToast(message, type = 'info') {
      const prev = document.getElementById('me-toast');
      if (prev) prev.remove();
      const border = type === 'success' ? '#22c55e' : type === 'error' ? '#ef4444' : '#f97316';
      const el = document.createElement('div');
      el.id = 'me-toast';
      el.style.cssText = `position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:#1e293b;color:#e2e8f0;padding:12px 20px;border-radius:8px;border:1px solid ${border};font-size:13px;font-weight:500;z-index:10002;box-shadow:0 4px 20px rgba(0,0,0,0.45);max-width:90vw;text-align:center;`;
      el.textContent = message;
      document.body.appendChild(el);
      setTimeout(() => {
        el.style.transition = 'opacity 0.3s';
        el.style.opacity = '0';
        setTimeout(() => el.remove(), 300);
      }, 3200);
    }

    // In-page title menu for a new incident — replaces the native browser
    // prompt(). Resolves to the typed title, or null if cancelled. Title is
    // read from an <input> value (never innerHTML), so no injection risk.
    function askIncidentTitle() {
      return new Promise((resolve) => {
        const overlay = document.createElement('div');
        overlay.id = 'new-incident-title-modal';
        overlay.style.cssText = 'position:fixed;inset:0;background:rgba(2,6,23,0.6);z-index:10050;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(2px);';
        const box = document.createElement('div');
        box.style.cssText = 'background:#0b1220;border:1px solid rgba(148,163,184,0.3);border-radius:14px;padding:1.2rem 1.3rem;width:min(90vw,380px);box-shadow:0 20px 50px rgba(0,0,0,0.6);';
        box.innerHTML = `
          <div style="font-size:0.95rem;font-weight:600;color:#e2e8f0;margin-bottom:0.7rem;">New incident title</div>
          <input id="nit-input" type="text" placeholder="e.g. Structure fire — Smith St" autocomplete="off"
                 style="width:100%;padding:0.7rem;background:rgba(2,6,23,0.6);border:1px solid rgba(148,163,184,0.3);border-radius:8px;color:#e2e8f0;font-size:0.9rem;font-family:inherit;box-sizing:border-box;">
          <div style="display:flex;gap:0.5rem;justify-content:flex-end;margin-top:1rem;">
            <button id="nit-cancel" class="btn btn-secondary" style="padding:0.5rem 0.9rem;">Cancel</button>
            <button id="nit-create" class="btn btn-primary" style="padding:0.5rem 0.9rem;">Create</button>
          </div>`;
        overlay.appendChild(box);
        document.body.appendChild(overlay);
        const input = box.querySelector('#nit-input');
        input.focus();
        let done = false;
        const finish = (val) => { if (done) return; done = true; overlay.remove(); resolve(val); };
        const submit = () => { const v = input.value.trim(); if (v) finish(v); else input.focus(); };
        box.querySelector('#nit-cancel').onclick = () => finish(null);
        box.querySelector('#nit-create').onclick = submit;
        input.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') { e.preventDefault(); submit(); }
          else if (e.key === 'Escape') { e.preventDefault(); finish(null); }
        });
        overlay.addEventListener('click', (e) => { if (e.target === overlay) finish(null); });
      });
    }

    async function createIncidentAtLocation(lat, lng) {
      // Ask for the title via an in-page menu (not a native browser prompt).
      const title = await askIncidentTitle();
      if (!title) { if (addMode) toggleAddMode(); return; }

      // Temp pin the moment the title is confirmed — the real pin only
      // renders after create + reload, and until then the chosen spot
      // was invisible while the editor filled in details.
      const tempIcon = L.divIcon({
        className: 'custom-pin',
        html: '<div style="width:22px;height:22px;border-radius:50%;border:2px dashed #f59e0b;background:rgba(245,158,11,0.25);box-shadow:0 0 12px rgba(245,158,11,0.8);animation:pulse 1.4s infinite;"></div>',
        iconSize: [26, 26],
        iconAnchor: [13, 13]
      });
      const tempMarker = L.marker([lat, lng], { icon: tempIcon, zIndexOffset: 1000, interactive: false }).addTo(map);
      try {
        const location = await reverseGeocode(lat, lng);
        const expireTime = new Date(Date.now() + 2 * 3600000); // Default: 2 hours
        const res = await apiFetch(`${PROXY_BASE}/api/incidents`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            title, lat, lng, location: location || '', type: [], description: '',
            status: 'Going', size: '-', responding_agencies: [], expires_at: expireTime
          })
        });
        // Always reset add-mode so the next map click doesn't create another.
        if (addMode) toggleAddMode();
        if (!res.ok) {
          showToast('Failed to create incident.', 'error');
          return;
        }
        let newId = null;
        try { newId = (await res.json())?.id ?? null; } catch (e) { /* ignore */ }
        await reloadIncidents();
        // Open the freshly-created pin so the user can fill in details + save.
        if (newId) {
          try {
            const one = await apiFetch(`${PROXY_BASE}/api/incidents/${newId}`);
            if (one.ok) selectIncident(await one.json());
          } catch (e) { /* pin still created; user can click it */ }
        }
        showToast('Incident created.', 'success');
      } finally {
        map.removeLayer(tempMarker);
      }
    }

    async function saveIncident() {
      if (!selectedId) return;
      const agencies = Array
        .from(document.querySelectorAll('#agency-checkboxes input:checked'))
        .map(cb => cb.value);
      const types = Array
        .from(document.querySelectorAll('#type-checkboxes input:checked'))
        .map(cb => cb.value);
      // A typed-but-not-yet-added callsign still counts — pressing Save
      // shouldn't silently drop it.
      addUnitFromInput();
      const updates = {
        title: document.getElementById('edit-title').value,
        type: types,
        description: document.getElementById('edit-desc').value,
        status: document.getElementById('edit-status').value,
        size: document.getElementById('edit-size').value,
        responding_agencies: agencies,
        units: currentUnits,
        updated_at: new Date()
      };
      // The slider always carries a value, so the incident's auto-remove
      // time is set from it on every save (20 min – 12 h).
      updates.expires_at = new Date(Date.now() + getExpiryMinutes() * 60000);
      const res = await apiFetch(`${PROXY_BASE}/api/incidents/${selectedId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates)
      });
      if (!res.ok) {
        showToast('Failed to save incident.', 'error');
      } else {
        // Hide grammar results when saving
        const titleResults = document.getElementById('title-grammar-results');
        const descResults = document.getElementById('description-grammar-results');
        if (titleResults) titleResults.style.display = 'none';
        if (descResults) descResults.style.display = 'none';
        showToast('Incident saved.', 'success');
        reloadIncidents();
      }
    }
    
    // In-page confirm dialog — replaces the native confirm(). Resolves true
    // if the user confirms. Message is set via textContent (no injection).
    function askConfirm(message, opts = {}) {
      const confirmLabel = opts.confirmLabel || 'Confirm';
      const danger = opts.danger !== false; // default to danger styling
      return new Promise((resolve) => {
        const overlay = document.createElement('div');
        overlay.id = 'confirm-modal';
        overlay.style.cssText = 'position:fixed;inset:0;background:rgba(2,6,23,0.6);z-index:10060;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(2px);';
        const box = document.createElement('div');
        box.style.cssText = 'background:#0b1220;border:1px solid rgba(148,163,184,0.3);border-radius:14px;padding:1.2rem 1.3rem;width:min(90vw,380px);box-shadow:0 20px 50px rgba(0,0,0,0.6);';
        box.innerHTML = `
          <div id="cm-msg" style="font-size:0.92rem;color:#e2e8f0;margin-bottom:1rem;line-height:1.4;"></div>
          <div style="display:flex;gap:0.5rem;justify-content:flex-end;">
            <button id="cm-cancel" class="btn btn-secondary" style="padding:0.5rem 0.9rem;">Cancel</button>
            <button id="cm-ok" class="btn ${danger ? 'btn-danger' : 'btn-primary'}" style="padding:0.5rem 0.9rem;"></button>
          </div>`;
        box.querySelector('#cm-msg').textContent = message;
        box.querySelector('#cm-ok').textContent = confirmLabel;
        overlay.appendChild(box);
        document.body.appendChild(overlay);
        box.querySelector('#cm-ok').focus();
        let done = false;
        const onKey = (e) => { if (e.key === 'Escape') finish(false); };
        const finish = (val) => { if (done) return; done = true; document.removeEventListener('keydown', onKey); overlay.remove(); resolve(val); };
        document.addEventListener('keydown', onKey);
        box.querySelector('#cm-cancel').onclick = () => finish(false);
        box.querySelector('#cm-ok').onclick = () => finish(true);
        overlay.addEventListener('click', (e) => { if (e.target === overlay) finish(false); });
      });
    }

    async function deleteIncident() {
      if (!selectedId) return;
      const ok = await askConfirm('Delete this pin? It comes off the map now; its data stays in the logs until the data retention window expires, then is permanently deleted.', {
        confirmLabel: 'Delete', danger: true,
      });
      if (!ok) return;
      const res = await apiFetch(`${PROXY_BASE}/api/incidents/${selectedId}`, { method: 'DELETE' });
      if (!res.ok) {
        showToast('Failed to delete incident.', 'error');
        return;
      }
      resetEditor();
      reloadIncidents();
      showToast('Pin deleted.', 'success');
    }
    function showPagerClusterEditor(clusterEntry) {
      try {
        selectedId = null;
        const editorPanel = document.getElementById('selection-editor');
        const instruction = document.getElementById('instruction-text');
        if (!editorPanel || !instruction) return;

        editorPanel.style.display = 'block';
        instruction.style.display = 'none';

        const header = document.querySelector('#edit-header h4');
        if (header) { header.textContent = 'Pager hits at this location'; header.style.color = '#22c55e'; }

        const idDisplay = document.getElementById('edit-id-display');
        if (idDisplay) idDisplay.textContent = '';

        // Hide all incident editing controls + logs for pure pager view
        document.getElementById('title-group').style.display = 'none';
        document.getElementById('status-size-group').style.display = 'none';
        document.getElementById('expiry-group').style.display = 'none';
        document.getElementById('type-group').style.display = 'none';
        document.getElementById('agency-group').style.display = 'none';
        document.getElementById('desc-group').style.display = 'none';
        document.getElementById('save-delete-group').style.display = 'none';
        document.getElementById('log-section').style.display = 'none';
        const _ab2 = document.getElementById('btn-archive');
        if (_ab2) _ab2.style.display = 'none';

        const existingRfs = document.getElementById('rfs-read-only-data');
        if (existingRfs && existingRfs.parentNode) {
          existingRfs.parentNode.removeChild(existingRfs);
        }
        const existingPager = document.getElementById('pager-read-only-data');
        if (existingPager && existingPager.parentNode) {
          existingPager.parentNode.removeChild(existingPager);
        }

        const raw = clusterEntry.rawDetails || {};
        const details = {
          hits: raw.hits || clusterEntry.hits || [],
          clusterAliases: raw.clusterAliases || [],
          clusterAgencies: raw.clusterAgencies || [],
          incidentId: raw.incidentId || null,
          incidentIds: raw.incidentIds || []
        };

        const pagerBlock = document.createElement('div');
        pagerBlock.id = 'pager-read-only-data';
        pagerBlock.innerHTML = renderPagerDetailsHtml(details);

        const logSection = document.getElementById('log-section');
        editorPanel.insertBefore(pagerBlock, logSection);

        // Units are editable on pager clusters too, persisted against a
        // deterministic stub row keyed on the cluster's incident id (or
        // its coordinates when it has none).
        const stubKey = 'pager:' + (details.incidentId ||
          (details.incidentIds && details.incidentIds[0]) ||
          (clusterEntry.lat + ',' + clusterEntry.lon));
        if (typeof generateRfsId === 'function') {
          _unitsStub = {
            id: generateRfsId(stubKey),
            title: 'Pager Incident Log',
            lat: clusterEntry.lat || 0,
            lng: clusterEntry.lon || 0,
            tag: 'Pager'
          };
          currentUnits = [];
          renderUnitChips();
          currentImages = [];
          renderPhotoTiles();
          document.getElementById('units-group').style.display = 'block';
          document.getElementById('photos-group').style.display = 'block';
          placeUnitsGroup(true); // under the pager details block
          loadStubUnits(_unitsStub);
          // Incident logs work on pager clusters too, against the same
          // stub row (created on the first entry).
          selectedId = _unitsStub.id;
          document.getElementById('log-section').style.display = 'block';
          document.getElementById('new-update-msg').value = '';
          loadIncidentLogs(_unitsStub.id);
        } else {
          _unitsStub = null;
          document.getElementById('units-group').style.display = 'none';
        }

        if (clusterEntry.lat != null && clusterEntry.lon != null && map) {
          map.panTo([clusterEntry.lat, clusterEntry.lon]);
        }
      } catch (e) {
        console.error('showPagerClusterEditor error', e);
      }
    }

    // Render the pager-hits block under the editor form from the unified
    // item's merged pagerDetails ({hits, clusterAliases, clusterAgencies}).
    function renderPagerSection(details) {
      try {
        const editorPanel = document.getElementById('selection-editor');
        const logSection = document.getElementById('log-section');
        if (!editorPanel || !logSection) return;

        const existing = document.getElementById('pager-read-only-data');
        if (existing && existing.parentNode) existing.parentNode.removeChild(existing);

        if (!details || !Array.isArray(details.hits) || !details.hits.length) return;
        if (typeof renderPagerDetailsHtml !== 'function') return;

        const pagerBlock = document.createElement('div');
        pagerBlock.id = 'pager-read-only-data';
        pagerBlock.innerHTML = `
          <hr style="border:0; border-top:1px solid var(--border-subtle); margin:1.5rem 0 1rem 0;">
          <h3 style="font-size:0.85rem; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:0.5rem;">
            Pager hits at this location
          </h3>
          ${renderPagerDetailsHtml(details)}
        `;
        editorPanel.insertBefore(pagerBlock, logSection);
      } catch (e) {
        console.error('renderPagerSection error', e);
      }
    }

    // Reload just the editor-owned incidents; the public page's own
    // countdown refresh handles every other layer.
    // Staff/owner: preserve a major incident forever. Snapshots the pin +
    // its logs into the permanent archive (searchable from the logs page)
    // and removes the live pin from the map.
    async function archiveIncident() {
      if (!selectedId) return;
      const ok = await askConfirm(
        'Archive this incident? A permanent snapshot (including its logs) is saved to the archive and the pin is removed from the live map.',
        { confirmLabel: 'Archive', danger: false },
      );
      if (!ok) return;
      const res = await apiFetch(`${PROXY_BASE}/api/incidents/${selectedId}/archive`, { method: 'POST' });
      if (!res.ok) {
        showToast('Failed to archive incident.', 'error');
        return;
      }
      resetEditor();
      reloadIncidents();
      showToast('Incident archived.', 'success');
    }

    function refreshData() {
      reloadIncidents();
    }


  // --- Panel visibility -------------------------------------------------
  // Desktop: the panel floats top-right whenever the User layer is on.
  // The vessels/aircraft list panel no longer competes for that corner —
  // map.html docks it INSIDE this panel while Map Controls is open (see
  // syncUnifiedListPanelDock), so the old mutual-exclusion with the AIS
  // list is gone. Mobile (<=900px): .map-sidebar becomes a 60% bottom
  // sheet, so it stays COLLAPSED behind a floating button and only opens
  // when the user taps a pin or the button; a close X collapses it again.
  let mobileSheetOpen = false;

  function isMobileLayout() {
    return window.matchMedia('(max-width: 900px)').matches;
  }

  function editorPanelVisibility() {
    const editorPanel = document.getElementById('editor-panel');
    if (!editorPanel) return;
    const userBtn = document.getElementById('btn-user');
    const userOn = !userBtn || userBtn.classList.contains('active');
    const mobile = isMobileLayout();
    const open = userOn && (!mobile || mobileSheetOpen);
    editorPanel.classList.toggle('open', open);

    const fab = document.getElementById('editor-mobile-fab');
    if (fab) fab.style.display = (mobile && userOn && !open) ? 'flex' : 'none';
    const closeBtn = document.getElementById('editor-panel-close');
    if (closeBtn) closeBtn.style.display = mobile ? 'flex' : 'none';
  }

  function openEditorSheet() {
    mobileSheetOpen = true;
    editorPanelVisibility();
  }

  function closeEditorSheet() {
    mobileSheetOpen = false;
    editorPanelVisibility();
  }

  // --- DOM injection (CSS + floating panel), done only on activation ---
  const EDITOR_CSS = '    /* --- Editor sidebar custom scrollbar --- */\n    #editor-panel {\n      overflow-y: auto;\n      scrollbar-width: thin;\n      scrollbar-color: transparent transparent; /* Firefox default: hidden */\n    }\n    /* Desktop: Map Controls is the top SECTION of the single #right-dock\n       card (map.html owns the card chrome) — flat background, divider\n       below separating it from the vessels/aircraft section. A\n       floating-card fallback covers the rare case the dock is missing.\n       Mobile keeps its bottom-sheet. */\n    @media (min-width: 901px) {\n      #right-dock #editor-panel.map-sidebar {\n        position: static;\n        transform: none;\n        visibility: visible;\n        width: 100%;\n        height: auto;\n        max-height: none;\n        flex: 0 1 auto;\n        min-height: 0;\n        padding: 12px;\n        order: 0;\n        background: transparent;\n        border: 0;\n        border-bottom: 1px solid rgba(148, 163, 184, 0.18);\n        border-radius: 0;\n        box-shadow: none;\n        backdrop-filter: none;\n      }\n      #right-dock #editor-panel.map-sidebar:not(.open) { display: none; }\n      .map-container > #editor-panel.map-sidebar {\n        right: 14px;\n        top: 96px;\n        height: auto;\n        max-height: calc(100% - 110px);\n        border: 1px solid rgba(125, 211, 252, 0.35);\n        border-radius: 10px;\n        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.5);\n      }\n      #editor-panel h3 {\n        margin: 0 0 10px;\n        font-size: 13px;\n        font-weight: 600;\n        color: #7dd3fc;\n        letter-spacing: 0.02em;\n      }\n    }\n    #editor-panel::-webkit-scrollbar {\n      width: 10px;\n    }\n    #editor-panel::-webkit-scrollbar-track {\n      background: transparent;\n    }\n    #editor-panel::-webkit-scrollbar-thumb {\n      background: transparent;\n      border-radius: 999px;\n      border: 2px solid transparent;\n      box-shadow: none;\n      transition: background 0.25s ease, box-shadow 0.25s ease, border-color 0.25s ease;\n    }\n    /* Show + animate thumb while hovering or actively scrolling */\n    #editor-panel:hover,\n    #editor-panel.scrolling {\n      scrollbar-color: #4ade80 rgba(15,23,42,0.9); /* Firefox thumb + track */\n    }\n    #editor-panel:hover::-webkit-scrollbar-thumb,\n    #editor-panel.scrolling::-webkit-scrollbar-thumb {\n      background: linear-gradient(180deg, #22c55e, #0ea5e9);\n      border-color: rgba(15,23,42,0.9);\n      box-shadow: 0 0 8px rgba(34,197,94,0.8);\n    }\n    /* Extra glow animation while scrolling */\n    #editor-panel.scrolling::-webkit-scrollbar-thumb {\n      animation: sidebarScrollGlow 1.2s infinite alternate;\n    }\n    @keyframes sidebarScrollGlow {\n      0% { box-shadow: 0 0 4px rgba(34,197,94,0.4); }\n      100% { box-shadow: 0 0 14px rgba(34,197,94,1); }\n    }\n    \n    .pill-checkbox {\n      display: inline-flex; align-items: center; gap: 6px; padding: 6px 12px;\n      border-radius: 20px; border: 1px solid rgba(255,255,255,0.2);\n      background: rgba(255,255,255,0.05); color: #cbd5e1; font-size: 0.75rem;\n      cursor: pointer; user-select: none; transition: all 0.2s;\n    }\n    .pill-checkbox:hover { background: rgba(255,255,255,0.1); border-color: rgba(255,255,255,0.4); color: #fff; }\n    .pill-checkbox input { accent-color: var(--accent); }\n    .pill-checkbox:has(input:checked) { background: rgba(249, 115, 22, 0.2); border-color: #f97316; color: #fdba74; }\n\n    /* Incident Type(s) toggle pills + group headings */\n    .type-group-label {\n      width: 100%; font-size: 0.62rem; font-weight: 700; letter-spacing: 0.08em;\n      text-transform: uppercase; color: var(--text-soft); margin: 0.45rem 0 0.15rem;\n    }\n    .type-group-label:first-child { margin-top: 0; }\n    .type-pill {\n      display: inline-flex; align-items: center; padding: 5px 11px; border-radius: 999px;\n      border: 1px solid rgba(148,163,184,0.3); background: rgba(255,255,255,0.04);\n      color: #cbd5e1; font-size: 0.72rem; font-weight: 500; cursor: pointer;\n      user-select: none; transition: background 0.15s, border-color 0.15s, color 0.15s;\n    }\n    .type-pill:hover { background: rgba(255,255,255,0.09); border-color: rgba(148,163,184,0.5); color: #fff; }\n    .type-pill input { position: absolute; opacity: 0; width: 0; height: 0; pointer-events: none; }\n    .type-pill:has(input:checked) {\n      background: rgba(56,189,248,0.18); border-color: rgba(56,189,248,0.6); color: #7dd3fc;\n    }\n    .type-pill:focus-within { outline: 2px solid rgba(56,189,248,0.5); outline-offset: 1px; }\n    \n    /* Auto-remove slider */\n    .expiry-head { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 0.5rem; }\n    .expiry-value {\n      font-size: 0.8rem; font-weight: 700; color: #7dd3fc;\n      background: rgba(56,189,248,0.12); border: 1px solid rgba(56,189,248,0.35);\n      padding: 0.1rem 0.55rem; border-radius: 999px;\n    }\n    .expiry-range {\n      -webkit-appearance: none; appearance: none; width: 100%; height: 6px;\n      border-radius: 4px; outline: none; margin: 0.1rem 0;\n      background: rgba(255,255,255,0.15);\n    }\n    .expiry-range::-webkit-slider-runnable-track {\n      height: 6px; border-radius: 4px;\n      background: linear-gradient(90deg, #38bdf8 var(--percent, 50%), rgba(255,255,255,0.15) var(--percent, 50%));\n    }\n    .expiry-range::-moz-range-track {\n      height: 6px; border-radius: 4px;\n      background: linear-gradient(90deg, #38bdf8 var(--percent, 50%), rgba(255,255,255,0.15) var(--percent, 50%));\n    }\n    .expiry-range::-webkit-slider-thumb {\n      -webkit-appearance: none; width: 16px; height: 16px; border-radius: 50%;\n      background: #38bdf8; border: 2px solid #e0f2fe; cursor: pointer; margin-top: -5px;\n      box-shadow: 0 0 8px rgba(56,189,248,0.7);\n    }\n    .expiry-range::-moz-range-thumb {\n      width: 16px; height: 16px; border-radius: 50%; background: #38bdf8;\n      border: 2px solid #e0f2fe; cursor: pointer; box-shadow: 0 0 8px rgba(56,189,248,0.7);\n    }\n    .expiry-scale {\n      display: flex; justify-content: space-between; margin-top: 0.25rem;\n      font-size: 0.62rem; color: var(--text-soft);\n    }\n\n    /* Pin-move grab overlay: sized like the user pin icon so the ring\n       hugs the pin exactly. Hidden until hovered/dragged (the suggest\n       flow adds .always since its UI copy references the handle). */\n    .editor-move-ghost { width: 32px; height: 32px; display: flex; align-items: center; justify-content: center; cursor: move; }\n    .editor-move-ghost .ghost-ring { width: 28px; height: 28px; border-radius: 50%; border: 2px solid var(--ghost-color, #a855f7); box-shadow: 0 0 10px var(--ghost-glow, rgba(168, 85, 247, 0.8)); opacity: 0; transition: opacity 0.15s; }\n    .editor-move-ghost.always .ghost-ring,\n    .editor-move-ghost:hover .ghost-ring,\n    .ghost-dragging .editor-move-ghost .ghost-ring { opacity: 1; }\n    /* Neutral (non-orange) primary buttons inside the editor panel;\n       danger buttons keep their red. */\n    #editor-panel .btn-primary {\n      background: rgba(148, 163, 184, 0.12);\n      border: 1px solid rgba(148, 163, 184, 0.4);\n      color: #cbd5e1;\n    }\n    #editor-panel .btn-primary:hover {\n      background: rgba(125, 211, 252, 0.12);\n      border-color: #7dd3fc;\n      color: #e0f2fe;\n    }';

  const PANEL_HTML = '      <div class="map-sidebar open" id="editor-panel">\n        <h3>Map Controls</h3>\n\n        <div style="display:flex; gap:0.5rem; margin-bottom:1.5rem;">\n          <button class="btn btn-primary btn-block" id="btn-add-mode" onclick="toggleAddMode()">+ Add Pin</button>\n          <button class="btn btn-secondary" onclick="refreshData()" title="Reload Data">↻</button>\n        </div>\n        <hr style="border:0; border-top:1px solid var(--border-subtle); margin-bottom:1.5rem;">\n        <div id="selection-editor" style="display:none;">\n          <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1rem;" id="edit-header">\n            <h4 style="margin:0; color:var(--accent);">Edit Incident</h4>\n            <span style="font-size:0.7rem; color:var(--text-soft);" id="edit-id-display"></span>\n          </div>\n          <input type="hidden" id="edit-id">\n          <div class="form-group" id="title-group">\n            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.4rem;">\n              <label class="form-label" for="edit-title" style="margin-bottom:0;">Title</label>\n              <button class="btn btn-secondary" style="padding:0.25rem 0.5rem; font-size:0.7rem;" onclick="checkGrammarForTitle()" title="Check grammar">✓ Grammar</button>\n            </div>\n            <input type="text" id="edit-title" class="form-input">\n            <div id="title-grammar-results" style="display:none; margin-top:0.5rem;"></div>\n          </div>\n          \n          <div class="form-group" id="location-group" style="display:none;">\n            <label class="form-label">Location</label>\n            <div id="edit-location-display" style="color:#94a3b8; font-size:0.85rem; padding:0.5rem; background:rgba(0,0,0,0.2); border-radius:6px; border:1px solid var(--border-subtle);">\n              <i class="fa-solid fa-location-dot" style="margin-right:0.4rem; color:#f59e0b;"></i>\n              <span id="edit-location-text">-</span>\n            </div>\n          </div>\n          \n          <div style="display:grid; grid-template-columns: 1fr 1fr; gap:0.5rem;" id="status-size-group">\n            <div class="form-group">\n              <label class="form-label" for="edit-status">Status</label>\n              <select id="edit-status" class="form-select">\n                <option value="Going">Going</option>\n                <option value="In Route">In Route</option>\n                <option value="On Scene">On Scene</option>\n                <option value="Out of Control">Out of Control</option>\n                <option value="Being Controlled">Being Controlled</option>\n                <option value="Emergency Warning">Emergency Warning</option>\n                <option value="Watch and Act">Watch and Act</option>\n                <option value="Advice">Advice</option>\n                <option value="Under Control">Under Control</option>\n                <option value="Pending">Pending</option>\n                <option value="Investigation">Investigation</option>\n                <option value="Monitor">Monitor</option>\n                <option value="Patrol">Patrol</option>\n                <option value="Off Scene">Off Scene</option>\n                <option value="Safe">Safe</option>\n              </select>\n            </div>\n            <div class="form-group">\n              <label class="form-label" for="edit-size">Size</label>\n              <input type="text" id="edit-size" class="form-input" placeholder="e.g. 5 ha">\n            </div>\n          </div>\n\n          <div class="form-group" id="expiry-group">\n            <div class="expiry-head">\n              <label class="form-label" for="edit-expiry-range" style="margin-bottom:0;">Auto-Remove In</label>\n              <span id="edit-expiry-label" class="expiry-value">2 hours</span>\n            </div>\n            <input type="range" id="edit-expiry-range" class="expiry-range" min="0" max="10" step="1" value="5" oninput="updateExpiryLabel()">\n            <div class="expiry-scale"><span>20 min</span><span>12 hrs</span></div>\n          </div>\n          <div class="form-group" id="type-group">\n            <div class="form-label">Incident Type(s)</div>\n            <div id="type-checkboxes" style="display:flex; flex-wrap:wrap; gap:0.4rem; max-height:150px; overflow-y:auto; background:rgba(0,0,0,0.2); padding:0.5rem; border-radius:6px; border:1px solid var(--border-subtle);"></div>\n          </div>\n          <div class="form-group" id="agency-group">\n            <div class="form-label">Responding Agencies</div>\n            <div style="display:flex; flex-wrap:wrap; gap:0.5rem;" id="agency-checkboxes">\n              <label class="pill-checkbox"><input type="checkbox" value="RFS"> RFS</label>\n              <label class="pill-checkbox"><input type="checkbox" value="FRNSW"> FRNSW</label>\n              <label class="pill-checkbox"><input type="checkbox" value="NSWAS"> NSWAS</label>\n              <label class="pill-checkbox"><input type="checkbox" value="SES"> SES</label>\n              <label class="pill-checkbox"><input type="checkbox" value="Police"> Police</label>\n              <label class="pill-checkbox"><input type="checkbox" value="VRA"> VRA</label>\n            </div>\n          </div>\n          <div class="form-group" id="units-group">\n            <div class="form-label">Attached Units</div>\n            <div id="unit-chips" style="display:flex; flex-wrap:wrap; gap:0.4rem; margin-bottom:0.4rem;"></div>\n            <input type="text" id="unit-input" class="form-input" placeholder="Callsign - Enter adds, Tab completes" autocomplete="off">\n          </div>\n          <div class="form-group" id="photos-group">\n            <div class="form-label">Photos <span id="photo-count" style="color:#94a3b8; font-weight:400;"></span></div>\n            <div id="photo-tiles" style="display:flex; flex-wrap:wrap; gap:0.4rem;"></div>\n            <input type="file" id="photo-input" accept="image/jpeg,image/png,image/webp,image/gif" style="display:none;">\n          </div>\n          <div class="form-group" id="desc-group">\n            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.4rem;">\n              <label class="form-label" for="edit-desc" style="margin-bottom:0;">Description (Markdown)</label>\n              <button class="btn btn-secondary" style="padding:0.25rem 0.5rem; font-size:0.7rem;" onclick="checkGrammarForDescription()" title="Check grammar">✓ Grammar</button>\n            </div>\n            <textarea id="edit-desc" class="form-textarea" placeholder="Use **bold**, *italics*, or lists..."></textarea>\n            <div id="description-grammar-results" style="display:none; margin-top:0.5rem;"></div>\n          </div>\n          <div style="display:grid; grid-template-columns: 1fr 1fr; gap:0.5rem; margin-bottom:1.5rem;" id="save-delete-group">\n            <button class="btn btn-primary" onclick="saveIncident()">Save Changes</button>\n            <button class="btn btn-danger" onclick="deleteIncident()">Delete Pin</button>\n          </div>\n          <button class="btn btn-secondary btn-block" id="btn-archive" style="display:none; margin-bottom:1.5rem;" onclick="archiveIncident()" title="Staff only - preserves this incident forever">\n            <i class="fa-solid fa-box-archive"></i> Archive Incident\n          </button>\n\n          <!-- Ownership-aware panels (rebuilt per-selection in selectIncident):\n               - ownership-notice: shown to non-owners in place of direct edit.\n               - suggest-panel: non-owners propose edits / notes.\n               - suggestions-review-panel: owners/admins review pending items. -->\n          <div id="ownership-notice" style="display:none; background:rgba(59,130,246,0.08); border:1px solid rgba(59,130,246,0.3); color:#93c5fd; padding:0.7rem 0.8rem; border-radius:6px; margin-bottom:1rem; font-size:0.8rem;"></div>\n          <div id="suggest-panel" style="display:none; margin-bottom:1.5rem;"></div>\n          <div id="suggestions-review-panel" style="display:none; margin-bottom:1.5rem;"></div>\n\n          <div class="form-group" style="margin-top:1.5rem; border-top:1px solid rgba(255,255,255,0.1); padding-top:1rem;" id="log-section">\n            <div class="form-label">Incident Logs</div>\n            <div id="editor-logs-container" style="max-height: 250px; overflow-y: auto; background: rgba(0,0,0,0.2); border: 1px solid var(--border-subtle); border-radius: 6px; margin-bottom:0.8rem;">\n              <div style="padding:1rem; color:var(--text-soft); font-size:0.8rem;">Loading...</div>\n            </div>\n            <div style="background:rgba(255,255,255,0.03); padding:0.8rem; border-radius:8px;">\n              <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.4rem;">\n                <label class="form-label" for="new-update-msg" style="margin-bottom:0;">New Log Entry</label>\n                <button class="btn btn-secondary" style="padding:0.25rem 0.5rem; font-size:0.7rem;" onclick="checkGrammarForNew()" title="Check grammar">✓ Grammar</button>\n              </div>\n              <textarea id="new-update-msg" class="form-textarea" placeholder="Type update here..." style="min-height:50px; font-size:0.85rem; margin-bottom:0.5rem;"></textarea>\n              <div id="new-log-grammar-results" style="display:none; margin-top:0.5rem;"></div>\n              <button class="btn btn-secondary btn-block" style="padding:0.4rem;" onclick="addUpdate()">Post Update</button>\n            </div>\n          </div>\n\n        </div>\n        <div id="instruction-text" style="color:var(--text-soft); font-size:0.9rem; text-align:center; margin-top:0.5rem;">\n          Select a pin to edit<br>or click <strong>+ Add Pin</strong> to create new.\n        </div>\n      </div>';

  function injectEditorDom() {
    if (document.getElementById('editor-panel')) return;
    const style = document.createElement('style');
    style.id = 'editor-module-css';
    style.textContent = EDITOR_CSS;
    document.head.appendChild(style);

    // Desktop: stack inside the shared right-side dock (map.html) so
    // panels never overlap; mobile keeps .map-container so the
    // bottom-sheet positioning works. map.html re-homes the panel on
    // breakpoint changes (updateMobileFiltersPlacement).
    const mapContainer = document.querySelector('.map-container') || document.body;
    const desktop = window.matchMedia('(min-width: 901px)').matches;
    const host = (desktop && document.getElementById('right-dock')) || mapContainer;
    const wrap = document.createElement('div');
    wrap.innerHTML = PANEL_HTML;
    host.appendChild(wrap.firstElementChild);

    // Mobile: floating button that opens the collapsed bottom sheet.
    const fab = document.createElement('button');
    fab.id = 'editor-mobile-fab';
    fab.type = 'button';
    fab.title = 'Map Controls';
    fab.setAttribute('aria-label', 'Open Map Controls');
    fab.innerHTML = '<i class="fa-solid fa-map-pin"></i>';
    fab.style.cssText = 'display:none; position:fixed; bottom:calc(84px + env(safe-area-inset-bottom, 0px)); right:12px; z-index:1004; width:48px; height:48px; border-radius:50%; background:#f97316; color:#fff; border:none; box-shadow:0 6px 20px rgba(0,0,0,0.5); font-size:1.05rem; align-items:center; justify-content:center; cursor:pointer;';
    fab.onclick = openEditorSheet;
    // The fab is position:fixed and mobile-only — keep it out of the
    // dock (which is display:none on mobile) so it can always show.
    mapContainer.appendChild(fab);

    // Mobile: close X inside the sheet (hidden on desktop).
    const panel = document.getElementById('editor-panel');
    if (panel) {
      const closeBtn = document.createElement('button');
      closeBtn.id = 'editor-panel-close';
      closeBtn.type = 'button';
      closeBtn.setAttribute('aria-label', 'Close Map Controls');
      closeBtn.innerHTML = '&times;';
      closeBtn.style.cssText = 'display:none; position:absolute; top:10px; right:12px; z-index:5; width:32px; height:32px; border-radius:8px; background:rgba(148,163,184,0.12); border:1px solid rgba(148,163,184,0.25); color:#cbd5e1; font-size:1.2rem; align-items:center; justify-content:center; cursor:pointer;';
      closeBtn.onclick = closeEditorSheet;
      panel.insertBefore(closeBtn, panel.firstChild);
    }
  }

  // --- Map click: place pin in add-mode (snap to pager cluster when the
  // page exposes pagerClustersByKey), otherwise clear the selection. ---
  async function onMapClick(e) {
    if (addMode) {
      let targetLat = e.latlng.lat;
      let targetLng = e.latlng.lng;
      try {
        if (typeof pagerClustersByKey !== 'undefined' && pagerClustersByKey) {
          const SNAP_THRESHOLD_METRES = 30;
          let bestDist = Infinity;
          let best = null;
          Object.values(pagerClustersByKey).forEach((entry) => {
            if (!entry) return;
            const d = haversineMeters(targetLat, targetLng, entry.lat, entry.lon);
            if (d <= SNAP_THRESHOLD_METRES && d < bestDist) { bestDist = d; best = entry; }
          });
          if (best) { targetLat = best.lat; targetLng = best.lon; }
        }
      } catch (err) { /* snap is best-effort */ }
      await createIncidentAtLocation(targetLat, targetLng);
    } else {
      resetEditor();
    }
  }


  // --- Drag-to-move: the public unified pins aren't draggable, so when an
  // editable incident is selected we float a draggable ghost handle on it;
  // dropping it saves the new coords (and reverse-geocoded location). ---
  let dragGhost = null;

  function removeDragGhost() {
    if (dragGhost) {
      try { map.removeLayer(dragGhost); } catch (e) { /* already gone */ }
      dragGhost = null;
    }
  }

  // Invisible-until-hover grab overlay sized/anchored EXACTLY like the
  // user pin icons (32px box, centre anchor) so the highlight ring hugs
  // the pin instead of floating off-centre. `always` keeps the ring
  // visible (used by the suggest-a-move flow, which tells the user to
  // drag "the blue handle").
  function buildGhostIcon(color, glow, title, always) {
    return L.divIcon({
      className: 'custom-pin',
      html: '<div class="editor-move-ghost' + (always ? ' always' : '') + '" title="' + title + '" style="--ghost-color:' + color + ';--ghost-glow:' + glow + ';"><div class="ghost-ring"></div></div>',
      iconSize: [32, 32],
      iconAnchor: [16, 16],
    });
  }

  // Show the ring while dragging too (the hover state doesn't survive
  // the pointer leaving the element mid-drag).
  function wireGhostDragClass(marker) {
    marker.on('dragstart', () => {
      const el = marker.getElement();
      if (el) el.classList.add('ghost-dragging');
    });
    marker.on('dragend', () => {
      const el = marker.getElement();
      if (el) el.classList.remove('ghost-dragging');
    });
  }

  function updateDragGhost(inc, canModify) {
    removeDragGhost();
    if (!canModify || !Number.isFinite(+inc.lat) || !Number.isFinite(+inc.lng)) return;
    const icon = buildGhostIcon('#a855f7', 'rgba(168,85,247,0.8)', 'Drag to move this pin', false);
    dragGhost = L.marker([inc.lat, inc.lng], { icon, draggable: true, zIndexOffset: 2000 }).addTo(map);
    wireGhostDragClass(dragGhost);
    dragGhost.bindTooltip('Drag to move this pin', { direction: 'top', offset: [0, -14], opacity: 0.9 });
    dragGhost.on('dragend', async (e) => {
      const pos = e.target.getLatLng();
      try {
        const newLocation = await reverseGeocode(pos.lat, pos.lng);
        const res = await apiFetch(`${PROXY_BASE}/api/incidents/${inc.id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ lat: pos.lat, lng: pos.lng, location: newLocation || '' }),
        });
        if (!res.ok) throw new Error('save failed');
        inc.lat = pos.lat;
        inc.lng = pos.lng;
        if (selectedId === inc.id && newLocation) {
          document.getElementById('edit-location-text').textContent = newLocation;
          document.getElementById('location-group').style.display = 'block';
        }
        showToast('Pin moved.', 'success');
        reloadIncidents();
      } catch (err) {
        showToast('Failed to move pin — reverting.', 'error');
        e.target.setLatLng([inc.lat, inc.lng]);
      }
    });
  }

  // --- Suggest-a-move (non-owner editors): same ghost mechanic, but the
  // drop records proposed coords into the suggestion instead of saving.
  // Only the pin's creator (or staff) may move directly; everyone else
  // proposes, and the owner's approval applies it. ---
  let suggestedMove = null;
  let suggestGhost = null;

  function clearSuggestMove() {
    suggestedMove = null;
    if (suggestGhost) {
      try { map.removeLayer(suggestGhost); } catch (e) { /* gone */ }
      suggestGhost = null;
    }
    const disp = document.getElementById('sugg-move-display');
    if (disp) disp.textContent = '';
    const clearBtn = document.getElementById('sugg-move-clear');
    if (clearBtn) clearBtn.style.display = 'none';
  }

  function startSuggestMove() {
    if (!currentIncident) return;
    clearSuggestMove();
    const inc = currentIncident;
    if (!Number.isFinite(+inc.lat) || !Number.isFinite(+inc.lng)) return;
    const icon = buildGhostIcon('#38bdf8', 'rgba(56,189,248,0.8)', 'Drag to the proposed location', true);
    suggestGhost = L.marker([inc.lat, inc.lng], { icon, draggable: true, zIndexOffset: 2000 }).addTo(map);
    wireGhostDragClass(suggestGhost);
    suggestGhost.bindTooltip('Drag to the proposed location', { direction: 'top', offset: [0, -14], opacity: 0.9 });
    const disp = document.getElementById('sugg-move-display');
    if (disp) disp.textContent = 'Drag the blue handle to where the pin should be.';
    suggestGhost.on('dragend', async (e) => {
      const pos = e.target.getLatLng();
      let loc = '';
      try { loc = await reverseGeocode(pos.lat, pos.lng); } catch (err) { /* coords still fine */ }
      suggestedMove = { lat: pos.lat, lng: pos.lng, location: loc || '' };
      const d = document.getElementById('sugg-move-display');
      if (d) d.textContent = `Proposed: ${loc || 'new position'} (${pos.lat.toFixed(5)}, ${pos.lng.toFixed(5)})`;
      const clearBtn = document.getElementById('sugg-move-clear');
      if (clearBtn) clearBtn.style.display = 'inline-block';
    });
  }

  // --- Activation: dormant unless the session passes the role check. ---
  async function activateEditor() {
    try {
      if (!window.sb || typeof map === 'undefined') return;
      const { data } = await sb.auth.getSession();
      const session = data && data.session;
      if (!session) return;

      const res = await fetch(`${API_BASE_URL}/api/check-editor/${session.user.id}`);
      if (!res.ok) return;
      const role = await res.json();
      if (!role.has_access) return; // pending / role-less users stay on the public map

      currentUserId = session.user.id;
      currentIsAdmin = !!(role.is_owner || role.is_team_member);
      currentUserEmail = session.user.email || null;

      injectEditorDom();
      renderTypeCheckboxes();
      wireUnitInput();
      wirePhotoInput();
      renderPhotoTiles();
      loadCallsignDict();

      // Pins stay with the public unified renderer (user/RFS/pager keep
      // merging and every layer toggle keeps working). The hooks below
      // reroute pin CLICKS from the public #incident-sidebar into the
      // editor panel instead.
      window.__editorActive = true;
      window.NSWPSNEditorHooks = {
        // showIncidentDetails(incident, pagerDetails) — raw incident row.
        openUser(incident, pagerDetails) {
          try { selectIncident(incident, pagerDetails); openEditorSheet(); } catch (e) { console.warn('[editor] openUser', e); }
        },
        // showRfsDetails(item) — unified RFS item.
        openRfs(item) {
          try {
            const d = item.rawDetails || {};
            selectRfsIncident({
              id: item.id,
              title: item.title,
              STATUS: d.STATUS || d.status || 'Unknown',
              LOCATION: d.LOCATION || d.location || '',
              link: item.link || 'https://www.rfs.nsw.gov.au/fire-information/fires-near-me',
              point: `${item.lat} ${item.lng}`,
            }, item.pagerDetails);
            openEditorSheet();
          } catch (e) { console.warn('[editor] openRfs', e); }
        },
        // showPagerDetails(item) — unified pager cluster item.
        openPager(item) {
          try {
            showPagerClusterEditor({ lat: item.lat, lon: item.lng, rawDetails: item.rawDetails || {} });
            openEditorSheet();
          } catch (e) { console.warn('[editor] openPager', e); }
        },
      };

      // Editors get a longer pager lookback: 48h instead of the public 24h.
      try {
        const range = document.getElementById('pager-hours-range');
        if (range) range.max = '48';
        const hint = document.querySelector('#pager-panel .pager-hint');
        if (hint) hint.textContent = 'Shows pager hits from the last N hours (editors: up to 48h)';
        // The public init ran before the max was raised and clamps the
        // stored window to the max — re-run it so a saved 25-48h window
        // is re-adopted, then refresh if the effective value changed.
        if (typeof pagerHours !== 'undefined' && typeof initPagerHoursFromStorage === 'function') {
          const before = pagerHours;
          initPagerHoursFromStorage();
          if (pagerHours !== before && typeof loadAllData === 'function') loadAllData();
        }
      } catch (e) { /* non-fatal */ }

      // Panel visibility: User-layer-gated, AIS-deferring, and collapsed
      // into a floating button on mobile (see editorPanelVisibility).
      try {
        const aisPanel = document.getElementById('ais-list-panel');
        const userBtn = document.getElementById('btn-user');
        if (window.MutationObserver) {
          const mo = new MutationObserver(editorPanelVisibility);
          if (aisPanel) mo.observe(aisPanel, { attributes: true, attributeFilter: ['class'] });
          if (userBtn) mo.observe(userBtn, { attributes: true, attributeFilter: ['class'] });
        }
        window.addEventListener('resize', editorPanelVisibility, { passive: true });
        editorPanelVisibility();
      } catch (e) { /* non-fatal */ }

      map.on('click', onMapClick);
      console.log('[editor] editor mode active');
    } catch (e) {
      console.warn('[editor] staying dormant:', e && e.message);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', activateEditor);
  } else {
    activateEditor();
  }

  // --- Exports: only what inline on* handlers in the injected HTML use ---
  window.addUpdate = addUpdate;
  window.applyGrammarFixes = applyGrammarFixes;
  window.approveSuggestion = approveSuggestion;
  window.cancelEditLog = cancelEditLog;
  window.checkGrammar = checkGrammar;
  window.checkGrammarForDescription = checkGrammarForDescription;
  window.checkGrammarForEdit = checkGrammarForEdit;
  window.checkGrammarForNew = checkGrammarForNew;
  window.checkGrammarForTitle = checkGrammarForTitle;
  window.deleteIncident = deleteIncident;
  window.deleteIncidentPhoto = deleteIncidentPhoto;
  window.deleteLog = deleteLog;
  window.updateExpiryLabel = updateExpiryLabel;
  window.deselectAllGrammarFixes = deselectAllGrammarFixes;
  window.refreshData = refreshData;
  window.rejectSuggestion = rejectSuggestion;
  window.saveEditLog = saveEditLog;
  window.saveIncident = saveIncident;
  window.selectAllGrammarFixes = selectAllGrammarFixes;
  window.startEditLog = startEditLog;
  window.submitSuggestionEdit = submitSuggestionEdit;
  window.submitSuggestionNote = submitSuggestionNote;
  window.toggleAddMode = toggleAddMode;
  window.updateGrammarPreviewFromCheckbox = updateGrammarPreviewFromCheckbox;
  window.startSuggestMove = startSuggestMove;
  window.clearSuggestMove = clearSuggestMove;
  window.archiveIncident = archiveIncident;
})();
