/* NSW PSN agency directory — populates the sidebar Agencies dropdown.
   Each agency links to agency.html?slug=<slug>; agency data is loaded from
   agency-data.json (TGIDs) and agency-extended.json (extended tables/codes/zones).
   The dropdown also hosts a global search box that filters across both data sets. */
(function () {
  const RESULTS_LIMIT = 30;

  function escapeHtml(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    }[c]));
  }

  // Strip {pill:variant:label} / {b:text} markup tokens to plain text for indexing/preview.
  function stripMarkup(value) {
    return String(value == null ? "" : value).replace(/\{(pill|b):([^}]+)\}/g, (_, kind, body) => {
      if (kind === "pill") {
        const idx = body.indexOf(":");
        return idx >= 0 ? body.slice(idx + 1) : body;
      }
      return body;
    });
  }

  function renderAgency(a) {
    const safeName = escapeHtml(a.name);
    const href = `agency.html?slug=${encodeURIComponent(a.slug)}`;
    const icon = a.icon ? `<i class="agency-item-icon ${escapeHtml(a.icon)}" aria-hidden="true"></i>` : "";
    return `<a class="agency-item-link" href="${href}" data-slug="${escapeHtml(a.slug)}">${icon}<span class="agency-item-name">${safeName}</span></a>`;
  }

  function renderCategory(cat) {
    const items = cat.agencies.map(renderAgency).join("");
    return `
      <details class="agency-cat" data-cat="${escapeHtml(cat.slug || cat.name)}">
        <summary><span class="agency-cat-name">${escapeHtml(cat.name)}</span></summary>
        <div class="agency-cat-list">${items}</div>
      </details>`;
  }

  function highlightCurrent(mount) {
    const params = new URLSearchParams(location.search);
    const path = (location.pathname.split("/").pop() || "").toLowerCase();
    const slug = (params.get("slug") || "").toLowerCase();

    function activate(s) {
      let link;
      if (typeof CSS !== "undefined" && typeof CSS.escape === "function") {
        link = mount.querySelector(`.agency-item-link[data-slug="${CSS.escape(s)}"]`);
      } else {
        // Fallback for very old browsers lacking CSS.escape.
        const items = mount.querySelectorAll(".agency-item-link");
        for (let i = 0; i < items.length; i++) {
          if (items[i].dataset.slug === s) { link = items[i]; break; }
        }
      }
      if (!link) return;
      link.classList.add("active");
      let el = link.closest("details");
      while (el) {
        el.open = true;
        el = el.parentElement && el.parentElement.closest("details");
      }
    }

    if (path === "agency.html" && slug) return activate(slug);
    const legacy = {
      "fire-and-rescue.html": "fire-and-rescue-nsw",
      "ambulance.html": "nsw-ambulance",
      "rural-fire-service.html": "nsw-rural-fire-service",
      "aviation.html": "nsw-aviation",
    };
    if (legacy[path]) activate(legacy[path]);
  }

  // ---------- search index ----------

  function buildIndex(tgidData, extData) {
    const idx = [];
    const agencies = (tgidData && tgidData.agencies) || {};
    for (const slug in agencies) {
      const a = agencies[slug];
      idx.push({
        type: "agency",
        slug,
        agencyName: a.name,
        category: a.category,
        icon: a.icon,
        primary: a.name,
        secondary: a.category,
        haystack: `${a.name} ${a.category}`.toLowerCase(),
      });
      for (const t of a.tgids || []) {
        const alias = t.alias && t.alias !== "-" ? t.alias : "";
        const desc = t.description && t.description !== "-" ? t.description : "";
        idx.push({
          type: "tgid",
          slug,
          agencyName: a.name,
          icon: a.icon,
          primary: t.tgid,
          secondary: [alias, desc].filter(Boolean).join(" — ") || a.name,
          haystack: `${t.tgid} ${alias} ${desc} ${a.name}`.toLowerCase(),
          q: t.tgid,
        });
      }
    }
    const extAgencies = (extData && extData.agencies) || {};
    for (const slug in extAgencies) {
      const ext = extAgencies[slug];
      const agencyName = (agencies[slug] || {}).name || slug;
      const icon = (agencies[slug] || {}).icon;
      function indexTable(tbl, sectionTitle, kind) {
        if (!tbl || !tbl.rows) return;
        for (const row of tbl.rows) {
          const cells = row.map(stripMarkup);
          const primary = cells[0] || sectionTitle;
          const secondary = cells.slice(1).filter(Boolean).join(" — ") || sectionTitle;
          idx.push({
            type: kind,
            slug,
            agencyName,
            icon,
            sectionTitle,
            primary,
            secondary,
            haystack: `${cells.join(" ")} ${sectionTitle} ${agencyName}`.toLowerCase(),
            q: primary,
          });
        }
      }
      for (const sec of ext.sections || []) {
        const title = sec.title || "";
        const kindGuess = guessKind(title);
        if (sec.type === "table") indexTable(sec.table, title, kindGuess);
        if (sec.type === "grouped-table") {
          for (const g of sec.groups || []) indexTable(g.table, `${title} — ${g.heading}`, kindGuess);
        }
      }
    }
    return idx;
  }

  function guessKind(sectionTitle) {
    const t = (sectionTitle || "").toLowerCase();
    if (t.includes("zone")) return "zone";
    if (t.includes("priority")) return "priority";
    if (t.includes("status")) return "status";
    if (t.includes("appliance") || t.includes("prefix") || t.includes("categor")) return "appliance";
    if (t.includes("call") || t.includes("code")) return "code";
    if (t.includes("phrase") || t.includes("radio")) return "phrase";
    return "row";
  }

  function searchIndex(idx, query, limit) {
    const q = (query || "").trim().toLowerCase();
    if (!q) return [];
    const tokens = q.split(/\s+/).filter(Boolean);
    const out = [];
    for (const e of idx) {
      let ok = true;
      for (const t of tokens) {
        if (!e.haystack.includes(t)) { ok = false; break; }
      }
      if (ok) {
        out.push(e);
        if (out.length >= limit) break;
      }
    }
    return out;
  }

  // ---------- search ui ----------

  function renderSearchPanel() {
    return `
      <div class="agency-search">
        <div class="agency-search-input-wrap">
          <i class="fa-solid fa-magnifying-glass agency-search-input-icon" aria-hidden="true"></i>
          <input type="search" class="agency-search-input" placeholder="Search agencies, TGIDs, zones, codes…" autocomplete="off" aria-label="Search agencies and talkgroups">
          <button type="button" class="agency-search-clear" aria-label="Clear search" hidden>×</button>
        </div>
        <div class="agency-search-results" hidden></div>
      </div>`;
  }

  function typeBadgeText(type) {
    if (type === "tgid") return "TGID";
    if (type === "agency") return "Agency";
    if (type === "zone") return "Zone";
    if (type === "priority") return "Priority";
    if (type === "status") return "Status";
    if (type === "appliance") return "Fleet";
    if (type === "code") return "Code";
    if (type === "phrase") return "Radio";
    return "Item";
  }

  function highlightMatch(text, query) {
    const q = (query || "").trim();
    if (!q) return escapeHtml(text);
    const tokens = q.split(/\s+/).filter(Boolean);
    if (!tokens.length) return escapeHtml(text);
    // Build a pattern that matches any of the tokens (case-insensitive)
    const escaped = tokens.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
    const re = new RegExp(`(${escaped.join("|")})`, "ig");
    return escapeHtml(text).replace(re, "<mark>$1</mark>");
  }

  function renderResults(results, query) {
    if (!results.length) {
      return `<div class="agency-search-empty">No matches.</div>`;
    }
    // Group by agency for readability
    const groups = new Map();
    for (const r of results) {
      if (!groups.has(r.slug)) groups.set(r.slug, { name: r.agencyName, icon: r.icon, items: [] });
      groups.get(r.slug).items.push(r);
    }
    const html = [];
    for (const [slug, g] of groups) {
      const icon = g.icon ? `<i class="agency-search-group-icon ${escapeHtml(g.icon)}" aria-hidden="true"></i>` : "";
      html.push(`<div class="agency-search-group">
        <a class="agency-search-group-header" href="agency.html?slug=${encodeURIComponent(slug)}">
          ${icon}<span>${escapeHtml(g.name)}</span>
        </a>`);
      for (const it of g.items) {
        const params = new URLSearchParams({ slug });
        if (it.q) params.set("q", it.q);
        const href = `agency.html?${params.toString()}`;
        const primary = highlightMatch(it.primary || "", query);
        const secondary = it.secondary ? highlightMatch(it.secondary, query) : "";
        html.push(`<a class="agency-search-item" href="${href}">
          <span class="agency-search-item-type">${typeBadgeText(it.type)}</span>
          <span class="agency-search-item-text">
            <span class="agency-search-item-primary">${primary}</span>
            ${secondary ? `<span class="agency-search-item-secondary">${secondary}</span>` : ""}
          </span>
        </a>`);
      }
      html.push(`</div>`);
    }
    return html.join("");
  }

  function attachSearch(mount, idx) {
    const root = mount.querySelector(".agency-root");
    const input = mount.querySelector(".agency-search-input");
    const clearBtn = mount.querySelector(".agency-search-clear");
    const resultsBox = mount.querySelector(".agency-search-results");
    const rootList = mount.querySelector(".agency-root-list");
    if (!input || !resultsBox || !rootList) return;

    function update() {
      const q = input.value;
      if (q && q.trim()) {
        const results = searchIndex(idx, q, RESULTS_LIMIT);
        resultsBox.innerHTML = renderResults(results, q);
        resultsBox.hidden = false;
        rootList.classList.add("agency-root-list-hidden");
        clearBtn.hidden = false;
        if (root && !root.open) root.open = true;
      } else {
        resultsBox.hidden = true;
        resultsBox.innerHTML = "";
        rootList.classList.remove("agency-root-list-hidden");
        clearBtn.hidden = true;
      }
    }

    input.addEventListener("input", update);
    input.addEventListener("focus", () => {
      if (root && !root.open) root.open = true;
    });
    clearBtn.addEventListener("click", () => {
      input.value = "";
      update();
      input.focus();
    });
    input.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        input.value = "";
        update();
        input.blur();
      }
    });
  }

  // ---------- shared data fetches (de-duped across scripts) ----------
  // Both agencies.js and agency.html need the same two JSON files. Without
  // sharing, each script issues its own request — visible as 2× requests
  // for agency-data.json and agency-extended.json in the network panel.
  // We expose lazy promises on `window` so whichever script asks first
  // kicks off the fetch and the other one awaits the same promise.
  window.AgencyJSON = window.AgencyJSON || {
    data: null,
    extended: null,
    loadData() {
      if (!this.data) {
        this.data = fetch("agency-data.json").then((r) => {
          if (!r.ok) throw new Error("HTTP " + r.status);
          return r.json();
        });
      }
      return this.data;
    },
    loadExtended() {
      if (!this.extended) {
        this.extended = fetch("agency-extended.json").then((r) => {
          if (!r.ok) return { agencies: {} };
          return r.json();
        }).catch(() => ({ agencies: {} }));
      }
      return this.extended;
    },
  };

  // ---------- main ----------

  async function render() {
    const mount = document.getElementById("agencies-nav-mount");
    if (!mount) return;
    let tgidData;
    try {
      tgidData = await window.AgencyJSON.loadData();
    } catch (e) {
      mount.innerHTML = `<div class="sidebar-section-label">Agencies</div><div style="color:var(--text-soft); font-size:0.78rem; padding:0.4rem 0.7rem;">Failed to load agency list.</div>`;
      return;
    }
    const extData = await window.AgencyJSON.loadExtended();

    const cats = tgidData.categories || [];
    mount.innerHTML = `
      <div class="sidebar-section-label">Agencies</div>
      <details class="agency-root" open>
        <summary><span class="agency-root-name">All Agencies</span></summary>
        <div class="agency-root-body">
          ${renderSearchPanel()}
          <div class="agency-root-list">
            ${cats.map(renderCategory).join("")}
          </div>
        </div>
      </details>`;

    highlightCurrent(mount);
    const idx = buildIndex(tgidData, extData);
    attachSearch(mount, idx);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", render);
  } else {
    render();
  }
})();
