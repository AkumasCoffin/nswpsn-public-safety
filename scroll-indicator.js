/* Floating scroll-card indicator + scroll-spy rail (agency reference pages only).
   Renders a thin rail along the right side with one dot per snap target (header
   plus every top-level card). Each dot is clickable and scrolls the page to that
   card. The active dot glows and is tracked by a floating tooltip that shows the
   card title.
   Uses capture-phase scroll listeners on both window and document so it works
   regardless of which element is the actual scroll container. */
(function () {
  if (typeof window === "undefined") return;
  if (window.matchMedia && window.matchMedia("(max-width: 900px)").matches) return;

  // Tracked sections only — the page header is intentionally excluded so the
  // first pin always represents the first content card (otherwise the page-load
  // active pin tends to land on the second one as the header sits above focus).
  const TARGET_SELECTOR = ".card-grid > .card, .main > .card";
  const HIDE_DELAY_MS = 2200;

  let rail = null;
  let thumb = null;
  let tip = null;
  let textEl = null;
  let pairs = []; // [{ els: [el,...], dot, title }] — multiple els per pair when cards share a row
  let activePair = null;
  let hideTimer = null;
  let rebuildPending = false;
  let lastTargetCount = 0;
  const ROW_TOLERANCE_PX = 16;

  function getTitle(el) {
    if (el.classList && el.classList.contains("main-header")) {
      const h1 = el.querySelector("h1");
      if (h1) return h1.textContent.trim();
      const bc = el.querySelector(".breadcrumb");
      return bc ? bc.textContent.trim() : "Top";
    }
    const t = el.querySelector(".card-title");
    return t ? t.textContent.trim() : "Section";
  }

  function show() {
    if (!tip) return;
    tip.classList.add("visible");
    if (hideTimer) clearTimeout(hideTimer);
    hideTimer = setTimeout(() => tip.classList.remove("visible"), HIDE_DELAY_MS);
  }

  // Resolve the actual scroll container. With this layout (`body { display: flex }`,
  // `html, body { height: 100% }`, `.main { min-height: 100vh }`), overflow lives
  // inside `.main`, not on the window. Pick whichever element actually scrolls.
  function getScroller() {
    const candidates = [];
    const main = document.querySelector("main.main");
    if (main) candidates.push(main);
    if (document.scrollingElement) candidates.push(document.scrollingElement);
    if (document.documentElement) candidates.push(document.documentElement);
    if (document.body) candidates.push(document.body);
    for (const el of candidates) {
      if (!el) continue;
      if (el.scrollHeight - el.clientHeight > 1) return el;
    }
    return document.scrollingElement || document.documentElement;
  }

  function getScrollY() {
    const el = getScroller();
    return el === document.documentElement || el === document.body
      ? (window.scrollY || window.pageYOffset || el.scrollTop || 0)
      : (el.scrollTop || 0);
  }

  function getDocScrollHeight() {
    const el = getScroller();
    return el ? el.scrollHeight : 0;
  }

  function getViewportHeight() {
    const el = getScroller();
    return el ? el.clientHeight : window.innerHeight;
  }

  function setScrollY(y) {
    const el = getScroller();
    // `behavior: instant` is supported on Element.scrollTo too. Direct assignment
    // is the most reliable cross-browser fallback.
    if (typeof el.scrollTo === "function") {
      try { el.scrollTo({ top: y, left: el.scrollLeft || 0, behavior: "instant" }); return; } catch (_) {}
    }
    el.scrollTop = y;
  }

  function groupRows(targets) {
    // Group consecutive targets that share approximately the same vertical row.
    // Cards in a 2-column CSS grid will have matching offsetTop; merge them so a
    // single dot represents that row with a combined "Card 1 / Card 2" tooltip.
    const scrollY = getScrollY();
    const groups = [];
    for (const el of targets) {
      const r = el.getBoundingClientRect();
      const top = r.top + scrollY;
      const last = groups.length ? groups[groups.length - 1] : null;
      if (last && Math.abs(last.top - top) < ROW_TOLERANCE_PX) {
        last.els.push(el);
      } else {
        groups.push({ top, els: [el] });
      }
    }
    return groups;
  }

  function clearDots() {
    if (!rail) return;
    rail.querySelectorAll(".scroll-spy-dot").forEach((d) => d.remove());
  }

  function ensureThumb() {
    if (!rail) return;
    if (!thumb || !rail.contains(thumb)) {
      thumb = document.createElement("div");
      thumb.className = "scroll-spy-thumb";
      rail.appendChild(thumb);
    }
  }

  function buildRail() {
    if (!rail) return;
    const targets = Array.from(document.querySelectorAll(TARGET_SELECTOR));
    if (targets.length < 2) {
      clearDots();
      ensureThumb();
      rail.classList.add("scroll-spy-rail-empty");
      pairs = [];
      activePair = null;
      return;
    }
    rail.classList.remove("scroll-spy-rail-empty");

    const groups = groupRows(targets);

    clearDots();
    ensureThumb();
    pairs = [];
    const previousActiveEls = activePair ? activePair.els : null;
    activePair = null;

    // Position each dot at the same place the thumb sits when that section is
    // scrolled to the top of the viewport. Both are pct = scrollY/scrollHeight,
    // which makes dot-and-thumb alignment exact for any document size.
    const sc = getScroller();
    const scrollHeight = Math.max(1, sc.scrollHeight);

    for (const grp of groups) {
      // Use the topmost element in the row to determine the snap position.
      let topMost = grp.els[0];
      let topMostY = Infinity;
      for (const el of grp.els) {
        const y = el.getBoundingClientRect().top + getScrollY();
        if (y < topMostY) { topMostY = y; topMost = el; }
      }
      const pct = Math.min(100, Math.max(0, (topMostY / scrollHeight) * 100));

      const titles = grp.els.map(getTitle);
      const titleText = titles.join(" / ");

      const dot = document.createElement("button");
      dot.type = "button";
      dot.className = "scroll-spy-dot";
      if (grp.els.length > 1) dot.classList.add("scroll-spy-dot-merged");
      dot.style.top = pct + "%";
      dot.setAttribute("aria-label", titleText);
      dot.title = titleText;
      dot.addEventListener("click", (e) => {
        e.preventDefault();
        topMost.scrollIntoView({ behavior: "smooth", block: "start" });
      });
      rail.appendChild(dot);
      const pair = { els: grp.els, dot, title: titleText };
      pairs.push(pair);

      if (previousActiveEls && grp.els.some((el) => previousActiveEls.indexOf(el) !== -1)) {
        activePair = pair;
        dot.classList.add("active");
      }
    }
  }

  function pickBestPair() {
    if (!pairs.length) return null;
    // The active section is the one whose top has most recently passed the
    // focus line as the user scrolls down. This matches the dot positioning
    // (offsetTop / scrollHeight), so the active dot is the one the thumb is
    // currently sitting over.
    const vh = window.innerHeight;
    const scrollY = getScrollY();
    const focusDocY = scrollY + vh * 0.25; // 25% of viewport from top in document coords

    let bestPair = pairs[0];
    let bestTopY = -Infinity;
    for (const p of pairs) {
      // Use the topmost element of the row (matches buildRail's dot positioning).
      let topY = Infinity;
      for (const el of p.els) {
        const y = el.getBoundingClientRect().top + scrollY;
        if (y < topY) topY = y;
      }
      if (topY <= focusDocY && topY > bestTopY) {
        bestTopY = topY;
        bestPair = p;
      }
    }
    return bestPair;
  }

  function updateThumb() {
    if (!thumb || !rail) return;
    const docHeight = getDocScrollHeight();
    const vh = getViewportHeight();
    if (docHeight <= vh) {
      thumb.style.display = "none";
      return;
    }
    thumb.style.display = "";
    const railRect = rail.getBoundingClientRect();
    const railH = railRect.height;
    const ratio = vh / docHeight;
    const thumbH = Math.max(28, ratio * railH);
    const max = Math.max(0, docHeight - vh);
    const scrollY = getScrollY();
    const progress = max > 0 ? scrollY / max : 0;
    const thumbTop = progress * (railH - thumbH);
    thumb.style.height = thumbH + "px";
    thumb.style.top = thumbTop + "px";
  }

  function update() {
    if (!tip) return;
    updateThumb();
    const best = pickBestPair();
    if (!best) return;
    if (best !== activePair) {
      if (activePair && activePair.dot) activePair.dot.classList.remove("active");
      activePair = best;
      activePair.dot.classList.add("active");
    }
    const title = activePair.title;
    if (textEl.textContent !== title) textEl.textContent = title;

    // Anchor the tooltip vertically to the thumb's center so the bubble follows
    // the user's actual scroll position rather than jumping between fixed dots.
    if (thumb && thumb.style.display !== "none") {
      const tr = thumb.getBoundingClientRect();
      const center = tr.top + tr.height / 2;
      tip.style.top = Math.max(60, Math.min(window.innerHeight - 60, center)) + "px";
    } else {
      const dotRect = activePair.dot.getBoundingClientRect();
      const dotCenter = dotRect.top + dotRect.height / 2;
      tip.style.top = Math.max(60, Math.min(window.innerHeight - 60, dotCenter)) + "px";
    }
  }

  function onAnyScroll() {
    update();
    show();
  }

  function scheduleRebuild() {
    if (rebuildPending) return;
    rebuildPending = true;
    requestAnimationFrame(() => {
      rebuildPending = false;
      // Only rebuild the rail when the structure changes; otherwise just update
      // the active dot. Saves churn during things like in-page search filtering.
      const count = document.querySelectorAll(TARGET_SELECTOR).length;
      if (count !== lastTargetCount) {
        lastTargetCount = count;
        buildRail();
      }
      update();
    });
  }

  function attachRailDrag() {
    if (!rail) return;
    let dragging = false;
    let grabOffset = 0; // distance between pointer Y and the top of the thumb at pointerdown
    let savedSnap = "";
    let savedBehavior = "";

    function maxScroll() {
      return Math.max(0, getDocScrollHeight() - getViewportHeight());
    }

    function setScroll(y) {
      // Bypass smooth/snap scroll on the actual scroll container.
      setScrollY(y);
    }

    function applyDragScroll(clientY) {
      const railRect = rail.getBoundingClientRect();
      const thumbH = thumb ? thumb.getBoundingClientRect().height || 28 : 28;
      const usable = Math.max(1, railRect.height - thumbH);
      const newThumbTop = clientY - grabOffset - railRect.top;
      const ratio = Math.max(0, Math.min(1, newThumbTop / usable));
      setScroll(ratio * maxScroll());
    }

    function startDrag(clientY) {
      dragging = true;
      rail.classList.add("scroll-spy-rail-dragging");

      // Disable smooth scroll + scroll-snap on the actual scroll container for
      // the duration of the drag — both conflict with rapid programmatic scrolls.
      const sc = getScroller();
      savedSnap = sc.style.scrollSnapType;
      savedBehavior = sc.style.scrollBehavior;
      sc.style.scrollSnapType = "none";
      sc.style.scrollBehavior = "auto";

      // Decide the grab offset. If the pointer is inside the thumb, lock to
      // that offset so the thumb stays under the cursor (real scrollbar feel).
      // Otherwise center the thumb on the click so track-clicks teleport it.
      if (thumb && rail.contains(thumb)) {
        const tr = thumb.getBoundingClientRect();
        if (clientY >= tr.top && clientY <= tr.bottom) {
          grabOffset = clientY - tr.top;
        } else {
          grabOffset = tr.height / 2;
        }
      } else {
        grabOffset = 0;
      }
      applyDragScroll(clientY);
    }

    function endDrag() {
      if (!dragging) return;
      dragging = false;
      rail.classList.remove("scroll-spy-rail-dragging");
      const sc = getScroller();
      sc.style.scrollSnapType = savedSnap;
      sc.style.scrollBehavior = savedBehavior;
    }

    // Listen for the start on the rail itself.
    rail.addEventListener("mousedown", (e) => {
      if (e.button !== 0) return;
      if (e.target && e.target.classList && e.target.classList.contains("scroll-spy-dot")) return;
      e.preventDefault();
      startDrag(e.clientY);
    });
    rail.addEventListener("touchstart", (e) => {
      if (!e.touches || e.touches.length !== 1) return;
      const t = e.touches[0];
      if (e.target && e.target.classList && e.target.classList.contains("scroll-spy-dot")) return;
      e.preventDefault();
      startDrag(t.clientY);
    }, { passive: false });

    // While dragging, listen on the document so events fire even if the cursor
    // leaves the rail. This is more reliable than pointer capture alone.
    document.addEventListener("mousemove", (e) => {
      if (!dragging) return;
      e.preventDefault();
      applyDragScroll(e.clientY);
    });
    document.addEventListener("touchmove", (e) => {
      if (!dragging || !e.touches || e.touches.length !== 1) return;
      e.preventDefault();
      applyDragScroll(e.touches[0].clientY);
    }, { passive: false });
    document.addEventListener("mouseup", endDrag);
    document.addEventListener("touchend", endDrag);
    document.addEventListener("touchcancel", endDrag);
    window.addEventListener("blur", endDrag);
  }

  function init() {
    if (document.getElementById("scroll-card-indicator-tip")) return;

    // Stamp the document so CSS can hide the native scrollbar and reclaim its gutter.
    document.documentElement.classList.add("has-scroll-spy");

    rail = document.createElement("div");
    rail.id = "scroll-spy-rail";
    rail.className = "scroll-spy-rail";
    document.body.appendChild(rail);

    // Sliding thumb behind the dots that mirrors the native scrollbar position.
    thumb = document.createElement("div");
    thumb.className = "scroll-spy-thumb";
    rail.appendChild(thumb);

    tip = document.createElement("div");
    tip.id = "scroll-card-indicator-tip";
    tip.className = "scroll-card-indicator";
    tip.setAttribute("aria-hidden", "true");
    tip.innerHTML =
      '<span class="scroll-card-indicator-dot"></span>' +
      '<span class="scroll-card-indicator-text">Top</span>';
    document.body.appendChild(tip);
    textEl = tip.querySelector(".scroll-card-indicator-text");

    lastTargetCount = document.querySelectorAll(TARGET_SELECTOR).length;
    buildRail();
    attachRailDrag();
    update();
    show();

    // Capture phase + multiple targets so we catch scroll regardless of which
    // element is the actual scroll container (overflow-x: hidden on html, body
    // can demote the other axis to `auto` and shift scroll off window).
    const opts = { passive: true, capture: true };
    window.addEventListener("scroll", onAnyScroll, opts);
    document.addEventListener("scroll", onAnyScroll, opts);
    window.addEventListener("wheel", onAnyScroll, opts);
    window.addEventListener("touchmove", onAnyScroll, opts);
    window.addEventListener("resize", () => {
      // Force a full rebuild on resize so dot positions reflect the new layout.
      lastTargetCount = -1;
      scheduleRebuild();
    }, { passive: true });

    if (window.MutationObserver) {
      const main = document.querySelector("main.main") || document.body;
      const mo = new MutationObserver(scheduleRebuild);
      mo.observe(main, { childList: true, subtree: true });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
