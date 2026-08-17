/*
 * Idle guard — stop polling when nobody is watching.
 *
 * A tab left open overnight kept sending a heartbeat every 60s and refreshing
 * every layer on its own timer, forever. Multiply that by a handful of forgotten
 * tabs and the backend is serving a steady stream of requests for nobody.
 *
 * After IDLE_MS with no genuine user input, this fires 'idle': registered
 * timers stop and the page tells the backend the viewer has gone (heartbeat
 * action=close), which also lets the collector drop to its idle cadence. The
 * next real interaction fires 'active', which re-registers and refreshes
 * immediately so the user never sees stale data.
 *
 * Include once per page, before the page's own script:
 *   <script src="idle-guard.js"></script>
 *
 * API:
 *   idleGuard.isIdle()                  -> boolean
 *   idleGuard.interval(fn, ms, opts)    -> handle   (setInterval that pauses)
 *   idleGuard.onChange(fn)              -> fn(isIdle)
 *   idleGuard.clear(handle)
 *
 * `interval` is the whole point: pass it a poll and it stops while idle and
 * runs once immediately on wake (unless opts.runOnResume === false).
 */
(function () {
  if (window.idleGuard) return;

  var IDLE_MS = 60 * 60 * 1000;   // 1 hour
  var CHECK_MS = 60 * 1000;       // how often we test for the deadline

  var lastActivity = Date.now();
  var idle = false;
  var timers = [];               // { fn, ms, id, runOnResume }
  var listeners = [];
  var pausedAt = null;           // when updates stopped, for the banner

  // ---- kiosk opt-out -------------------------------------------------------
  // Wall displays and ops monitors are supposed to run unattended, which is
  // exactly what this feature breaks. ?kiosk=1 disables the timeout for the
  // browser and is remembered, so the flag only has to be used once; ?kiosk=0
  // clears it again.
  var KIOSK_KEY = 'nswpsn_kiosk';
  var kiosk = false;
  try {
    var q = new URLSearchParams(window.location.search).get('kiosk');
    if (q === '1' || q === 'true') localStorage.setItem(KIOSK_KEY, '1');
    else if (q === '0' || q === 'false') localStorage.removeItem(KIOSK_KEY);
    kiosk = localStorage.getItem(KIOSK_KEY) === '1';
  } catch (e) { /* private mode — just behave normally */ }

  function now() { return Date.now(); }

  function startTimer(t) {
    if (t.id !== null) return;
    t.id = window.setInterval(t.fn, t.ms);
  }
  function stopTimer(t) {
    if (t.id === null) return;
    window.clearInterval(t.id);
    t.id = null;
  }

  function goIdle() {
    if (idle || kiosk) return;
    idle = true;
    pausedAt = now();
    for (var i = 0; i < timers.length; i++) stopTimer(timers[i]);
    showBanner();
    notify();
  }

  function goActive(fromInput) {
    lastActivity = now();
    if (!idle) return;
    idle = false;
    pausedAt = null;
    hideBanner();
    for (var i = 0; i < timers.length; i++) {
      startTimer(timers[i]);
      // Fire once straight away so the page isn't showing hour-old data
      // while it waits out the first interval.
      if (timers[i].runOnResume) {
        try { timers[i].fn(); } catch (e) {}
      }
    }
    notify();
  }

  // ---- paused banner -------------------------------------------------------
  // This is a public-safety map. A frozen page looks identical to a live one,
  // so someone glancing at a forgotten tab could read hour-old incidents as
  // current. The banner is the whole reason pausing is safe to do at all.
  //
  // pointer-events:none so it never blocks a map control — a click lands on
  // the page underneath, which is itself the gesture that resumes.
  var bannerEl = null;
  function showBanner() {
    if (bannerEl) return;
    var when = new Date(pausedAt || now())
      .toLocaleTimeString('en-AU', { hour: '2-digit', minute: '2-digit' });
    var el = document.createElement('div');
    el.setAttribute('role', 'status');
    el.setAttribute('aria-live', 'polite');
    el.style.cssText = [
      'position:fixed', 'top:12px', 'left:50%', 'transform:translateX(-50%)',
      'z-index:2147483000', 'pointer-events:none',
      'background:#7c2d12', 'color:#fff',
      'border:1px solid #ea580c', 'border-radius:10px',
      'padding:0.55rem 0.95rem', 'box-shadow:0 10px 30px -8px rgba(0,0,0,0.7)',
      'font:600 0.82rem/1.35 system-ui,-apple-system,Segoe UI,Roboto,sans-serif',
      'text-align:center', 'max-width:min(92vw,420px)',
    ].join(';');
    el.innerHTML =
      '<div>&#9208; Live updates paused</div>' +
      '<div style="font-weight:400;opacity:0.85;margin-top:2px;">' +
      'Stopped at ' + when + ' after an hour with no activity.<br>' +
      'Click anywhere to resume.</div>';
    (document.body || document.documentElement).appendChild(el);
    bannerEl = el;
  }
  function hideBanner() {
    if (!bannerEl) return;
    if (bannerEl.parentNode) bannerEl.parentNode.removeChild(bannerEl);
    bannerEl = null;
  }

  function notify() {
    for (var i = 0; i < listeners.length; i++) {
      try { listeners[i](idle); } catch (e) {}
    }
  }

  // Real input only. Deliberately NOT listening to mousemove: a nudged desk,
  // a hovering cursor or an OS animation would reset the clock forever and the
  // timeout would never fire, which is the failure mode this exists to avoid.
  var EVENTS = ['pointerdown', 'keydown', 'wheel', 'touchstart'];
  for (var e = 0; e < EVENTS.length; e++) {
    window.addEventListener(EVENTS[e], function () { goActive(true); }, { passive: true, capture: true });
  }
  // Scroll is genuine intent but fires in bursts, so it's throttled.
  var lastScroll = 0;
  window.addEventListener('scroll', function () {
    var t = now();
    if (t - lastScroll < 1000) return;
    lastScroll = t;
    goActive(true);
  }, { passive: true, capture: true });

  // Coming back to the tab counts as intent. Going away does NOT mark activity
  // — a backgrounded tab should be allowed to time out.
  document.addEventListener('visibilitychange', function () {
    if (!document.hidden) goActive(true);
  });

  // The deadline check is itself an interval, but at one tick a minute it is
  // not what this is trying to prevent.
  window.setInterval(function () {
    if (!idle && now() - lastActivity >= IDLE_MS) goIdle();
  }, CHECK_MS);

  window.idleGuard = {
    isIdle: function () { return idle; },
    isKiosk: function () { return kiosk; },
    idleMs: IDLE_MS,
    /** Milliseconds since the last real interaction. */
    idleFor: function () { return now() - lastActivity; },
    interval: function (fn, ms, opts) {
      var t = {
        fn: fn,
        ms: ms,
        id: null,
        runOnResume: !(opts && opts.runOnResume === false),
      };
      timers.push(t);
      if (!idle) startTimer(t);
      return t;
    },
    clear: function (t) {
      if (!t) return;
      stopTimer(t);
      var i = timers.indexOf(t);
      if (i >= 0) timers.splice(i, 1);
    },
    onChange: function (fn) {
      if (typeof fn === 'function') listeners.push(fn);
    },
    /** Test hook: force the transition without waiting an hour. */
    _forceIdle: goIdle,
    _forceActive: goActive,
  };
})();
