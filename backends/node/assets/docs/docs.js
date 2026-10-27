// The API documentation page (served by src/api/v1/docs.ts).
//
// Asks for the visitor's API key, fetches /api/v1/openapi.json with it — the
// server answers with only the operations that key's scopes cover — and hands
// that document to the Scalar viewer, with the key preset so "Try it" works.
//
// The key lives in sessionStorage: this tab only, gone when it closes. It is
// sent to this origin and nowhere else; the page's Content-Security-Policy
// would refuse any other host anyway. Scalar is loaded only once a key has
// opened the document, so a visitor without one never downloads it.
(function () {
  'use strict';

  var STORE = 'ausaware.apiKey';
  var SCALAR_SRC = document.currentScript && document.currentScript.dataset.scalar;

  var gate = document.getElementById('gate');
  var form = document.getElementById('form');
  var input = document.getElementById('key');
  var openBtn = document.getElementById('open');
  var msg = document.getElementById('msg');
  var who = document.getElementById('who');
  var forget = document.getElementById('forget');

  function stored() {
    try { return sessionStorage.getItem(STORE) || ''; } catch (e) { return ''; }
  }
  function store(key) {
    try {
      if (key) sessionStorage.setItem(STORE, key);
      else sessionStorage.removeItem(STORE);
    } catch (e) { /* private mode: the key just isn't remembered */ }
  }

  function showError(text) {
    msg.textContent = text;
    msg.hidden = false;
  }

  function busy(on) {
    openBtn.disabled = on;
    openBtn.textContent = on ? 'Opening…' : 'Open the docs';
  }

  // Why the key could not open the document, in words.
  function refusal(status, body) {
    var e = body && body.error;
    var code = e && typeof e === 'object' ? e.code : '';
    if (status === 401 || code === 'invalid_api_key') return 'That key was not recognised. Check it was copied whole.';
    if (status === 429) return 'This key is over its per-minute limit. Wait a minute and try again.';
    if (status === 403 && e && e.message) return e.message;
    if (status >= 500) return 'The API could not check the key just now. Try again shortly.';
    return 'The key could not open the documentation (HTTP ' + status + ').';
  }

  function loadScalar() {
    return new Promise(function (resolve, reject) {
      if (window.Scalar) return resolve(window.Scalar);
      var s = document.createElement('script');
      s.src = SCALAR_SRC;
      s.onload = function () { window.Scalar ? resolve(window.Scalar) : reject(new Error('viewer did not load')); };
      s.onerror = function () { reject(new Error('viewer did not load')); };
      document.head.appendChild(s);
    });
  }

  function render(spec, key) {
    return loadScalar().then(function (Scalar) {
      gate.hidden = true;
      who.textContent = 'Key ' + key.slice(0, 13) + '…';
      who.hidden = false;
      forget.hidden = false;
      Scalar.createApiReference('#app', {
        content: spec,
        // "Try it" calls this API directly. Scalar's default is to route
        // requests through its own hosted proxy, which would hand the key to
        // a third party.
        servers: [{ url: location.origin }],
        proxyUrl: '',
        authentication: {
          preferredSecurityScheme: 'ApiKey',
          securitySchemes: { ApiKey: { value: key } },
        },
        persistAuth: false,
        // Nothing leaves for scalar.com: no telemetry, no web fonts, no AI
        // agent, no MCP generator, no developer toolbar (its Share and Deploy
        // publish the document to Scalar's service). The "Generate SDK"
        // button, which opens Scalar's dashboard, is hidden in index.html.
        telemetry: false,
        withDefaultFonts: false,
        agent: { disabled: true },
        mcp: { disabled: true },
        showDeveloperTools: 'never',
        darkMode: window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches,
        metaData: { title: 'AusAware API' },
      });
    });
  }

  function open(key) {
    msg.hidden = true;
    busy(true);
    return fetch('/api/v1/openapi.json', { headers: { 'X-API-Key': key }, cache: 'no-store' })
      .then(function (res) {
        return res.json().catch(function () { return null; }).then(function (body) {
          if (!res.ok) throw new Error(refusal(res.status, body));
          store(key);
          return render(body, key);
        });
      })
      .catch(function (err) {
        store('');
        gate.hidden = false;
        showError(err && err.message && !/^(Failed to fetch|NetworkError|Load failed)/.test(err.message)
          ? err.message
          : 'Could not reach the API. Check your connection and try again.');
      })
      .then(function () { busy(false); });
  }

  form.addEventListener('submit', function (ev) {
    ev.preventDefault();
    var key = input.value.trim();
    if (key) open(key);
  });

  forget.addEventListener('click', function () {
    store('');
    location.reload();
  });

  var remembered = stored();
  if (remembered) open(remembered);
})();
