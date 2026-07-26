/*
 * Shared in-page dialogs — drop-in replacements for the native window.alert /
 * window.confirm / window.prompt, styled to the site's dark theme.
 *
 * Include once per page: <script src="ui-dialogs.js"></script>
 *
 * API (all return Promises — native confirm/prompt were synchronous, so call
 * sites must `await`):
 *   await uiAlert(message, {title, okText})                 -> void
 *   await uiConfirm(message, {title, okText, cancelText, danger}) -> boolean
 *   await uiPrompt(message, {title, defaultValue, placeholder, okText, cancelText}) -> string | null
 *
 * Enter confirms, Escape cancels. Focus is trapped to the dialog and restored
 * on close. Self-contained: injects its own CSS + DOM, no dependencies.
 */
(function () {
  if (window.__uiDialogsLoaded) return;
  window.__uiDialogsLoaded = true;

  var CSS =
    '.uidlg-overlay{position:fixed;inset:0;z-index:100000;display:flex;align-items:center;' +
    'justify-content:center;padding:1.5rem;background:rgba(3,7,18,.72);' +
    '-webkit-backdrop-filter:blur(4px);backdrop-filter:blur(4px);' +
    'font-family:"Space Grotesk",-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;' +
    'animation:uidlgFade .12s ease-out}' +
    '@keyframes uidlgFade{from{opacity:0}to{opacity:1}}' +
    '.uidlg{width:min(440px,100%);background:#1e293b;color:#fff;border:1px solid rgba(148,163,184,.22);' +
    'border-radius:14px;box-shadow:0 25px 60px -12px rgba(0,0,0,.75);overflow:hidden;' +
    'animation:uidlgPop .14s ease-out}' +
    '@keyframes uidlgPop{from{transform:translateY(8px) scale(.98);opacity:.4}to{transform:none;opacity:1}}' +
    '.uidlg-h{padding:1.05rem 1.25rem .3rem;font-size:1.05rem;font-weight:700}' +
    '.uidlg-b{padding:.5rem 1.25rem 1.1rem;font-size:.9rem;line-height:1.5;color:#cbd5e1;white-space:pre-wrap}' +
    '.uidlg-input{width:100%;margin-top:.85rem;padding:.6rem .8rem;background:rgba(2,6,23,.5);' +
    'border:1px solid rgba(148,163,184,.22);border-radius:8px;color:#fff;font:inherit;font-size:.9rem}' +
    '.uidlg-input:focus{outline:none;border-color:#f97316;box-shadow:0 0 0 3px rgba(249,115,22,.12)}' +
    '.uidlg-f{display:flex;justify-content:flex-end;gap:.6rem;padding:.9rem 1.25rem 1.15rem}' +
    '.uidlg-btn{padding:.55rem 1.1rem;border:none;border-radius:8px;font:inherit;font-weight:700;' +
    'font-size:.85rem;cursor:pointer;transition:filter .15s,background .15s}' +
    '.uidlg-btn:hover{filter:brightness(1.08)}' +
    '.uidlg-btn.cancel{background:rgba(255,255,255,.08);color:#e2e8f0;border:1px solid rgba(148,163,184,.22)}' +
    '.uidlg-btn.ok{background:#f97316;color:#fff}' +
    '.uidlg-btn.ok.danger{background:#ef4444}';

  function injectCSS() {
    if (document.getElementById('uidlg-style')) return;
    var s = document.createElement('style');
    s.id = 'uidlg-style';
    s.textContent = CSS;
    (document.head || document.documentElement).appendChild(s);
  }

  // kind: 'alert' | 'confirm' | 'prompt'
  function open(kind, message, opts) {
    opts = opts || {};
    injectCSS();
    return new Promise(function (resolve) {
      var prevFocus = document.activeElement;
      var overlay = document.createElement('div');
      overlay.className = 'uidlg-overlay';

      var dlg = document.createElement('div');
      dlg.className = 'uidlg';
      dlg.setAttribute('role', 'dialog');
      dlg.setAttribute('aria-modal', 'true');

      var title = opts.title || (kind === 'alert' ? 'Notice' : kind === 'prompt' ? 'Input' : 'Confirm');
      var okText = opts.okText || (kind === 'confirm' ? 'Confirm' : 'OK');
      var cancelText = opts.cancelText || 'Cancel';

      var html =
        '<div class="uidlg-h"></div>' +
        '<div class="uidlg-b"></div>' +
        (kind === 'prompt' ? '<div style="padding:0 1.25rem"><input class="uidlg-input" type="text"></div>' : '') +
        '<div class="uidlg-f">' +
        (kind === 'alert' ? '' : '<button class="uidlg-btn cancel"></button>') +
        '<button class="uidlg-btn ok' + (opts.danger ? ' danger' : '') + '"></button>' +
        '</div>';
      dlg.innerHTML = html;
      // Text via textContent (no HTML injection from messages).
      dlg.querySelector('.uidlg-h').textContent = title;
      dlg.querySelector('.uidlg-b').textContent = message == null ? '' : String(message);
      dlg.querySelector('.uidlg-btn.ok').textContent = okText;
      var cancelBtn = dlg.querySelector('.uidlg-btn.cancel');
      if (cancelBtn) cancelBtn.textContent = cancelText;
      var input = dlg.querySelector('.uidlg-input');
      if (input) {
        input.value = opts.defaultValue != null ? String(opts.defaultValue) : '';
        if (opts.placeholder) input.placeholder = opts.placeholder;
      }

      overlay.appendChild(dlg);
      document.body.appendChild(overlay);

      function cleanup() {
        document.removeEventListener('keydown', onKey, true);
        if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
        try { if (prevFocus && prevFocus.focus) prevFocus.focus(); } catch (e) {}
      }
      function done(val) { cleanup(); resolve(val); }
      function onOk() { done(kind === 'prompt' ? (input ? input.value : '') : true); }
      function onCancel() { done(kind === 'prompt' ? null : false); }

      dlg.querySelector('.uidlg-btn.ok').addEventListener('click', onOk);
      if (cancelBtn) cancelBtn.addEventListener('click', onCancel);
      // Click on the dim backdrop cancels (alerts resolve/close).
      overlay.addEventListener('mousedown', function (e) { if (e.target === overlay) onCancel(); });

      function onKey(e) {
        if (e.key === 'Escape') { e.preventDefault(); onCancel(); }
        else if (e.key === 'Enter' && (kind !== 'prompt' || document.activeElement === input)) { e.preventDefault(); onOk(); }
      }
      document.addEventListener('keydown', onKey, true);

      setTimeout(function () { (input || dlg.querySelector('.uidlg-btn.ok')).focus(); }, 20);
    });
  }

  window.uiAlert = function (message, opts) { return open('alert', message, opts); };
  window.uiConfirm = function (message, opts) { return open('confirm', message, opts); };
  window.uiPrompt = function (message, opts) { return open('prompt', message, opts); };
})();
