// ======== UMAMI EVENT TRACKING ========
// Shared analytics helpers for NSW PSN site
// Requires: umami script loaded via <script defer> in <head>

(function () {
  // Prevent double-execution (e.g. from Cloudflare Rocket Loader)
  if (window.__analyticsLoaded) return;
  window.__analyticsLoaded = true;

  function track(name, data) {
    if (typeof umami !== 'undefined') {
      umami.track(name, data);
    }
  }

  // --- Sidebar navigation tracking (event delegation) ---
  document.addEventListener('click', function (e) {
    // Sidebar nav links
    const navLink = e.target.closest('.sidebar-nav a');
    if (navLink) {
      track('sidebar-nav', { page: navLink.textContent.trim() });
      return;
    }

    // Sidebar external links (Radio Feed, Pager Feed, Discord, etc.)
    const sidebarExternal = e.target.closest('.sidebar .listen-btn-wide, .sidebar .discord-btn-wide');
    if (sidebarExternal) {
      track('sidebar-external', { label: sidebarExternal.textContent.trim() });
      return;
    }

    // Mobile menu toggle
    if (e.target.closest('.mobile-menu-btn')) {
      track('mobile-menu-toggle');
      return;
    }
  });

  // --- Outbound link tracking (all external links) ---
  document.addEventListener('click', function (e) {
    var link = e.target.closest('a[target="_blank"]');
    if (!link) return;
    // Skip sidebar externals (already tracked above)
    if (link.closest('.sidebar')) return;
    var href = link.getAttribute('href') || '';
    var label = link.textContent.trim().substring(0, 50);
    track('outbound-link', { url: href, label: label });
  });

  // Expose helper globally for page-specific tracking
  window.umamiTrack = track;
})();
