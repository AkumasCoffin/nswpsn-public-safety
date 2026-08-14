// ======== COMMON AUTH SYSTEM ========
// Include this script on any page that needs authentication
// Requires: Supabase client script loaded before this

// Config fallbacks — config.js may or may not be loaded before this script
if (typeof SUPABASE_URL === 'undefined') var SUPABASE_URL = 'https://wwcickcmezfrcqyclcuo.supabase.co';
if (typeof SUPABASE_KEY === 'undefined') var SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind3Y2lja2NtZXpmcmNxeWNsY3VvIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjQ0ODI3NDIsImV4cCI6MjA4MDA1ODc0Mn0.xP9I9vAbBB-1afCpnOAwLJeoKTF2Dmewwv-aCKVXKrQ';
if (typeof API_BASE_URL === 'undefined') var API_BASE_URL = 'https://api.forcequit.xyz';

// Initialize Supabase client (only if not already initialized)
// Use window.sb if it exists (for pages that have their own client), otherwise create one
let sb;
if (window.sb) {
  sb = window.sb;
} else {
  sb = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);
  window.sb = sb;  // Make available globally for other scripts
}

// Inject the account/profile section at the TOP of the sidebar (under the
// logo + subtitle) and the Legal links at the bottom (before the footer).
function injectAuthSection() {
  const sidebar = document.querySelector('.sidebar');
  if (!sidebar) return;

  // Check if auth section already exists
  if (!document.getElementById('auth-section')) {
    const authSection = document.createElement('div');
    authSection.id = 'auth-section';
    authSection.style.cssText = 'margin:0.7rem 0 0.9rem; border-bottom:1px solid rgba(148,163,184,0.2); padding-bottom:0.8rem;';
    authSection.innerHTML = `
      <div class="sidebar-section-label">Account</div>
      <div id="auth-logged-out">
        <a href="login.html" style="width:100%; padding:0.6rem 1rem; background:rgba(249,115,22,0.15); border:1px solid rgba(249,115,22,0.3); border-radius:8px; color:#f97316; font-size:0.85rem; font-weight:500; cursor:pointer; display:flex; align-items:center; justify-content:center; gap:0.5rem; font-family:inherit; text-decoration:none; box-sizing:border-box;">
          <i class="fas fa-sign-in-alt"></i> Login
        </a>
      </div>
      <div id="auth-logged-in" style="display:none;">
        <div style="display:flex; align-items:center; gap:0.5rem;">
          <div id="auth-avatar" style="width:26px; height:26px; border-radius:50%; background:rgba(249,115,22,0.2); display:flex; align-items:center; justify-content:center; color:#f97316; font-weight:700; font-size:0.75rem; overflow:hidden; flex-shrink:0;"></div>
          <div id="auth-user-email" style="flex:1; min-width:0; font-size:0.82rem; color:#fff; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;"></div>
          <button onclick="toggleNotifications(event)" id="notif-btn" title="Notifications" aria-label="Notifications" style="position:relative; width:28px; height:28px; padding:0; background:rgba(148,163,184,0.1); border:1px solid rgba(148,163,184,0.2); border-radius:6px; color:#cbd5e1; font-size:0.75rem; cursor:pointer; display:flex; align-items:center; justify-content:center; font-family:inherit; flex-shrink:0;">
            <i class="fas fa-bell"></i>
            <span id="notif-badge" style="display:none; position:absolute; top:-5px; right:-5px; min-width:15px; height:15px; padding:0 3px; box-sizing:border-box; background:#ef4444; color:#fff; border-radius:999px; font-size:0.6rem; font-weight:700; line-height:15px; text-align:center;"></span>
          </button>
          <button onclick="openProfileModal()" title="Profile" aria-label="Profile" style="width:28px; height:28px; padding:0; background:rgba(148,163,184,0.1); border:1px solid rgba(148,163,184,0.2); border-radius:6px; color:#cbd5e1; font-size:0.75rem; cursor:pointer; display:flex; align-items:center; justify-content:center; font-family:inherit; flex-shrink:0;">
            <i class="fas fa-user-cog"></i>
          </button>
          <button onclick="doLogout()" title="Logout" aria-label="Logout" style="width:28px; height:28px; padding:0; background:rgba(239,68,68,0.12); border:1px solid rgba(239,68,68,0.25); border-radius:6px; color:#ef4444; font-size:0.75rem; cursor:pointer; display:flex; align-items:center; justify-content:center; font-family:inherit; flex-shrink:0;">
            <i class="fas fa-sign-out-alt"></i>
          </button>
        </div>
        <div id="auth-role-buttons" style="display:flex; gap:0.4rem; margin-top:0.5rem; flex-wrap:wrap;"></div>
      </div>
    `;
    // Insert under the subtitle (or logo), i.e. at the top of the sidebar.
    const anchor = sidebar.querySelector('.sidebar-subtitle') || sidebar.querySelector('.sidebar-logo');
    if (anchor) {
      anchor.insertAdjacentElement('afterend', authSection);
    } else {
      sidebar.insertBefore(authSection, sidebar.firstChild);
    }
  }

  // Legal links stay at the bottom, before the footer.
  const sidebarFooter = document.querySelector('.sidebar-footer');
  if (sidebarFooter && !document.getElementById('legal-section')) {
    const legal = document.createElement('div');
    legal.id = 'legal-section';
    legal.style.cssText = 'margin-top:1.5rem; border-top:1px solid rgba(148,163,184,0.2); padding-top:1rem;';
    legal.innerHTML = `
      <div class="sidebar-section-label">Legal</div>
      <nav class="sidebar-nav">
        <a href="terms.html"${location.pathname.endsWith('/terms.html') ? ' class="active"' : ''}>Terms &amp; Conditions</a>
        <a href="privacy.html"${location.pathname.endsWith('/privacy.html') ? ' class="active"' : ''}>Privacy Policy</a>
      </nav>
    `;
    sidebarFooter.parentNode.insertBefore(legal, sidebarFooter);
  }
}

// Create login and password reset modals (always runs)
function createAuthModals() {
  // Create login modal if it doesn't exist
  if (!document.getElementById('login-modal')) {
    const modal = document.createElement('div');
    modal.id = 'login-modal';
    modal.style.cssText = 'display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:#020617; z-index:10000; align-items:center; justify-content:center;';
    modal.innerHTML = `
      <div style="background:#1e293b; border:1px solid rgba(148,163,184,0.2); border-radius:12px; padding:2.5rem; max-width:360px; width:90%; box-shadow:0 25px 50px -12px rgba(0,0,0,0.7);">
        <div style="text-align:center; margin-bottom:2rem;">
          <div style="font-size:1.5rem; font-weight:700; color:#fff; text-transform:uppercase; letter-spacing:0.1em;">Forcequit <span style="color:#f97316;">Login</span></div>
          <div style="color:#94a3b8; font-size:0.9rem; margin-top:0.5rem;">NSW PSN Reference</div>
        </div>
        <button onclick="doDiscordLogin()" id="discord-modal-btn" style="width:100%; padding:0.8rem; background:#5865F2; border:none; border-radius:8px; color:#fff; font-weight:700; cursor:pointer; font-size:0.9rem; font-family:inherit; display:flex; align-items:center; justify-content:center; gap:0.5rem; transition:background 0.2s;">
          <i class="fab fa-discord"></i> Continue with Discord
        </button>
        <div style="display:flex; align-items:center; gap:0.75rem; margin:1.2rem 0; color:#94a3b8; font-size:0.75rem; text-transform:uppercase; letter-spacing:0.05em;">
          <span style="flex:1; border-top:1px solid rgba(148,163,184,0.2);"></span>
          <span>or</span>
          <span style="flex:1; border-top:1px solid rgba(148,163,184,0.2);"></span>
        </div>
        <div style="margin-bottom:1.2rem;">
          <label style="display:block; color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:0.5rem; font-weight:600;">Email Address</label>
          <input type="email" id="login-email" style="width:100%; padding:0.75rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.95rem; box-sizing:border-box; font-family:inherit;" placeholder="name@forcequit.xyz">
        </div>
        <div style="margin-bottom:1.2rem;">
          <label style="display:block; color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:0.5rem; font-weight:600;">Password</label>
          <input type="password" id="login-password" style="width:100%; padding:0.75rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.95rem; box-sizing:border-box; font-family:inherit;" placeholder="••••••••" onkeydown="if(event.key==='Enter') doLogin()">
        </div>
        <button onclick="doLogin()" id="login-submit-btn" style="width:100%; padding:0.8rem; background:#f97316; border:none; border-radius:8px; color:#fff; font-weight:700; cursor:pointer; text-transform:uppercase; letter-spacing:0.05em; font-size:0.9rem; margin-top:1rem; font-family:inherit; transition:background 0.2s;">
          Authenticate
        </button>
        <div style="text-align:center; margin-top:1rem;">
          <a href="#" onclick="event.preventDefault(); openPasswordResetModal();" style="color:#94a3b8; font-size:0.85rem; text-decoration:none;">Forgot Password?</a>
        </div>
        <div id="login-error" style="color:#ef4444; font-size:0.85rem; margin-top:1.2rem; text-align:center; min-height:1.2em;"></div>
        <div style="border-top:1px solid rgba(148,163,184,0.2); margin-top:1.5rem; padding-top:1.5rem; text-align:center;">
          <p style="color:#94a3b8; font-size:0.85rem; margin:0 0 0.75rem 0;">Don't have an account?</p>
          <a href="signup.html" style="display:block; width:100%; padding:0.8rem; background:transparent; border:1px solid #f97316; border-radius:8px; color:#f97316; font-weight:700; cursor:pointer; text-transform:uppercase; letter-spacing:0.05em; font-size:0.9rem; text-decoration:none; text-align:center; box-sizing:border-box; font-family:inherit; transition:all 0.2s;">Request Access</a>
        </div>
      </div>
    `;
    document.body.appendChild(modal);
    
    // Close modal on click outside
    modal.addEventListener('click', (e) => {
      if (e.target.id === 'login-modal') closeLoginModal();
    });
  }
  
  // Create password reset modal if it doesn't exist
  if (!document.getElementById('password-reset-modal')) {
    const resetModal = document.createElement('div');
    resetModal.id = 'password-reset-modal';
    resetModal.style.cssText = 'display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.8); z-index:10001; align-items:center; justify-content:center;';
    resetModal.innerHTML = `
      <div style="background:#1e293b; border:1px solid rgba(148,163,184,0.2); border-radius:12px; padding:2rem; max-width:420px; width:90%; box-shadow:0 25px 50px -12px rgba(0,0,0,0.7);">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1.5rem;">
          <h2 style="margin:0; font-size:1.25rem; font-weight:700; color:#fff;">Reset Password</h2>
          <button onclick="closePasswordResetModal()" style="background:none; border:none; color:#94a3b8; font-size:1.5rem; cursor:pointer; padding:0; width:30px; height:30px; display:flex; align-items:center; justify-content:center;">&times;</button>
        </div>
        <p style="color:#94a3b8; font-size:0.9rem; margin-bottom:1.5rem;">Enter your email address and we'll send you a link to reset your password.</p>
        <form id="password-reset-form" onsubmit="handlePasswordResetRequest(event); return false;">
          <div style="margin-bottom:1.2rem;">
            <label style="display:block; color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:0.5rem; font-weight:600;">Email Address</label>
            <input type="email" id="reset-email-input" required style="width:100%; padding:0.75rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.95rem; box-sizing:border-box; font-family:inherit;" placeholder="name@forcequit.xyz">
          </div>
          <button type="submit" id="reset-submit-btn" style="width:100%; padding:0.8rem; background:#f97316; color:#fff; border:none; border-radius:8px; font-weight:700; cursor:pointer; text-transform:uppercase; letter-spacing:0.05em; font-size:0.9rem; font-family:inherit; transition:background 0.2s;">Send Reset Link</button>
          <div id="reset-message" style="margin-top:1rem; font-size:0.85rem; text-align:center; min-height:1.2em;"></div>
        </form>
      </div>
    `;
    document.body.appendChild(resetModal);
    
    // Close modal on click outside
    resetModal.addEventListener('click', (e) => {
      if (e.target.id === 'password-reset-modal') closePasswordResetModal();
    });
  }
}

function openLoginModal() {
  const modal = document.getElementById('login-modal');
  if (modal) {
    modal.style.display = 'flex';
    document.getElementById('login-email')?.focus();
  }
}

function closeLoginModal() {
  const modal = document.getElementById('login-modal');
  if (modal) {
    modal.style.display = 'none';
    const errorDiv = document.getElementById('login-error');
    if (errorDiv) errorDiv.textContent = '';
  }
}

async function doLogin() {
  const email = document.getElementById('login-email')?.value;
  const password = document.getElementById('login-password')?.value;
  const errorDiv = document.getElementById('login-error');
  
  if (!email || !password) {
    if (errorDiv) errorDiv.textContent = 'Please enter email and password.';
    return;
  }
  
  if (errorDiv) errorDiv.textContent = 'Signing in...';
  
  const { data, error } = await sb.auth.signInWithPassword({ email, password });
  
  if (error) {
    if (typeof umami !== 'undefined') umami.track('login-failed', { method: 'modal' });
    if (errorDiv) errorDiv.textContent = error.message;
    return;
  }

  // Check if user needs to change password on first login
  if (data?.user?.user_metadata?.force_password_change) {
    closeLoginModal();
    window.location.href = 'change-password.html';
    return;
  }
  
  if (typeof umami !== 'undefined') umami.track('login-success', { method: 'modal' });
  closeLoginModal();
  checkAuthState();
}

// ---- Profile modal (username, linked accounts, more to come) ----
function createProfileModal() {
  if (document.getElementById('profile-modal')) return;
  const modal = document.createElement('div');
  modal.id = 'profile-modal';
  modal.style.cssText = 'display:none; position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.8); z-index:10002; align-items:center; justify-content:center;';
  modal.innerHTML = `
    <div style="background:#1e293b; border:1px solid rgba(148,163,184,0.2); border-radius:12px; padding:2rem; max-width:420px; width:90%; box-shadow:0 25px 50px -12px rgba(0,0,0,0.7); max-height:90vh; overflow-y:auto;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1.25rem;">
        <h2 style="margin:0; font-size:1.25rem; font-weight:700; color:#fff;">Your Profile</h2>
        <button onclick="closeProfileModal()" style="background:none; border:none; color:#94a3b8; font-size:1.5rem; cursor:pointer; padding:0; width:30px; height:30px; display:flex; align-items:center; justify-content:center;">&times;</button>
      </div>

      <div style="text-align:center; margin-bottom:1.3rem;">
        <div id="profile-avatar-preview" style="width:82px; height:82px; border-radius:50%; margin:0 auto 0.55rem; background:rgba(249,115,22,0.2); color:#f97316; display:grid; place-items:center; font-size:1.9rem; font-weight:700; overflow:hidden; border:2px solid rgba(148,163,184,0.25);"></div>
        <button type="button" onclick="pickProfileAvatar()" id="profile-avatar-btn" style="padding:0.45rem 0.9rem; background:rgba(148,163,184,0.12); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#e2e8f0; font-size:0.8rem; cursor:pointer; font-family:inherit;"><i class="fas fa-camera"></i> Change picture</button>
        <input type="file" id="profile-avatar-input" accept="image/jpeg,image/png,image/webp" style="display:none">
        <div style="color:#64748b; font-size:0.72rem; margin-top:0.4rem;">Overrides your Discord avatar.</div>
      </div>

      <div style="margin-bottom:1.2rem;">
        <label style="display:block; color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:0.5rem; font-weight:600;">Username</label>
        <div style="display:flex; gap:0.5rem;">
          <input type="text" id="profile-username" maxlength="32" style="flex:1; padding:0.65rem 0.75rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.9rem; box-sizing:border-box; font-family:inherit;" placeholder="Pick a username">
          <button onclick="saveProfile()" id="profile-save-btn" style="padding:0.65rem 1rem; background:#f97316; border:none; border-radius:8px; color:#fff; font-weight:700; cursor:pointer; font-size:0.85rem; font-family:inherit;">Save</button>
        </div>
        <div style="color:#64748b; font-size:0.75rem; margin-top:0.35rem;">How you'll appear around the site.</div>
      </div>

      <div style="margin-bottom:1.2rem;">
        <label style="display:block; color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:0.5rem; font-weight:600;">Bio</label>
        <textarea id="profile-bio" maxlength="500" rows="3" placeholder="A line or two about you — shown on your public profile." style="width:100%; padding:0.65rem 0.75rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.9rem; box-sizing:border-box; font-family:inherit; resize:vertical;"></textarea>
        <div style="display:flex; justify-content:space-between; gap:0.5rem; color:#64748b; font-size:0.75rem; margin-top:0.35rem;">
          <span>Shown on your contributor profile.</span>
          <span id="profile-bio-count">0/500</span>
        </div>
      </div>

      <div style="margin-bottom:1.2rem;">
        <label style="display:block; color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:0.5rem; font-weight:600;">Social links</label>
        <div style="display:flex; flex-direction:column; gap:0.45rem;">
          <div style="display:flex; align-items:center; gap:0.6rem;"><i class="fab fa-x-twitter" style="color:#94a3b8; width:18px; text-align:center;"></i><input type="text" id="profile-twitter" placeholder="X / Twitter link" style="flex:1; padding:0.55rem 0.7rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.85rem; box-sizing:border-box; font-family:inherit;"></div>
          <div style="display:flex; align-items:center; gap:0.6rem;"><i class="fab fa-facebook" style="color:#94a3b8; width:18px; text-align:center;"></i><input type="text" id="profile-facebook" placeholder="Facebook link" style="flex:1; padding:0.55rem 0.7rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.85rem; box-sizing:border-box; font-family:inherit;"></div>
          <div style="display:flex; align-items:center; gap:0.6rem;"><i class="fab fa-instagram" style="color:#94a3b8; width:18px; text-align:center;"></i><input type="text" id="profile-instagram" placeholder="Instagram link" style="flex:1; padding:0.55rem 0.7rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.85rem; box-sizing:border-box; font-family:inherit;"></div>
          <div style="display:flex; align-items:center; gap:0.6rem;"><i class="fab fa-youtube" style="color:#94a3b8; width:18px; text-align:center;"></i><input type="text" id="profile-youtube" placeholder="YouTube link" style="flex:1; padding:0.55rem 0.7rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.85rem; box-sizing:border-box; font-family:inherit;"></div>
          <div style="display:flex; align-items:center; gap:0.6rem;"><i class="fas fa-globe" style="color:#94a3b8; width:18px; text-align:center;"></i><input type="text" id="profile-website" placeholder="Website link" style="flex:1; padding:0.55rem 0.7rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.85rem; box-sizing:border-box; font-family:inherit;"></div>
        </div>
        <div style="color:#64748b; font-size:0.72rem; margin-top:0.4rem;">Shown on your contributor profile. Click Save to update.</div>
      </div>

      <div style="margin-bottom:1.2rem;">
        <label style="display:block; color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:0.5rem; font-weight:600;">Linked Accounts</label>
        <div id="profile-email-row" style="display:flex; align-items:center; gap:0.6rem; padding:0.6rem 0.75rem; background:rgba(2,6,23,0.4); border:1px solid rgba(148,163,184,0.15); border-radius:8px; margin-bottom:0.5rem;">
          <i class="fas fa-envelope" style="color:#94a3b8; width:18px; text-align:center;"></i>
          <span id="profile-email-value" style="flex:1; color:#e2e8f0; font-size:0.85rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;"></span>
        </div>
        <div id="profile-discord-row" style="display:flex; align-items:center; gap:0.6rem; padding:0.6rem 0.75rem; background:rgba(2,6,23,0.4); border:1px solid rgba(148,163,184,0.15); border-radius:8px;">
          <i class="fab fa-discord" style="color:#5865F2; width:18px; text-align:center;"></i>
          <span id="profile-discord-value" style="flex:1; color:#e2e8f0; font-size:0.85rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;"></span>
          <button id="profile-discord-link-btn" onclick="linkDiscordAccount()" style="display:none; padding:0.35rem 0.7rem; background:#5865F2; border:none; border-radius:6px; color:#fff; font-weight:600; cursor:pointer; font-size:0.75rem; font-family:inherit;">Link</button>
          <span id="profile-discord-linked-badge" style="display:none; color:#22c55e; font-size:0.75rem; font-weight:600;"><i class="fas fa-check"></i> Linked</span>
        </div>
      </div>

      <a id="profile-change-password" href="change-password.html" style="display:flex; width:100%; padding:0.55rem; background:rgba(148,163,184,0.1); border:1px solid rgba(148,163,184,0.2); border-radius:6px; color:#94a3b8; font-size:0.8rem; cursor:pointer; align-items:center; justify-content:center; gap:0.4rem; font-family:inherit; text-decoration:none; box-sizing:border-box;">
        <i class="fas fa-key"></i> Change Password
      </a>

      <div id="profile-message" style="margin-top:1rem; font-size:0.85rem; text-align:center; min-height:1.2em;"></div>
    </div>
  `;
  document.body.appendChild(modal);
  modal.addEventListener('click', (e) => {
    if (e.target.id === 'profile-modal') closeProfileModal();
  });
  const bioInput = document.getElementById('profile-bio');
  if (bioInput) bioInput.addEventListener('input', updateBioCount);
  const avInput = document.getElementById('profile-avatar-input');
  if (avInput) avInput.addEventListener('change', handleProfileAvatar);
}

// ===================== NOTIFICATIONS (sidebar bell) =====================
// Polling is paused while the tab is hidden — a background tab shouldn't keep
// hitting the API, and the count is refreshed the moment it becomes visible.
let _notifTimer = null;
let _notifPanel = null;
const NOTIF_POLL_MS = 60_000;

function escNotif(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

async function notifHeaders() {
  const { data } = await sb.auth.getSession();
  const t = data.session?.access_token;
  return t ? { Authorization: 'Bearer ' + t, 'Content-Type': 'application/json' } : null;
}

/** Unread count only — cheap enough to poll. */
async function refreshNotifBadge() {
  const badge = document.getElementById('notif-badge');
  if (!badge) return;
  try {
    const h = await notifHeaders();
    if (!h) return;
    const r = await fetch(`${API_BASE_URL}/api/notifications?limit=1`, { headers: h });
    if (!r.ok) return;
    const j = await r.json();
    const n = Number(j.unreadCount) || 0;
    if (n > 0) { badge.style.display = ''; badge.textContent = n > 99 ? '99+' : String(n); }
    else badge.style.display = 'none';
  } catch (e) { /* transient — try again next tick */ }
}

function startNotifPolling() {
  stopNotifPolling();
  if (document.hidden) return;
  _notifTimer = setInterval(refreshNotifBadge, NOTIF_POLL_MS);
}
function stopNotifPolling() {
  if (_notifTimer) { clearInterval(_notifTimer); _notifTimer = null; }
}
document.addEventListener('visibilitychange', () => {
  if (document.hidden) { stopNotifPolling(); return; }
  // Coming back: refresh immediately, then resume the interval.
  if (document.getElementById('notif-btn')) { refreshNotifBadge(); startNotifPolling(); }
});

function closeNotifPanel() {
  if (_notifPanel) { _notifPanel.remove(); _notifPanel = null; }
}

async function toggleNotifications(event) {
  if (event) { event.preventDefault(); event.stopPropagation(); }
  if (_notifPanel) { closeNotifPanel(); return; }
  const btn = document.getElementById('notif-btn');
  if (!btn) return;

  const panel = document.createElement('div');
  _notifPanel = panel;
  panel.id = 'notif-panel';
  panel.style.cssText = 'position:absolute; z-index:10001; width:min(320px, calc(100vw - 2rem)); max-height:60vh; overflow-y:auto; background:#1e293b; border:1px solid rgba(148,163,184,0.25); border-radius:10px; box-shadow:0 20px 40px -12px rgba(0,0,0,0.7); padding:0.5rem;';
  panel.innerHTML = '<div style="padding:0.7rem; color:#94a3b8; font-size:0.8rem;">Loading…</div>';
  document.body.appendChild(panel);
  // Anchor under the bell, clamped to the viewport.
  const r = btn.getBoundingClientRect();
  panel.style.top = `${Math.round(r.bottom + window.scrollY + 6)}px`;
  panel.style.left = `${Math.round(Math.max(8, Math.min(r.left + window.scrollX, window.innerWidth - panel.offsetWidth - 8)))}px`;

  try {
    const h = await notifHeaders();
    if (!h) { panel.innerHTML = '<div style="padding:0.7rem; color:#94a3b8; font-size:0.8rem;">Sign in to see notifications.</div>'; return; }
    const res = await fetch(`${API_BASE_URL}/api/notifications?limit=20`, { headers: h });
    const j = await res.json().catch(() => ({}));
    const list = j.notifications || [];
    const head = `<div style="display:flex; align-items:center; justify-content:space-between; gap:0.5rem; padding:0.45rem 0.6rem 0.6rem; border-bottom:1px solid rgba(148,163,184,0.15);">
        <span style="font-size:0.7rem; text-transform:uppercase; letter-spacing:0.1em; color:#94a3b8; font-weight:700;">Notifications</span>
        ${list.some((n) => !n.read) ? '<button id="notif-readall" style="background:none;border:0;color:#f97316;font:inherit;font-size:0.72rem;cursor:pointer;">Mark all read</button>' : ''}
      </div>`;
    if (!list.length) {
      panel.innerHTML = head + '<div style="padding:0.9rem 0.7rem; color:#64748b; font-size:0.8rem;">Nothing yet.</div>';
    } else {
      panel.innerHTML = head + list.map((n) => `
        <a href="${escNotif(n.link || '#')}" data-nid="${escNotif(n.id)}" style="display:block; padding:0.6rem; border-radius:8px; text-decoration:none; color:inherit; background:${n.read ? 'transparent' : 'rgba(249,115,22,0.09)'};">
          <div style="font-size:0.8rem; font-weight:600; color:#e2e8f0;">${escNotif(n.title)}</div>
          ${n.body ? `<div style="font-size:0.75rem; color:#94a3b8; margin-top:0.15rem;">${escNotif(n.body)}</div>` : ''}
          <div style="font-size:0.68rem; color:#64748b; margin-top:0.2rem;">${escNotif(notifAgo(n.created_at))}</div>
        </a>`).join('');
    }
    const readAll = panel.querySelector('#notif-readall');
    if (readAll) readAll.addEventListener('click', async (e) => {
      e.preventDefault(); e.stopPropagation();
      await markNotificationsRead(null);
      closeNotifPanel();
    });
    // Clicking an item marks just that one read, then follows the link.
    panel.querySelectorAll('[data-nid]').forEach((a) => a.addEventListener('click', () => {
      markNotificationsRead([Number(a.dataset.nid)]);
    }));
  } catch (e) {
    panel.innerHTML = '<div style="padding:0.7rem; color:#ef4444; font-size:0.8rem;">Could not load notifications.</div>';
  }
}

async function markNotificationsRead(ids) {
  try {
    const h = await notifHeaders();
    if (!h) return;
    await fetch(`${API_BASE_URL}/api/notifications/read`, {
      method: 'POST', headers: h, body: JSON.stringify(ids ? { ids } : {}),
    });
    refreshNotifBadge();
  } catch (e) { /* non-fatal */ }
}

function notifAgo(iso) {
  const ms = Date.now() - new Date(iso).getTime();
  if (!isFinite(ms) || ms < 0) return '';
  const m = Math.floor(ms / 60000);
  if (m < 1) return 'just now';
  if (m < 60) return m + 'm ago';
  const h = Math.floor(m / 60);
  if (h < 24) return h + 'h ago';
  return Math.floor(h / 24) + 'd ago';
}

document.addEventListener('click', (e) => {
  if (_notifPanel && !e.target.closest('#notif-panel') && !e.target.closest('#notif-btn')) closeNotifPanel();
});
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeNotifPanel(); });

/** Live "n/500" counter under the bio field. */
function updateBioCount() {
  const el = document.getElementById('profile-bio');
  const out = document.getElementById('profile-bio-count');
  if (el && out) out.textContent = `${el.value.length}/500`;
}

// Pending avatar (uploaded to R2 but not yet saved to the profile).
let _pendingAvatarKey = null, _pendingAvatarUrl = null;
function pickProfileAvatar() { document.getElementById('profile-avatar-input')?.click(); }
async function _optimizeAvatar(file) {
  try {
    const bmp = await createImageBitmap(file, { imageOrientation: 'from-image' });
    const size = 256; const canvas = document.createElement('canvas'); canvas.width = size; canvas.height = size;
    const ctx = canvas.getContext('2d');
    const scale = Math.max(size / bmp.width, size / bmp.height);
    const w = bmp.width * scale, h = bmp.height * scale;
    ctx.drawImage(bmp, (size - w) / 2, (size - h) / 2, w, h);
    const blob = await new Promise((res) => canvas.toBlob(res, 'image/webp', 0.85));
    return blob || file;
  } catch (e) { return file; }
}
async function handleProfileAvatar(e) {
  const file = e.target.files && e.target.files[0]; if (!file) return;
  const btn = document.getElementById('profile-avatar-btn');
  const msg = document.getElementById('profile-message');
  if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Uploading…'; }
  try {
    const { data } = await sb.auth.getSession();
    const jwt = data.session?.access_token;
    if (!jwt) throw new Error('not signed in');
    const blob = await _optimizeAvatar(file);
    const r = await fetch(`${API_BASE_URL}/api/profiles/avatar-url`, { method: 'POST', headers: { Authorization: 'Bearer ' + jwt } });
    if (!r.ok) throw new Error('presign ' + r.status);
    const { uploadURL, key, publicUrl } = await r.json();
    const up = await fetch(uploadURL, { method: 'PUT', body: blob });
    if (!up.ok) throw new Error('upload ' + up.status);
    _pendingAvatarKey = key; _pendingAvatarUrl = publicUrl;
    const prev = document.getElementById('profile-avatar-preview');
    if (prev) prev.innerHTML = `<img src="${publicUrl}" style="width:100%;height:100%;object-fit:cover;">`;
    // Save on upload: persist immediately so the pfp sticks without pressing
    // Save. Mirror to Supabase metadata (sidebar/avatars) + the backend profile.
    // Socials come from the (already-prefilled) modal inputs so they're preserved.
    if (msg) { msg.style.color = '#94a3b8'; msg.textContent = 'Saving picture…'; }
    try {
      await sb.auth.updateUser({ data: { custom_avatar_url: publicUrl } });
      const v = (id) => (document.getElementById(id)?.value || '').trim();
      const uname = (document.getElementById('profile-username')?.value || '').trim();
      const body = { display_name: uname || null, bio: v('profile-bio'), avatar_key: key, twitter: v('profile-twitter'), facebook: v('profile-facebook'), instagram: v('profile-instagram'), youtube: v('profile-youtube'), website: v('profile-website') };
      await fetch(`${API_BASE_URL}/api/profiles`, { method: 'PUT', headers: { Authorization: 'Bearer ' + jwt, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      _pendingAvatarKey = null; _pendingAvatarUrl = null;
      if (typeof checkAuthState === 'function') checkAuthState();
      if (msg) { msg.style.color = '#22c55e'; msg.textContent = 'Picture saved.'; }
    } catch (e) {
      // Upload succeeded but the save didn't — leave it pending so Save applies it.
      if (msg) { msg.style.color = '#94a3b8'; msg.textContent = 'Picture ready — click Save to apply.'; }
    }
  } catch (err) {
    if (msg) { msg.style.color = '#ef4444'; msg.textContent = 'Avatar upload failed (' + err.message + ').'; }
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fas fa-camera"></i> Change picture'; }
  }
}

async function openProfileModal() {
  createProfileModal();
  const { data } = await sb.auth.getSession();
  const session = data.session;
  if (!session) return;
  const user = session.user || {};
  const meta = user.user_metadata || {};
  const identities = user.identities || [];

  document.getElementById('profile-username').value = meta.display_name || '';
  document.getElementById('profile-email-value').textContent = user.email || 'No email on account';

  const discordIdentity = identities.find((i) => i.provider === 'discord');
  const discordValue = document.getElementById('profile-discord-value');
  const linkBtn = document.getElementById('profile-discord-link-btn');
  const linkedBadge = document.getElementById('profile-discord-linked-badge');
  if (discordIdentity) {
    const idData = discordIdentity.identity_data || {};
    discordValue.textContent = idData.full_name || idData.name || idData.user_name || 'Discord';
    linkBtn.style.display = 'none';
    linkedBadge.style.display = 'inline';
  } else {
    discordValue.textContent = 'Not linked';
    linkBtn.style.display = 'inline-block';
    linkedBadge.style.display = 'none';
  }

  // Password management only makes sense for accounts with an email identity.
  const hasEmailIdentity = identities.some((i) => i.provider === 'email');
  document.getElementById('profile-change-password').style.display = hasEmailIdentity ? 'flex' : 'none';

  // Avatar + social links: preview the current avatar, then prefill from the
  // backend profile (socials + custom avatar).
  _pendingAvatarKey = null; _pendingAvatarUrl = null;
  const avPrev = document.getElementById('profile-avatar-preview');
  const fallbackAv = meta.custom_avatar_url || meta.avatar_url;
  if (avPrev) avPrev.innerHTML = fallbackAv ? `<img src="${fallbackAv}" style="width:100%;height:100%;object-fit:cover;">` : (meta.display_name || user.email || '?').charAt(0).toUpperCase();
  try {
    const pr = await fetch(`${API_BASE_URL}/api/profiles/${user.id}`, { headers: { Authorization: 'Bearer ' + session.access_token } });
    if (pr.ok) {
      const { profile } = await pr.json();
      ['twitter', 'facebook', 'instagram', 'youtube', 'website'].forEach((k) => { const el = document.getElementById('profile-' + k); if (el) el.value = (profile && profile[k]) || ''; });
      const bioEl = document.getElementById('profile-bio');
      if (bioEl) { bioEl.value = (profile && profile.bio) || ''; updateBioCount(); }
      if (profile && profile.avatar_url && avPrev) avPrev.innerHTML = `<img src="${profile.avatar_url}" style="width:100%;height:100%;object-fit:cover;">`;
    }
  } catch (e) { /* ignore */ }

  const msg = document.getElementById('profile-message');
  if (msg) msg.textContent = '';
  document.getElementById('profile-modal').style.display = 'flex';
}

function closeProfileModal() {
  const modal = document.getElementById('profile-modal');
  if (modal) modal.style.display = 'none';
}

// Publish the user's display name (+ Discord avatar, if any) to their public
// profile, at most once per browser session. Safe partial write — the backend
// only fills discord_avatar_url and (when empty) display_name. Runs for EVERY
// logged-in user so email-signup users (no Discord avatar) are still captured
// and become searchable as co-authors.
let _avatarSynced = false;
async function syncProfileAvatarOnce(session) {
  if (_avatarSynced) return;
  const uid = session.user?.id;
  if (!uid) return;
  const av = session.user?.user_metadata?.avatar_url || '';
  const flag = 'pfpsync:' + uid + ':' + av;
  try { if (sessionStorage.getItem(flag)) { _avatarSynced = true; return; } } catch (e) { /* ignore */ }
  _avatarSynced = true;
  try {
    await fetch(`${API_BASE_URL}/api/profiles/sync`, { method: 'POST', headers: { Authorization: 'Bearer ' + session.access_token } });
    try { sessionStorage.setItem(flag, '1'); } catch (e) { /* ignore */ }
  } catch (e) { _avatarSynced = false; }
}

async function saveProfile() {
  const input = document.getElementById('profile-username');
  const btn = document.getElementById('profile-save-btn');
  const msg = document.getElementById('profile-message');
  const username = (input.value || '').trim();
  if (username.length > 0 && username.length < 2) {
    msg.style.color = '#ef4444';
    msg.textContent = 'Username must be at least 2 characters.';
    return;
  }
  btn.disabled = true;
  btn.textContent = 'Saving…';
  try {
    const { data: sess } = await sb.auth.getSession();
    const jwt = sess.session?.access_token;
    // 1) Supabase metadata: display name, and mirror the custom avatar so the
    //    sidebar/avatars can use it without a backend round-trip.
    const metaData = { display_name: username || null };
    if (_pendingAvatarUrl) metaData.custom_avatar_url = _pendingAvatarUrl;
    const { error } = await sb.auth.updateUser({ data: metaData });
    if (error) throw new Error(error.message);
    // 2) Backend profile: social links + custom avatar.
    if (jwt) {
      const v = (id) => (document.getElementById(id)?.value || '').trim();
      const body = { display_name: username || null, bio: v('profile-bio'), twitter: v('profile-twitter'), facebook: v('profile-facebook'), instagram: v('profile-instagram'), youtube: v('profile-youtube'), website: v('profile-website') };
      if (_pendingAvatarKey) body.avatar_key = _pendingAvatarKey;
      await fetch(`${API_BASE_URL}/api/profiles`, { method: 'PUT', headers: { Authorization: 'Bearer ' + jwt, 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).catch(() => {});
    }
    _pendingAvatarKey = null; _pendingAvatarUrl = null;
    if (typeof umami !== 'undefined') umami.track('profile-saved');
    msg.style.color = '#22c55e';
    msg.textContent = 'Saved!';
    checkAuthState();
  } catch (err) {
    msg.style.color = '#ef4444';
    msg.textContent = err.message || 'Failed to save.';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Save';
  }
}

async function linkDiscordAccount() {
  const msg = document.getElementById('profile-message');
  const btn = document.getElementById('profile-discord-link-btn');
  btn.disabled = true;
  btn.textContent = 'Redirecting…';
  // linkIdentity adds the Discord identity to the CURRENT user (requires
  // "manual linking" enabled in Supabase auth settings). Returns here after.
  const { error } = await sb.auth.linkIdentity({
    provider: 'discord',
    options: { redirectTo: window.location.origin + window.location.pathname + window.location.search },
  });
  if (error) {
    btn.disabled = false;
    btn.textContent = 'Link';
    msg.style.color = '#ef4444';
    msg.textContent = error.message;
  }
}

// ---- One-time username prompt for existing accounts ----
// Accounts created before usernames existed (or admin-created ones) have no
// display_name and would show as "Account" in the sidebar. Ask them to pick
// one on page load; "Later" defers for the rest of the browser session.
let usernamePromptShown = false;

function maybeAskUsername(session) {
  try {
    if (usernamePromptShown) return;
    if (sessionStorage.getItem('nswpsn_username_prompt_dismissed') === '1') return;
    const meta = session.user?.user_metadata || {};
    // A Discord-provided name counts — same rule as signup.
    if (meta.display_name || meta.full_name || meta.name || meta.user_name) return;
    usernamePromptShown = true;

    const overlay = document.createElement('div');
    overlay.id = 'username-prompt-modal';
    overlay.style.cssText = 'position:fixed; inset:0; background:rgba(2,6,23,0.7); z-index:10005; display:flex; align-items:center; justify-content:center; backdrop-filter:blur(2px);';
    overlay.innerHTML = `
      <div style="background:#1e293b; border:1px solid rgba(148,163,184,0.25); border-radius:12px; padding:1.6rem; max-width:360px; width:90%; box-shadow:0 25px 50px -12px rgba(0,0,0,0.7);">
        <div style="font-size:1.1rem; font-weight:700; color:#fff; margin-bottom:0.4rem;"><i class="fas fa-user" style="color:#f97316; margin-right:0.4rem;"></i>Choose a username</div>
        <p style="color:#94a3b8; font-size:0.85rem; margin:0 0 1rem;">Your account doesn't have a username yet — pick how you'll appear around the site.</p>
        <input type="text" id="username-prompt-input" maxlength="32" placeholder="Username" style="width:100%; padding:0.7rem 0.75rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.95rem; box-sizing:border-box; font-family:inherit;">
        <div id="username-prompt-msg" style="color:#ef4444; font-size:0.8rem; min-height:1.1em; margin-top:0.45rem;"></div>
        <div style="display:flex; gap:0.5rem; margin-top:0.7rem;">
          <button id="username-prompt-later" style="flex:1; padding:0.6rem; background:rgba(148,163,184,0.1); border:1px solid rgba(148,163,184,0.2); border-radius:8px; color:#94a3b8; font-size:0.85rem; cursor:pointer; font-family:inherit;">Later</button>
          <button id="username-prompt-save" style="flex:2; padding:0.6rem; background:#f97316; border:none; border-radius:8px; color:#fff; font-weight:700; font-size:0.85rem; cursor:pointer; font-family:inherit;">Save username</button>
        </div>
      </div>`;
    document.body.appendChild(overlay);
    const input = overlay.querySelector('#username-prompt-input');
    input.focus();

    const close = () => overlay.remove();
    overlay.querySelector('#username-prompt-later').onclick = () => {
      try { sessionStorage.setItem('nswpsn_username_prompt_dismissed', '1'); } catch (e) {}
      close();
    };
    const save = async () => {
      const msg = overlay.querySelector('#username-prompt-msg');
      const username = input.value.trim();
      if (username.length < 2) {
        msg.textContent = 'Username must be at least 2 characters.';
        return;
      }
      const btn = overlay.querySelector('#username-prompt-save');
      btn.disabled = true;
      btn.textContent = 'Saving…';
      const { error } = await sb.auth.updateUser({ data: { display_name: username } });
      if (error) {
        btn.disabled = false;
        btn.textContent = 'Save username';
        msg.textContent = error.message;
        return;
      }
      if (typeof umami !== 'undefined') umami.track('username-prompt-saved');
      close();
      checkAuthState();
    };
    overlay.querySelector('#username-prompt-save').onclick = save;
    input.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); save(); } });
  } catch (e) {
    console.warn('username prompt failed', e);
  }
}

async function doDiscordLogin() {
  const errorDiv = document.getElementById('login-error');
  const btn = document.getElementById('discord-modal-btn');
  if (errorDiv) errorDiv.textContent = '';
  if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Redirecting to Discord…'; }

  // Return to the page the user is on; onAuthStateChange -> checkAuthState()
  // updates the sidebar once the session lands.
  const { error } = await sb.auth.signInWithOAuth({
    provider: 'discord',
    options: { redirectTo: window.location.origin + window.location.pathname + window.location.search }
  });

  if (error) {
    if (typeof umami !== 'undefined') umami.track('login-failed', { method: 'discord-modal' });
    if (errorDiv) errorDiv.textContent = error.message;
    if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fab fa-discord"></i> Continue with Discord'; }
  }
  // On success the browser navigates away to Discord.
}

async function doLogout() {
  if (typeof umami !== 'undefined') umami.track('logout');
  await sb.auth.signOut();
  checkAuthState();
}

async function checkAuthState() {
  const { data } = await sb.auth.getSession();
  const session = data.session;
  
  const loggedOutDiv = document.getElementById('auth-logged-out');
  const loggedInDiv = document.getElementById('auth-logged-in');
  const emailDiv = document.getElementById('auth-user-email');
  const buttonsDiv = document.getElementById('auth-role-buttons');
  
  if (!loggedOutDiv || !loggedInDiv) return;
  
  if (session) {
    // User is logged in - fetch their roles
    loggedOutDiv.style.display = 'none';
    loggedInDiv.style.display = 'block';
    const meta = session.user?.user_metadata || {};
    // Never show the email in the sidebar — username / Discord name only
    // (email still visible inside the profile modal).
    const displayName = meta.display_name || meta.full_name || meta.name || meta.user_name
      || 'Account';
    if (emailDiv) emailDiv.textContent = displayName;

    // Existing account with no name at all: ask them to pick a username.
    maybeAskUsername(session);

    // Avatar: Discord avatar image when available, else the first letter.
    const avatarDiv = document.getElementById('auth-avatar');
    if (avatarDiv) {
      const customAvatar = meta.custom_avatar_url || meta.avatar_url;
      if (customAvatar) {
        avatarDiv.innerHTML = '';
        const img = document.createElement('img');
        img.src = customAvatar;
        img.alt = '';
        img.style.cssText = 'width:100%; height:100%; object-fit:cover;';
        avatarDiv.appendChild(img);
      } else {
        avatarDiv.textContent = (displayName || '?').charAt(0).toUpperCase();
      }
    }

    // Publish the user's display name (+ Discord avatar, if any) to the public
    // profile once per session, so their name/picture show when others open
    // their profile from a post AND so they're findable in the co-author search.
    // Runs for every logged-in user, not just Discord ones. The backend reads
    // both from the verified JWT — the body carries nothing sensitive.
    syncProfileAvatarOnce(session);

    // Notification bell: show the unread count and poll while the tab is visible.
    refreshNotifBadge();
    startNotifPolling();

    // Fetch roles with retry logic
    const fetchRolesWithRetry = async (retries = 2) => {
      const userId = session.user?.id;
      if (!userId) return null;
      for (let attempt = 0; attempt <= retries; attempt++) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 8000); // 8 second timeout
          
          const roleCheck = await fetch(`${API_BASE_URL}/api/check-editor/${userId}`, {
            signal: controller.signal
          });
          clearTimeout(timeoutId);
          
          if (roleCheck.ok) {
            return await roleCheck.json();
          } else {
            console.warn(`Role check failed with status ${roleCheck.status}, attempt ${attempt + 1}/${retries + 1}`);
          }
        } catch (e) {
          if (e.name === 'AbortError') {
            console.warn(`Role check timed out, attempt ${attempt + 1}/${retries + 1}`);
          } else {
            console.warn(`Role check error: ${e.message}, attempt ${attempt + 1}/${retries + 1}`);
          }
        }
        
        // Wait before retry (if not last attempt)
        if (attempt < retries) {
          await new Promise(r => setTimeout(r, 1000));
        }
      }
      return null;
    };
    
    const roleData = await fetchRolesWithRetry();
    
    if (roleData) {
      // Build role-based buttons
      let buttons = '';
      
      // (No Editor chip — the map page enables editor mode automatically
      // for signed-in editors, so the normal Incident Map link covers it.)

      // Staff chip - anyone who can load the staff page. Mirrors the backend
      // is_admin gate in /api/check-admin: owner, staff, any area manager, or
      // the view-only feeder:monitor role (read-only Data + Nodes tabs).
      // Legacy names are accepted too, for pre-migration-059 rows.
      const _roles = Array.isArray(roleData.roles) ? roleData.roles : [];
      const _hasAny = (...names) => names.some((n) => _roles.includes(n));
      if (roleData.is_team_member || roleData.is_owner ||
          _hasAny('staff', 'team_member', 'feeder:monitor', 'node_monitor',
                  'feeder:manager', 'wire:manager', 'map:manager')) {
        buttons += `<a href="staff.html" style="flex:1; display:flex; align-items:center; justify-content:center; gap:0.35rem; padding:0.35rem 0.5rem; background:rgba(249,115,22,0.15); border:1px solid rgba(249,115,22,0.3); border-radius:6px; color:#fb923c; font-size:0.72rem; text-decoration:none; white-space:nowrap;">
          <i class="fas fa-users-cog"></i> Staff
        </a>`;
      }

      // Radio Feeder chip - for radio contributors (links to their node
      // download + status page). Same shape/placement as the Staff chip,
      // distinct sky accent so the two are easy to tell apart.
      if (_hasAny('feeder:radio', 'radio_contributor', 'feeder:pager', 'pager_contributor')) {
        buttons += `<a href="feeder.html" style="flex:1; display:flex; align-items:center; justify-content:center; gap:0.35rem; padding:0.35rem 0.5rem; background:rgba(56,189,248,0.15); border:1px solid rgba(56,189,248,0.3); border-radius:6px; color:#38bdf8; font-size:0.72rem; text-decoration:none; white-space:nowrap;">
          <i class="fas fa-satellite-dish"></i> Feeder
        </a>`;
      }

      if (buttonsDiv) buttonsDiv.innerHTML = buttons;
    } else {
      // Role check failed after retries - show warning in sidebar
      console.error('Failed to load user roles after retries');
      if (buttonsDiv) {
        buttonsDiv.innerHTML = `<div style="font-size:0.75rem; color:#f97316; padding:0.5rem; background:rgba(249,115,22,0.1); border-radius:6px; text-align:center;">
          <i class="fas fa-exclamation-triangle"></i> Couldn't load roles. <a href="#" onclick="checkAuthState(); return false;" style="color:#60a5fa; text-decoration:underline;">Retry</a>
        </div>`;
      }
    }
  } else {
    // User is logged out
    loggedOutDiv.style.display = 'block';
    loggedInDiv.style.display = 'none';
    if (buttonsDiv) buttonsDiv.innerHTML = '';
    // Logged out: no bell activity.
    stopNotifPolling();
    closeNotifPanel();
    const nb = document.getElementById('notif-badge');
    if (nb) nb.style.display = 'none';
  }
}

// Close modals on Escape key
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    closeLoginModal();
    closePasswordResetModal();
  }
});

// Password Reset Functions
function openPasswordResetModal() {
  const modal = document.getElementById('password-reset-modal');
  const form = document.getElementById('password-reset-form');
  const message = document.getElementById('reset-message');
  const emailInput = document.getElementById('reset-email-input');
  
  if (modal) {
    modal.style.display = 'flex';
    if (form) form.reset();
    if (message) message.textContent = '';
    // Pre-fill with email from login form if available
    const loginEmail = document.getElementById('login-email')?.value;
    if (emailInput && loginEmail) {
      emailInput.value = loginEmail;
    }
    if (emailInput) emailInput.focus();
  }
}

function closePasswordResetModal() {
  const modal = document.getElementById('password-reset-modal');
  if (modal) {
    modal.style.display = 'none';
  }
}

async function handlePasswordResetRequest(event) {
  event.preventDefault();
  
  const emailInput = document.getElementById('reset-email-input');
  const submitBtn = document.getElementById('reset-submit-btn');
  const messageEl = document.getElementById('reset-message');
  
  if (!emailInput || !submitBtn || !messageEl) return;
  
  const email = emailInput.value.trim();
  if (!email) {
    messageEl.style.color = '#ef4444';
    messageEl.textContent = 'Please enter your email address.';
    return;
  }
  
  // Disable button and show loading state
  submitBtn.disabled = true;
  const originalText = submitBtn.textContent;
  submitBtn.textContent = 'Sending...';
  messageEl.textContent = '';
  
  try {
    const { error } = await sb.auth.resetPasswordForEmail(email, {
      redirectTo: window.location.origin + '/reset-password.html'
    });
    
    if (error) {
      messageEl.style.color = '#ef4444';
      messageEl.textContent = error.message;
    } else {
      if (typeof umami !== 'undefined') umami.track('password-reset-request', { method: 'modal' });
      messageEl.style.color = '#22c55e';
      messageEl.textContent = 'Reset link sent! Check your email inbox.';
      emailInput.value = '';
    }
  } catch (err) {
    messageEl.style.color = '#ef4444';
    messageEl.textContent = 'An error occurred. Please try again.';
  } finally {
    submitBtn.disabled = false;
    submitBtn.textContent = originalText;
  }
}

// Initialize auth on DOM ready
function initAuth() {
  createAuthModals();  // Always create modals (for pages like map-editor that need them)
  injectAuthSection(); // Only injects if sidebar-footer exists
  
  // Initial check
  checkAuthState();
  
  // Listen for auth state changes (session restored, login, logout, token refresh)
  // This ensures the sidebar updates when the session is restored from storage
  sb.auth.onAuthStateChange((event, session) => {
    // Log auth events for debugging (can remove in production)
    if (event === 'SIGNED_IN' || event === 'SIGNED_OUT' || event === 'INITIAL_SESSION') {
      console.log('[Auth]', event, session ? 'Session present' : 'No session');
    }
    checkAuthState();
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initAuth);
} else {
  initAuth();
}
