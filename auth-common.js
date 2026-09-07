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
        <div style="display:flex; align-items:center; gap:0.6rem; position:relative;">
          <button type="button" id="auth-avatar-btn" onclick="toggleAccountMenu(event)" title="Account" aria-label="Account menu" aria-haspopup="menu" style="padding:0; background:none; border:0; cursor:pointer; flex-shrink:0; border-radius:50%;">
            <div id="auth-avatar" style="width:40px; height:40px; border-radius:50%; background:rgba(249,115,22,0.2); display:flex; align-items:center; justify-content:center; color:#f97316; font-weight:700; font-size:1rem; overflow:hidden; border:2px solid rgba(148,163,184,0.25); transition:border-color 0.15s;"></div>
          </button>
          <div id="auth-user-email" style="flex:1; min-width:0; font-size:0.82rem; color:#fff; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;"></div>
          <button onclick="toggleNotifications(event)" id="notif-btn" title="Notifications" aria-label="Notifications" style="position:relative; width:28px; height:28px; padding:0; background:rgba(148,163,184,0.1); border:1px solid rgba(148,163,184,0.2); border-radius:6px; color:#cbd5e1; font-size:0.75rem; cursor:pointer; display:flex; align-items:center; justify-content:center; font-family:inherit; flex-shrink:0;">
            <i class="fas fa-bell"></i>
            <span id="notif-badge" style="display:none; position:absolute; top:-5px; right:-5px; min-width:15px; height:15px; padding:0 3px; box-sizing:border-box; background:#ef4444; color:#fff; border-radius:999px; font-size:0.6rem; font-weight:700; line-height:15px; text-align:center;"></span>
          </button>
          <div id="auth-account-menu" role="menu" style="display:none; position:absolute; top:calc(100% + 6px); left:0; z-index:1200; min-width:230px; background:#1e293b; border:1px solid rgba(148,163,184,0.25); border-radius:10px; box-shadow:0 12px 30px rgba(0,0,0,0.5); padding:0.35rem; box-sizing:border-box;">
            <div id="auth-menu-roles"></div>
            <button type="button" role="menuitem" onclick="closeAccountMenu(); openProfileModal();" style="display:flex; align-items:center; gap:0.6rem; width:100%; padding:0.55rem 0.7rem; background:none; border:0; border-radius:7px; color:#e2e8f0; font-size:0.83rem; font-family:inherit; cursor:pointer; text-align:left;" onmouseover="this.style.background='rgba(148,163,184,0.1)'" onmouseout="this.style.background='none'">
              <i class="fas fa-user-cog" style="width:16px; text-align:center; color:#94a3b8;"></i> Profile &amp; Account Settings
            </button>
            <button type="button" role="menuitem" onclick="closeAccountMenu(); doLogout();" style="display:flex; align-items:center; gap:0.6rem; width:100%; padding:0.55rem 0.7rem; background:none; border:0; border-radius:7px; color:#fca5a5; font-size:0.83rem; font-family:inherit; cursor:pointer; text-align:left;" onmouseover="this.style.background='rgba(239,68,68,0.12)'" onmouseout="this.style.background='none'">
              <i class="fas fa-sign-out-alt" style="width:16px; text-align:center;"></i> Logout
            </button>
          </div>
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
          <div style="color:#94a3b8; font-size:0.9rem; margin-top:0.5rem;">AusAware</div>
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
    <style>
      #profile-grid { display:grid; grid-template-columns:1fr 1fr; gap:2.4rem; align-items:start; }
      @media (max-width: 900px) { #profile-grid { grid-template-columns:1fr; gap:1.4rem; } }
      #profile-modal .pf-card { background:#1e293b; border:1px solid rgba(148,163,184,0.2); border-radius:12px; padding:1.4rem 1.5rem; margin-bottom:1.2rem; }
      #profile-modal .pf-col-hd { color:#f97316; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.1em; font-weight:700; margin:0 0 0.8rem; }
      #profile-tabs { display:flex; gap:0.4rem; margin-bottom:1.3rem; }
      #profile-tabs button { background:rgba(148,163,184,0.08); border:1px solid rgba(148,163,184,0.2); border-radius:8px; color:#94a3b8; font:inherit; font-size:0.82rem; font-weight:600; padding:0.45rem 1.1rem; cursor:pointer; }
      #profile-tabs button.on { background:rgba(249,115,22,0.16); border-color:rgba(249,115,22,0.45); color:#f97316; }
      #profile-modal .pf-sect-hd { color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; font-weight:600; margin-bottom:0.6rem; display:block; }
    </style>
    <div style="background:#0f172a; width:100%; height:100%; overflow-y:auto; padding:1.6rem clamp(1rem, 4vw, 3rem) 3rem; box-sizing:border-box;">
      <div style="max-width:1180px; margin:0 auto;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1.4rem;">
        <h2 style="margin:0; font-size:1.35rem; font-weight:800; color:#fff;">Your Profile</h2>
        <button onclick="closeProfileModal()" style="background:rgba(148,163,184,0.12); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#cbd5e1; font-size:1.3rem; cursor:pointer; padding:0; width:34px; height:34px; display:flex; align-items:center; justify-content:center;">&times;</button>
      </div>

      <div id="profile-tabs">
        <button data-ptab="main" class="on">Profile</button>
        <button data-ptab="posts" id="profile-posts-tab-btn" style="display:none">My posts</button>
      </div>

      <div id="profile-tab-main">
      <div id="profile-grid">
      <div><!-- MAIN settings: who you are -->
      <div class="pf-col-hd">Profile</div>
      <div class="pf-card">
      <div style="text-align:center; margin-bottom:1.3rem;">
        <div id="profile-avatar-preview" style="width:82px; height:82px; border-radius:50%; margin:0 auto 0.55rem; background:rgba(249,115,22,0.2); color:#f97316; display:grid; place-items:center; font-size:1.9rem; font-weight:700; overflow:hidden; border:2px solid rgba(148,163,184,0.25);"></div>
        <button type="button" onclick="pickProfileAvatar()" id="profile-avatar-btn" style="padding:0.45rem 0.9rem; background:rgba(148,163,184,0.12); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#e2e8f0; font-size:0.8rem; cursor:pointer; font-family:inherit;"><i class="fas fa-camera"></i> Change picture</button>
        <button type="button" onclick="removeProfileAvatar()" id="profile-avatar-remove" style="display:none; padding:0.45rem 0.9rem; background:rgba(239,68,68,0.12); border:1px solid rgba(239,68,68,0.3); border-radius:8px; color:#fca5a5; font-size:0.8rem; cursor:pointer; font-family:inherit; margin-left:0.4rem;"><i class="fas fa-trash"></i> Remove</button>
        <input type="file" id="profile-avatar-input" accept="image/jpeg,image/png,image/webp" style="display:none">
        <div style="color:#64748b; font-size:0.72rem; margin-top:0.4rem;">Overrides your Discord avatar.</div>
        <div id="profile-stats" style="display:none; justify-content:center; gap:1.4rem; margin-top:0.9rem;"></div>
        <div id="profile-tags" style="display:none; flex-wrap:wrap; justify-content:center; gap:0.35rem; margin-top:0.7rem;"></div>
      </div>

      <div style="margin-bottom:1.2rem;">
        <label style="display:block; color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:0.5rem; font-weight:600;">Display name</label>
        <div style="display:flex; gap:0.5rem;">
          <input type="text" id="profile-username" maxlength="40" style="flex:1; padding:0.65rem 0.75rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.9rem; box-sizing:border-box; font-family:inherit;" placeholder="Your public name — spaces and caps welcome">
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

      <div style="margin-bottom:0.4rem;">
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
      <div id="profile-message" style="margin-top:0.6rem; font-size:0.85rem; text-align:center; min-height:1.2em;"></div>
      </div><!-- /pf-card -->
      </div><!-- /main column -->

      <div><!-- OTHER settings: account + media (more to come) -->
      <div class="pf-col-hd">Account &amp; media</div>

      <div id="profile-wm-section" class="pf-card" style="display:none;">
        <label style="display:block; color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:0.5rem; font-weight:600;">Media watermark</label>
        <canvas id="profile-wm-preview" width="640" height="480" style="width:100%; border-radius:10px; border:1px solid rgba(148,163,184,0.25); background:#0b1220; display:none; margin-bottom:0.55rem;"></canvas>
        <div style="display:flex; gap:0.5rem; align-items:center; flex-wrap:wrap;">
          <button type="button" onclick="pickProfileWatermark()" style="padding:0.45rem 0.9rem; background:rgba(148,163,184,0.12); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#e2e8f0; font-size:0.8rem; cursor:pointer; font-family:inherit;"><i class="fas fa-stamp"></i> Upload watermark (PNG)</button>
          <button type="button" id="profile-wm-remove" onclick="removeProfileWatermark()" style="display:none; padding:0.45rem 0.9rem; background:rgba(239,68,68,0.12); border:1px solid rgba(239,68,68,0.3); border-radius:8px; color:#fca5a5; font-size:0.8rem; cursor:pointer; font-family:inherit;"><i class="fas fa-trash"></i> Remove</button>
          <span id="profile-wm-msg" style="color:#64748b; font-size:0.72rem;"></span>
        </div>
        <input type="file" id="profile-wm-input" accept="image/png" style="display:none">
        <div style="color:#64748b; font-size:0.72rem; margin-top:0.4rem;">A transparent PNG, stamped onto the bottom-right of your Wire &amp; Fleet photos when the watermark toggle is on. Without one, your username is used. The preview shows how it will sit on a photo.</div>
        <label style="display:flex; gap:0.55rem; align-items:flex-start; margin-top:0.7rem; padding-top:0.7rem; border-top:1px solid rgba(148,163,184,0.15); cursor:pointer;">
          <input type="checkbox" id="profile-wm-default" onchange="saveWatermarkDefault(this)" style="width:auto; margin-top:0.15rem; accent-color:#38bdf8;">
          <span style="color:#cbd5e1; font-size:0.82rem;">Watermark my media by default
            <span style="display:block; color:#64748b; font-size:0.72rem; margin-top:0.15rem;">Starts the watermark switch on when you compose. You can still change it per post.</span>
          </span>
        </label>
      </div>

      <div id="profile-referral-section" class="pf-card" style="display:none;">
        <label style="display:block; color:#cbd5e1; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:0.5rem; font-weight:600;">Referral link</label>
        <div style="display:flex; align-items:center; gap:0.5rem;">
          <input type="text" id="profile-referral-link" readonly style="flex:1; min-width:0; padding:0.55rem 0.7rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#e2e8f0; font-size:0.8rem; box-sizing:border-box; font-family:inherit;">
          <button type="button" onclick="copyProfileReferralLink(this)" title="Copy link" style="padding:0.5rem 0.8rem; background:rgba(148,163,184,0.12); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#e2e8f0; font-size:0.8rem; cursor:pointer; font-family:inherit;"><i class="fas fa-copy"></i></button>
        </div>
        <div id="profile-referral-stats" style="color:#64748b; font-size:0.72rem; margin-top:0.4rem;"></div>
        <div style="color:#64748b; font-size:0.72rem; margin-top:0.3rem;">Send this to someone you'd vouch for as a contributor — their application arrives tagged with your name.</div>
      </div>

      <div class="pf-card">
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
          <button id="profile-discord-unlink-btn" onclick="unlinkDiscordAccount()" style="display:none; padding:0.35rem 0.7rem; background:rgba(239,68,68,0.12); border:1px solid rgba(239,68,68,0.3); border-radius:6px; color:#fca5a5; font-weight:600; cursor:pointer; font-size:0.75rem; font-family:inherit;">Unlink</button>
        </div>
        <a id="profile-change-password" href="change-password.html" style="display:flex; width:100%; padding:0.55rem; background:rgba(148,163,184,0.1); border:1px solid rgba(148,163,184,0.2); border-radius:6px; color:#94a3b8; font-size:0.8rem; cursor:pointer; align-items:center; justify-content:center; gap:0.4rem; font-family:inherit; text-decoration:none; box-sizing:border-box; margin-top:0.7rem;">
          <i class="fas fa-key"></i> Change Password
        </a>
      </div>

      </div><!-- /other column -->
      </div><!-- /profile-grid -->
      </div><!-- /profile-tab-main -->

      <div id="profile-tab-posts" style="display:none;">
        <div id="profile-deleted-card" class="pf-card" style="display:none; border-color:rgba(239,68,68,0.4);">
          <label class="pf-sect-hd" style="color:#fca5a5;">Pending deletion</label>
          <div style="color:#64748b; font-size:0.72rem; margin:-0.3rem 0 0.6rem;">Deleted posts stay recoverable for 5 days, then they're gone for good.</div>
          <div id="profile-deleted-list"></div>
        </div>
        <div id="profile-drafts-card" class="pf-card" style="display:none; border-color:rgba(245,158,11,0.35);">
          <label class="pf-sect-hd" style="color:#fbbf24;">Drafts</label>
          <div id="profile-drafts-list"></div>
        </div>
        <div class="pf-card">
          <label class="pf-sect-hd">Articles</label>
          <div id="profile-articles-list"></div>
        </div>
        <div class="pf-card">
          <label class="pf-sect-hd">Fleet</label>
          <div id="profile-fleet-list"></div>
        </div>
      </div>
      </div>
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
  const wmInput = document.getElementById('profile-wm-input');
  if (wmInput) wmInput.addEventListener('change', handleProfileWatermarkFile);
  document.querySelectorAll('#profile-tabs button').forEach((b) => b.addEventListener('click', () => {
    document.querySelectorAll('#profile-tabs button').forEach((x) => x.classList.toggle('on', x === b));
    document.getElementById('profile-tab-main').style.display = b.dataset.ptab === 'main' ? '' : 'none';
    document.getElementById('profile-tab-posts').style.display = b.dataset.ptab === 'posts' ? '' : 'none';
  }));
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
    const btn2 = 'background:none;border:0;font:inherit;font-size:0.72rem;cursor:pointer;padding:0;';
    const head = `<div style="display:flex; align-items:center; justify-content:space-between; gap:0.5rem; padding:0.45rem 0.6rem 0.6rem; border-bottom:1px solid rgba(148,163,184,0.15);">
        <span style="font-size:0.7rem; text-transform:uppercase; letter-spacing:0.1em; color:#94a3b8; font-weight:700;">Notifications</span>
        <span style="display:flex; gap:0.6rem; align-items:center;">
          ${list.some((n) => !n.read) ? `<button id="notif-readall" style="${btn2}color:#f97316;">Mark all read</button>` : ''}
          ${list.length ? `<button id="notif-clear" style="${btn2}color:#94a3b8;">Clear</button>` : ''}
        </span>
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
    // Clearing deletes the list rather than just quieting the badge, so it asks
    // once. Deliberately a second click on the button itself rather than a
    // dialog: this file is loaded on every page, and ui-dialogs.js is not.
    const clearBtn = panel.querySelector('#notif-clear');
    if (clearBtn) {
      let armed = false;
      clearBtn.addEventListener('click', async (e) => {
        e.preventDefault(); e.stopPropagation();
        if (!armed) {
          armed = true;
          clearBtn.textContent = 'Clear all? Tap again';
          clearBtn.style.color = '#ef4444';
          setTimeout(() => {
            if (!armed || !clearBtn.isConnected) return;
            armed = false; clearBtn.textContent = 'Clear'; clearBtn.style.color = '#94a3b8';
          }, 4000);
          return;
        }
        armed = false;
        await clearNotifications();
        closeNotifPanel();
      });
    }
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

// Empty the caller's notification list. Server-side this is scoped to the
// signed-in user, so it can only ever clear your own.
async function clearNotifications() {
  try {
    const h = await notifHeaders();
    if (!h) return;
    await fetch(`${API_BASE_URL}/api/notifications`, { method: 'DELETE', headers: h });
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

// ---- custom media watermark (profile) --------------------------------------
// The preview draws the uploaded PNG bottom-right over an example photo so
// the contributor sees exactly how their photos will carry it.
let _wmPreviewBitmap = null; // the user's watermark, as an ImageBitmap
const _WM_PREVIEW_BG = 'assets/watermark-preview.webp';

function pickProfileWatermark() { document.getElementById('profile-wm-input')?.click(); }

function _wmMsg(text, err) {
  const el = document.getElementById('profile-wm-msg');
  if (el) { el.textContent = text || ''; el.style.color = err ? '#fca5a5' : '#64748b'; }
}

async function drawWatermarkPreview() {
  const canvas = document.getElementById('profile-wm-preview');
  if (!canvas) return;
  // Hidden until a watermark exists -- an example scene with nothing on it
  // reads as a stray photo in the panel.
  if (!_wmPreviewBitmap) { canvas.style.display = 'none'; return; }
  canvas.style.display = 'block';
  const ctx = canvas.getContext('2d');
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  // Example background, cover-fitted.
  try {
    const bg = await new Promise((res, rej) => {
      const img = new Image();
      img.onload = () => res(img); img.onerror = rej;
      img.src = _WM_PREVIEW_BG;
    });
    const s = Math.max(w / bg.width, h / bg.height);
    ctx.drawImage(bg, (w - bg.width * s) / 2, (h - bg.height * s) / 2, bg.width * s, bg.height * s);
  } catch (e) {
    ctx.fillStyle = '#1e293b'; ctx.fillRect(0, 0, w, h);
  }
  // Same placement maths the composers use: ~28% of the width, 2.5% pad.
  const targetW = Math.max(64, Math.round(w * 0.28));
  const targetH = Math.round(targetW * (_wmPreviewBitmap.height / _wmPreviewBitmap.width));
  const pad = Math.round(Math.min(w, h) * 0.025);
  ctx.globalAlpha = 0.95;
  ctx.drawImage(_wmPreviewBitmap, w - targetW - pad, h - targetH - pad, targetW, targetH);
  ctx.globalAlpha = 1;
}

async function loadProfileWatermark(session) {
  _wmPreviewBitmap = null;
  try {
    const r = await fetch(`${API_BASE_URL}/api/profiles/watermark`, { headers: { Authorization: 'Bearer ' + session.access_token } });
    if (r.ok) _wmPreviewBitmap = await createImageBitmap(await r.blob());
  } catch (e) { /* none set */ }
  const rm = document.getElementById('profile-wm-remove');
  if (rm) rm.style.display = _wmPreviewBitmap ? 'inline-block' : 'none';
  drawWatermarkPreview();
}

async function removeProfileWatermark() {
  try {
    const { data } = await sb.auth.getSession();
    const jwt = data.session?.access_token; if (!jwt) return;
    const r = await fetch(`${API_BASE_URL}/api/profiles/watermark`, {
      method: 'PUT', headers: { Authorization: 'Bearer ' + jwt, 'Content-Type': 'application/json' },
      body: JSON.stringify({ key: null }),
    });
    if (!r.ok) { _wmMsg('Failed to remove.', true); return; }
    _wmPreviewBitmap = null;
    const rm = document.getElementById('profile-wm-remove');
    if (rm) rm.style.display = 'none';
    _wmMsg('Watermark removed.');
    drawWatermarkPreview();
  } catch (e) { _wmMsg('Failed to remove.', true); }
}

async function handleProfileWatermarkFile() {
  const inp = document.getElementById('profile-wm-input');
  {
    const file = inp.files && inp.files[0];
    inp.value = '';
    if (!file) return;
    if (file.type !== 'image/png') { _wmMsg('Watermarks must be PNG files (for transparency).', true); return; }
    if (file.size > 1_000_000) { _wmMsg('Keep the watermark under 1 MB.', true); return; }
    _wmMsg('Uploading…');
    try {
      const { data } = await sb.auth.getSession();
      const jwt = data.session?.access_token; if (!jwt) return;
      const pre = await fetch(`${API_BASE_URL}/api/profiles/watermark-url`, { method: 'POST', headers: { Authorization: 'Bearer ' + jwt } });
      if (!pre.ok) { _wmMsg('Uploads not available right now.', true); return; }
      const { uploadURL, key } = await pre.json();
      const put = await fetch(uploadURL, { method: 'PUT', body: file, headers: { 'Content-Type': 'image/png' } });
      if (!put.ok) throw new Error('upload failed');
      const save = await fetch(`${API_BASE_URL}/api/profiles/watermark`, {
        method: 'PUT', headers: { Authorization: 'Bearer ' + jwt, 'Content-Type': 'application/json' },
        body: JSON.stringify({ key }),
      });
      if (!save.ok) throw new Error('save failed');
      _wmPreviewBitmap = await createImageBitmap(file);
      const rm = document.getElementById('profile-wm-remove');
      if (rm) rm.style.display = 'inline-block';
      _wmMsg('Watermark saved.');
      drawWatermarkPreview();
    } catch (e) { _wmMsg('Upload failed — try again.', true); }
  }
}

/** Remove the custom profile picture (the Discord fallback, if any,
 *  returns). Sends the full form alongside clear_avatar because the
 *  profile PUT is a whole-row upsert -- a clear-only body would blank
 *  the name/bio/socials. */
async function removeProfileAvatar() {
  try {
    const { data } = await sb.auth.getSession();
    const jwt = data.session?.access_token; if (!jwt) return;
    const v = (id) => (document.getElementById(id)?.value || '').trim();
    const body = {
      display_name: v('profile-username') || null, bio: v('profile-bio'),
      twitter: v('profile-twitter'), facebook: v('profile-facebook'),
      instagram: v('profile-instagram'), youtube: v('profile-youtube'), website: v('profile-website'),
      clear_avatar: true,
    };
    const r = await fetch(`${API_BASE_URL}/api/profiles`, {
      method: 'PUT', headers: { Authorization: 'Bearer ' + jwt, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) return;
    await sb.auth.updateUser({ data: { custom_avatar_url: null } }).catch(() => {});
    const j = await r.json().catch(() => ({}));
    const prev = document.getElementById('profile-avatar-preview');
    const fallback = j.profile && j.profile.avatar_url;
    if (prev) {
      prev.innerHTML = fallback
        ? `<img src="${fallback}" style="width:100%;height:100%;object-fit:cover;">`
        : (v('profile-username') || '?').charAt(0).toUpperCase();
    }
    const rm = document.getElementById('profile-avatar-remove');
    if (rm) rm.style.display = 'none';
    checkAuthState();
  } catch (e) { /* leave the picture in place */ }
}

/** Unlink the Discord identity, after an explicit confirmation. Only ever
 *  offered while an email identity exists (see the modal-open wiring). */
async function unlinkDiscordAccount() {
  const ask = 'Unlink your Discord account? You will keep signing in with your email and password.';
  const ok = typeof uiConfirm === 'function'
    ? await uiConfirm(ask, { title: 'Unlink Discord?', okText: 'Unlink', danger: true })
    : window.confirm(ask);
  if (!ok) return;
  const msg = document.getElementById('profile-message');
  try {
    const { data, error: idErr } = await sb.auth.getUserIdentities();
    if (idErr) throw idErr;
    const discord = (data?.identities || []).find((i) => i.provider === 'discord');
    if (!discord) return;
    const { error } = await sb.auth.unlinkIdentity(discord);
    if (error) throw error;
    document.getElementById('profile-discord-value').textContent = 'Not linked';
    document.getElementById('profile-discord-link-btn').style.display = 'inline-block';
    document.getElementById('profile-discord-linked-badge').style.display = 'none';
    document.getElementById('profile-discord-unlink-btn').style.display = 'none';
    if (msg) { msg.style.color = '#22c55e'; msg.textContent = 'Discord account unlinked.'; }
  } catch (e) {
    if (msg) { msg.style.color = '#ef4444'; msg.textContent = 'Could not unlink: ' + (e && e.message ? e.message : 'unknown error'); }
  }
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


// ---- "My posts" inside the profile modal ---------------------------------
// Replaces the separate "My posts" button that used to sit on The Wire. Shows
// EVERYTHING you've written, including drafts and anything awaiting review —
// this is your own private view, so unlike a public profile it shouldn't hide
// your unpublished work from you.
function profilePostHref(item) {
  if (item.kind === 'fleet') return `wire?tab=fleet&vehicle=${encodeURIComponent(item.id)}`;
  return `wire?article=${encodeURIComponent(item.slug || item.id)}`;
}

function _profilePostRow(i) {
  const cover = i.cover || null;
  const src = i.kind === 'fleet'
    ? (i.image_url || null)
    : (cover && cover.kind === 'image' ? (cover.thumb_url || cover.feed_url || cover.url)
      : (cover ? (cover.poster_feed_url || cover.poster_url) : null));
  const thumb = src
    ? `<img src="${escNotif(src)}" alt="" loading="lazy" style="width:100%; height:100%; object-fit:cover;">`
    : `<i class="fas fa-${i.kind === 'fleet' ? 'truck-field' : 'newspaper'}" style="color:#475569;"></i>`;
  // Only flag what ISN'T live — a published post needs no badge.
  const st = i.status && i.status !== 'published'
    ? `<span style="font-size:0.62rem; text-transform:uppercase; letter-spacing:0.04em; font-weight:700; color:#fbbf24;">${escNotif(i.status)}</span>` : '';
  return `<a href="${escNotif(profilePostHref(i))}" style="display:flex; gap:0.9rem; align-items:center; padding:0.6rem; border-radius:10px; text-decoration:none; color:inherit;" onmouseover="this.style.background='rgba(148,163,184,0.07)'" onmouseout="this.style.background='none'">
    <div style="width:132px; height:88px; flex-shrink:0; border-radius:9px; overflow:hidden; background:rgba(2,6,23,0.5); display:grid; place-items:center; font-size:1.4rem;">${thumb}</div>
    <div style="min-width:0; flex:1;">
      <div style="font-size:1rem; font-weight:700; color:#e2e8f0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${escNotif(i.title || 'Untitled')}</div>
      <div style="font-size:0.78rem; color:#64748b; display:flex; gap:0.6rem; align-items:center; margin-top:0.25rem;">
        <span>${escNotif(notifAgo(i.published_at || i.created_at) || '')}</span>
        ${st}
      </div>
    </div>
  </a>`;
}

function _deletedPostRow(i) {
  const daysLeft = i.delete_after
    ? Math.max(0, Math.ceil((new Date(i.delete_after).getTime() - Date.now()) / 86_400_000))
    : '?';
  return `<div style="display:flex; gap:0.6rem; align-items:center; padding:0.45rem; border-radius:8px;">
    <div style="min-width:0; flex:1;">
      <div style="font-size:0.82rem; font-weight:600; color:#e2e8f0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${escNotif(i.title || 'Untitled')}</div>
      <div style="font-size:0.7rem; color:#64748b;">${i.kind === 'fleet' ? 'Fleet vehicle' : 'Article'} · deletes in ${daysLeft} day${daysLeft === 1 ? '' : 's'}</div>
    </div>
    <button type="button" data-recover="${escNotif(i.kind)}:${escNotif(i.id)}" style="padding:0.35rem 0.8rem; background:rgba(34,197,94,0.12); border:1px solid rgba(34,197,94,0.35); border-radius:7px; color:#86efac; font-size:0.75rem; font-weight:600; cursor:pointer; font-family:inherit; flex-shrink:0;"><i class="fas fa-rotate-left"></i> Recover</button>
  </div>`;
}

async function recoverProfilePost(kind, id, session) {
  const base = kind === 'fleet' ? 'fleet' : 'articles';
  try {
    const r = await fetch(`${API_BASE_URL}/api/wire/${base}/${encodeURIComponent(id)}/recover`, {
      method: 'POST', headers: { Authorization: 'Bearer ' + session.access_token },
    });
    if (r.ok) loadProfilePosts(session);
  } catch (e) { /* the row stays; they can retry */ }
}

function _fillProfileList(id, items, emptyText) {
  const el = document.getElementById(id);
  if (!el) return;
  el.innerHTML = items.length
    ? items.map(_profilePostRow).join('')
    : `<div style="color:#64748b; font-size:0.8rem; padding:0.3rem 0;">${emptyText}</div>`;
}

async function loadProfilePosts(session) {
  const tabBtn = document.getElementById('profile-posts-tab-btn');
  if (!tabBtn) return;
  const h = { Authorization: 'Bearer ' + session.access_token };
  const byDate = (a, b) => new Date(b.published_at || b.created_at || 0) - new Date(a.published_at || a.created_at || 0);
  let articles = [], fleet = [], delA = [], delF = [];
  try {
    [articles, fleet, delA, delF] = await Promise.all([
      fetch(`${API_BASE_URL}/api/wire/articles?mine=1`, { headers: h }).then((r) => r.json()).then((j) => j.articles || []).catch(() => []),
      fetch(`${API_BASE_URL}/api/wire/fleet?mine=1`, { headers: h }).then((r) => r.json()).then((j) => j.vehicles || []).catch(() => []),
      fetch(`${API_BASE_URL}/api/wire/articles?mine=1&deleted=1`, { headers: h }).then((r) => r.json()).then((j) => j.articles || []).catch(() => []),
      fetch(`${API_BASE_URL}/api/wire/fleet?mine=1&deleted=1`, { headers: h }).then((r) => r.json()).then((j) => j.vehicles || []).catch(() => []),
    ]);
  } catch (e) { /* all stay empty */ }
  const drafts = articles.filter((a) => a.status === 'draft').sort(byDate);
  const published = articles.filter((a) => a.status !== 'draft').sort(byDate);
  fleet = fleet.sort(byDate);
  const deleted = [...delA, ...delF].sort((a, b) => new Date(a.delete_after || 0) - new Date(b.delete_after || 0));
  // Contributors only — a reader with no posts gets no empty tab.
  if (!articles.length && !fleet.length && !deleted.length) { tabBtn.style.display = 'none'; return; }
  tabBtn.style.display = '';
  const draftsCard = document.getElementById('profile-drafts-card');
  if (draftsCard) draftsCard.style.display = drafts.length ? 'block' : 'none';
  _fillProfileList('profile-drafts-list', drafts, '');
  _fillProfileList('profile-articles-list', published, 'No articles yet.');
  _fillProfileList('profile-fleet-list', fleet, 'No fleet vehicles yet.');
  const delCard = document.getElementById('profile-deleted-card');
  if (delCard) delCard.style.display = deleted.length ? 'block' : 'none';
  const delList = document.getElementById('profile-deleted-list');
  if (delList) {
    delList.innerHTML = deleted.map(_deletedPostRow).join('');
    delList.querySelectorAll('[data-recover]').forEach((b) => b.addEventListener('click', () => {
      const [kind, id] = b.dataset.recover.split(':');
      b.disabled = true;
      recoverProfilePost(kind, id, session);
    }));
  }
}

/** Your badges. Awarded by staff, or automatically for posting to The Wire
 *  before it went public. Shown with full labels — the modal has the room. */
function renderProfileTags(tags) {
  const el = document.getElementById('profile-tags');
  if (!el) return;
  if (!tags || !tags.length) { el.style.display = 'none'; return; }
  el.style.display = 'flex';
  el.innerHTML = tags.map((t) =>
    `<span title="${escNotif(t.description || '')}" style="display:inline-flex;align-items:center;gap:0.3rem;font-size:0.72rem;font-weight:600;padding:0.25rem 0.55rem;border-radius:999px;border:1px solid ${escNotif(t.color)};color:${escNotif(t.color)};background:rgba(148,163,184,0.08);">
       <i class="${escNotif(t.icon)}"></i>${escNotif(t.label)}</span>`
  ).join('');
}

/** Likes + views across your published work, same figures the public sees. */
function renderProfileStats(stats) {
  const el = document.getElementById('profile-stats');
  if (!el) return;
  if (!stats || (!stats.posts && !stats.views && !stats.likes)) { el.style.display = 'none'; return; }
  el.style.display = 'flex';
  el.innerHTML = [
    ['fa-regular fa-heart', stats.likes, 'likes'],
    ['fa-regular fa-eye', stats.views, 'views'],
  ].map(([icon, n, label]) => `<span style="display:flex; align-items:center; gap:0.35rem; color:#94a3b8; font-size:0.8rem;">
      <i class="${icon}"></i><strong style="color:#e2e8f0;">${Number(n || 0).toLocaleString()}</strong> ${label}
    </span>`).join('');
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
  const unlinkBtn = document.getElementById('profile-discord-unlink-btn');
  if (discordIdentity) {
    const idData = discordIdentity.identity_data || {};
    discordValue.textContent = idData.full_name || idData.name || idData.user_name || 'Discord';
    linkBtn.style.display = 'none';
    linkedBadge.style.display = 'inline';
    // Unlinking needs another way back in -- only offered when an email
    // identity exists, or the account would be locked out.
    if (unlinkBtn) unlinkBtn.style.display = identities.some((i) => i.provider === 'email') ? 'inline-block' : 'none';
  } else {
    discordValue.textContent = 'Not linked';
    linkBtn.style.display = 'inline-block';
    linkedBadge.style.display = 'none';
    if (unlinkBtn) unlinkBtn.style.display = 'none';
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
      const pj = await pr.json();
      const profile = pj.profile;
      ['twitter', 'facebook', 'instagram', 'youtube', 'website'].forEach((k) => { const el = document.getElementById('profile-' + k); if (el) el.value = (profile && profile[k]) || ''; });
      const bioEl = document.getElementById('profile-bio');
      if (bioEl) { bioEl.value = (profile && profile.bio) || ''; updateBioCount(); }
      if (profile && profile.avatar_url && avPrev) avPrev.innerHTML = `<img src="${profile.avatar_url}" style="width:100%;height:100%;object-fit:cover;">`;
      const avRm = document.getElementById('profile-avatar-remove');
      if (avRm) avRm.style.display = profile && profile.has_custom_avatar ? 'inline-block' : 'none';
      const wmDef = document.getElementById('profile-wm-default');
      if (wmDef) wmDef.checked = !!(profile && profile.watermark_default);
      renderProfileStats(pj.stats);
      renderProfileTags(pj.tags);
    }
  } catch (e) { /* ignore */ }
  // Posts load after the form is populated so the modal isn't held on them.
  loadProfilePosts(session);
  // Watermark section: only shown to Wire contributors (it stamps their posts).
  try {
    const ce = await fetch(`${API_BASE_URL}/api/check-editor/${user.id}`).then((r) => r.json()).catch(() => ({}));
    const sect = document.getElementById('profile-wm-section');
    if (sect && (ce.can_feed_media || ce.is_owner)) {
      sect.style.display = 'block';
      loadProfileWatermark(session);
    } else if (sect) { sect.style.display = 'none'; }
    // Referral link: contributors who can vouch for new applicants. Mirrors
    // canRefer on the backend (which is the real gate — this only decides
    // whether to show the card).
    const REFERRAL_ROLES = ['feeder:radio', 'feeder:pager', 'wire:contributor', 'map:editor'];
    const refSect = document.getElementById('profile-referral-section');
    const mayRefer = !!ce.is_owner || REFERRAL_ROLES.some((r) => (ce.roles || []).includes(r));
    if (refSect) refSect.style.display = mayRefer ? 'block' : 'none';
    if (mayRefer) loadProfileReferral(session);
  } catch (e) { /* section stays hidden */ }

  const msg = document.getElementById('profile-message');
  if (msg) msg.textContent = '';
  document.getElementById('profile-modal').style.display = 'flex';
}

// Personal referral link + how it's doing. The code is minted server-side on
// first request (GET /api/referral-code, gated on canRefer).
async function loadProfileReferral(session) {
  const sect = document.getElementById('profile-referral-section');
  try {
    const res = await fetch(`${API_BASE_URL}/api/referral-code`, {
      headers: { Authorization: `Bearer ${session.access_token}` },
    });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const j = await res.json();
    if (!j.code) throw new Error('no code');
    const link = document.getElementById('profile-referral-link');
    if (link) link.value = `${location.origin}/signup?as=contributor&ref=${encodeURIComponent(j.code)}`;
    const stats = document.getElementById('profile-referral-stats');
    if (stats) {
      const uses = j.uses || 0;
      stats.textContent = `${uses} signup${uses === 1 ? '' : 's'} · ${j.approved || 0} approved`;
    }
  } catch (e) {
    if (sect) sect.style.display = 'none'; // no link is better than a broken one
  }
}

function copyProfileReferralLink(btn) {
  const el = document.getElementById('profile-referral-link');
  const text = el ? el.value : '';
  if (!text) return;
  const done = () => {
    if (!btn) return;
    const prev = btn.innerHTML;
    btn.innerHTML = '<i class="fas fa-check" style="color:#22c55e"></i>';
    setTimeout(() => { btn.innerHTML = prev; }, 1400);
  };
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(done).catch(() => { if (el) el.select(); });
  } else {
    try {
      el.select();
      document.execCommand('copy');
      done();
    } catch (e) { /* user can copy manually */ }
  }
}

// "Watermark my media by default" — an account preference the compose pages
// read as the initial state of their per-post watermark switch.
async function saveWatermarkDefault(el) {
  try {
    const { data } = await sb.auth.getSession();
    const token = data.session?.access_token;
    if (!token) return;
    await fetch(`${API_BASE_URL}/api/profiles/watermark-default`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ enabled: !!el.checked }),
    });
    // Keep the compose pages' local cache in step on this device.
    try { localStorage.setItem('nswpsn:wireWatermark', el.checked ? '1' : '0'); } catch (e) {}
  } catch (e) { /* preference is best-effort */ }
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
  // Display name: free text — spaces and capitals welcome (runs of
  // whitespace collapsed).
  const username = (input.value || '').trim().replace(/\s+/g, ' ');
  if (username.length > 0 && username.length < 2) {
    msg.style.color = '#ef4444';
    msg.textContent = 'Display name must be at least 2 characters.';
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
        <div style="font-size:1.1rem; font-weight:700; color:#fff; margin-bottom:0.4rem;"><i class="fas fa-user" style="color:#f97316; margin-right:0.4rem;"></i>Choose a display name</div>
        <p style="color:#94a3b8; font-size:0.85rem; margin:0 0 1rem;">Your account doesn't have a display name yet — pick how you'll appear around the site. Spaces and capitals are fine.</p>
        <input type="text" id="username-prompt-input" maxlength="40" placeholder="Display name" style="width:100%; padding:0.7rem 0.75rem; background:rgba(2,6,23,0.5); border:1px solid rgba(148,163,184,0.25); border-radius:8px; color:#fff; font-size:0.95rem; box-sizing:border-box; font-family:inherit;">
        <div id="username-prompt-msg" style="color:#ef4444; font-size:0.8rem; min-height:1.1em; margin-top:0.45rem;"></div>
        <div style="display:flex; gap:0.5rem; margin-top:0.7rem;">
          <button id="username-prompt-later" style="flex:1; padding:0.6rem; background:rgba(148,163,184,0.1); border:1px solid rgba(148,163,184,0.2); border-radius:8px; color:#94a3b8; font-size:0.85rem; cursor:pointer; font-family:inherit;">Later</button>
          <button id="username-prompt-save" style="flex:2; padding:0.6rem; background:#f97316; border:none; border-radius:8px; color:#fff; font-weight:700; font-size:0.85rem; cursor:pointer; font-family:inherit;">Save display name</button>
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
      const username = input.value.trim().replace(/\s+/g, ' ');
      if (username.length < 2) {
        msg.textContent = 'Display name must be at least 2 characters.';
        return;
      }
      const btn = overlay.querySelector('#username-prompt-save');
      btn.disabled = true;
      btn.textContent = 'Saving…';
      const { error } = await sb.auth.updateUser({ data: { display_name: username } });
      if (error) {
        btn.disabled = false;
        btn.textContent = 'Save display name';
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

// ---- account dropdown (the sidebar avatar) ----
function toggleAccountMenu(e) {
  if (e) e.stopPropagation();
  const menu = document.getElementById('auth-account-menu');
  if (!menu) return;
  menu.style.display = menu.style.display !== 'none' ? 'none' : 'block';
}
// One permanent outside-click closer (the toggle stops its own propagation).
document.addEventListener('click', (ev) => {
  if (!ev.target.closest('#auth-account-menu') && !ev.target.closest('#auth-avatar-btn')) closeAccountMenu();
});
function closeAccountMenu() {
  const menu = document.getElementById('auth-account-menu');
  if (menu) menu.style.display = 'none';
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
      // They live in the avatar dropdown, styled like its other entries.
      const menuItem = (href, icon, label, color) =>
        `<a href="${href}" role="menuitem" style="display:flex; align-items:center; gap:0.6rem; width:100%; padding:0.55rem 0.7rem; border-radius:7px; color:${color}; font-size:0.83rem; text-decoration:none; box-sizing:border-box;" onmouseover="this.style.background='rgba(148,163,184,0.1)'" onmouseout="this.style.background='none'">
          <i class="fas ${icon}" style="width:16px; text-align:center;"></i> ${label}
        </a>`;
      if (roleData.is_team_member || roleData.is_owner ||
          _hasAny('staff', 'team_member', 'feeder:monitor', 'node_monitor',
                  'feeder:manager', 'wire:manager', 'map:manager')) {
        buttons += menuItem('staff.html', 'fa-users-cog', 'Staff', '#fb923c');
      }

      // Radio Feeder - for radio contributors (links to their node
      // download + status page). Distinct sky accent from Staff.
      if (_hasAny('feeder:radio', 'radio_contributor', 'feeder:pager', 'pager_contributor')) {
        buttons += menuItem('feeder.html', 'fa-satellite-dish', 'Feeder', '#38bdf8');
      }

      const rolesSlot = document.getElementById('auth-menu-roles');
      if (rolesSlot) {
        rolesSlot.innerHTML = buttons
          ? buttons + '<div style="height:1px; background:rgba(148,163,184,0.18); margin:0.3rem 0.2rem;"></div>'
          : '';
      }
      if (buttonsDiv) buttonsDiv.innerHTML = '';
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
    const rolesSlot = document.getElementById('auth-menu-roles');
    if (rolesSlot) rolesSlot.innerHTML = '';
    closeAccountMenu();
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
