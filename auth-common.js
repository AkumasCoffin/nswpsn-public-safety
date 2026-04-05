// ======== COMMON AUTH SYSTEM ========
// Include this script on any page that needs authentication
// Requires: Supabase client script loaded before this

// SUPABASE_URL, SUPABASE_KEY, and API_BASE_URL are provided by config.js (loaded before this script)

// Initialize Supabase client (only if not already initialized)
// Use window.sb if it exists (for pages that have their own client), otherwise create one
let sb;
if (window.sb) {
  sb = window.sb;
} else {
  sb = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);
  window.sb = sb;  // Make available globally for other scripts
}

// Inject auth section HTML into sidebar (only if sidebar-footer exists)
function injectAuthSection() {
  // Find sidebar footer or end of sidebar nav
  const sidebarFooter = document.querySelector('.sidebar-footer');
  if (!sidebarFooter) return;
  
  // Check if auth section already exists
  if (document.getElementById('auth-section')) return;
  
  // Create auth section HTML
  const authSection = document.createElement('div');
  authSection.id = 'auth-section';
  authSection.style.cssText = 'margin-top:1.5rem; border-top:1px solid rgba(148,163,184,0.2); padding-top:1rem;';
  authSection.innerHTML = `
    <div class="sidebar-section-label">Account</div>
    <div id="auth-logged-out">
      <a href="login.html" style="width:100%; padding:0.6rem 1rem; background:rgba(249,115,22,0.15); border:1px solid rgba(249,115,22,0.3); border-radius:8px; color:#f97316; font-size:0.85rem; font-weight:500; cursor:pointer; display:flex; align-items:center; justify-content:center; gap:0.5rem; font-family:inherit; text-decoration:none; box-sizing:border-box;">
        <i class="fas fa-sign-in-alt"></i> Login
      </a>
    </div>
    <div id="auth-logged-in" style="display:none;">
      <div style="padding:0.75rem; background:rgba(255,255,255,0.03); border-radius:8px; margin-bottom:0.75rem;">
        <div id="auth-user-email" style="font-size:0.8rem; color:#fff; font-weight:500; margin-bottom:0.25rem; word-break:break-all;"></div>
        <div style="font-size:0.7rem; color:#22c55e;">● Logged in</div>
      </div>
      <div id="auth-role-buttons" style="display:flex; flex-direction:column; gap:0.5rem; margin-bottom:0.75rem;"></div>
      <a href="change-password.html" style="display:flex; width:100%; padding:0.5rem; background:rgba(148,163,184,0.1); border:1px solid rgba(148,163,184,0.2); border-radius:6px; color:#94a3b8; font-size:0.8rem; cursor:pointer; align-items:center; justify-content:center; gap:0.4rem; font-family:inherit; text-decoration:none; margin-bottom:0.5rem; box-sizing:border-box;">
        <i class="fas fa-key"></i> Change Password
      </a>
      <button onclick="doLogout()" style="width:100%; padding:0.5rem; background:rgba(239,68,68,0.15); border:1px solid rgba(239,68,68,0.3); border-radius:6px; color:#ef4444; font-size:0.8rem; cursor:pointer; display:flex; align-items:center; justify-content:center; gap:0.4rem; font-family:inherit;">
        <i class="fas fa-sign-out-alt"></i> Logout
      </button>
    </div>
  `;
  
  // Insert before footer
  sidebarFooter.parentNode.insertBefore(authSection, sidebarFooter);
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
          <a href="editor-signup.html" style="display:block; width:100%; padding:0.8rem; background:transparent; border:1px solid #f97316; border-radius:8px; color:#f97316; font-weight:700; cursor:pointer; text-transform:uppercase; letter-spacing:0.05em; font-size:0.9rem; text-decoration:none; text-align:center; box-sizing:border-box; font-family:inherit; transition:all 0.2s;">Request Access</a>
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
    if (errorDiv) errorDiv.textContent = error.message;
    return;
  }
  
  // Check if user needs to change password on first login
  if (data?.user?.user_metadata?.force_password_change) {
    closeLoginModal();
    window.location.href = 'change-password.html';
    return;
  }
  
  closeLoginModal();
  checkAuthState();
}

async function doLogout() {
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
    if (emailDiv) emailDiv.textContent = session.user.email;
    
    // Fetch roles with retry logic
    const fetchRolesWithRetry = async (retries = 2) => {
      for (let attempt = 0; attempt <= retries; attempt++) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 8000); // 8 second timeout
          
          const roleCheck = await fetch(`${API_BASE_URL}/api/check-editor/${session.user.id}`, {
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
      
      // Map Editor button - for map_editor or owner
      if (roleData.is_map_editor || roleData.is_owner) {
        buttons += `<a href="map-editor.html" style="display:flex; align-items:center; gap:0.5rem; padding:0.5rem 0.75rem; background:rgba(59,130,246,0.15); border:1px solid rgba(59,130,246,0.3); border-radius:6px; color:#60a5fa; font-size:0.8rem; text-decoration:none;">
          <i class="fas fa-map-marked-alt"></i> Map Editor
        </a>`;
      }
      
      // User Management button - for team_member or owner
      if (roleData.is_team_member || roleData.is_owner) {
        buttons += `<a href="editor-requests.html" style="display:flex; align-items:center; gap:0.5rem; padding:0.5rem 0.75rem; background:rgba(249,115,22,0.15); border:1px solid rgba(249,115,22,0.3); border-radius:6px; color:#fb923c; font-size:0.8rem; text-decoration:none;">
          <i class="fas fa-users-cog"></i> User Management
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
