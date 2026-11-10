// =============================================================================
// NSW PSN — public client config
// =============================================================================
// Copy this file to config.js and replace each placeholder with the real value
// for your deployment. Every value here is sent to the browser, so don't put
// service-role keys or any other secret here. The one "key" below is
// intentionally public:
//
//   • SUPABASE_KEY  — the Supabase **anon** key. Row-Level Security on the
//                     Supabase side is what actually gates data access; the
//                     anon key only identifies the client.
//
// There is NO API key in this file any more. Pages authenticate to /api with a
// short-lived session token they request from POST /api/session/token
// (api-session.js); the backend issues one only to this site's own pages and
// binds it to the requesting browser. The backend's NSWPSN_API_KEY is for
// server-side callers (the Discord bot, rdio) and never reaches a browser.
//
// The Supabase **service role** key (admin-level) lives in the BACKEND's .env
// as SUPABASE_SERVICE_ROLE_KEY. It must NEVER appear in this file.
// =============================================================================

// Supabase project URL. Find it in Supabase → Project Settings → API.
const SUPABASE_URL = 'https://your-project.supabase.co';

// Supabase anon (public) key. Same Settings → API page, under "Project API keys".
const SUPABASE_KEY = 'your_supabase_anon_key_here';

// Where the live + map pages call /api/* endpoints. No trailing slash.
// Production typically points at a Cloudflare-fronted subdomain like
// https://api.example.com; local dev can use http://localhost:8000.
const API_BASE_URL = 'https://your-api-domain.example.com';
