// Package localweb lets the node's owner listen to their own node from their
// home network. It is a reverse proxy in front of the local rdio-scanner, which
// itself stays bound to loopback: rdio serves its listener page, its admin page
// and its admin/upload API all on one port, and only the listener may leave the
// machine.
//
// The filter is an ALLOWLIST. Anything not named below gets a 404, so an admin
// route added to rdio later is blocked without anyone having to remember this
// file exists.
package localweb

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

// topLevelFile matches the webapp's own files at the root: main.<hash>.js,
// styles.<hash>.css, favicon.ico, manifest.webmanifest, ngsw.json and so on.
var topLevelFile = regexp.MustCompile(`^/[A-Za-z0-9._-]+\.[A-Za-z0-9]+$`)

// Server is the LAN-facing listen-only proxy.
type Server struct {
	addr  string
	proxy *httputil.ReverseProxy
	srv   *http.Server
	// port is the TCP port actually bound; 0 until Run has bound (and again
	// after it stops), so the status frame never advertises a dead link.
	port atomic.Int32
}

// New builds a Server listening on addr and proxying allowed requests to
// upstream (the local rdio, e.g. http://127.0.0.1:17391).
func New(addr, upstream string) (*Server, error) {
	u, err := url.Parse(upstream)
	if err != nil {
		return nil, err
	}
	s := &Server{addr: addr, proxy: httputil.NewSingleHostReverseProxy(u)}
	// The browser's Host passes through unchanged (NewSingleHostReverseProxy
	// leaves it alone) and rdio accepts any websocket Origin, so the websocket
	// upgrade needs nothing special: ReverseProxy forwards it by itself.
	s.proxy.ErrorLog = log.New(log.Writer(), "[nodeagent] localweb: ", log.LstdFlags|log.Lmsgprefix)
	s.srv = &http.Server{
		Handler:           http.HandlerFunc(s.serve),
		ReadHeaderTimeout: 15 * time.Second,
	}
	return s, nil
}

// Port is the bound TCP port, or 0 while not listening.
func (s *Server) Port() int { return int(s.port.Load()) }

// Host is the host part of the configured address ("" or 0.0.0.0 / :: for
// every interface).
func (s *Server) Host() string {
	h, _, err := net.SplitHostPort(s.addr)
	if err != nil {
		return ""
	}
	return h
}

// Run binds and serves until ctx is cancelled. A bind failure is returned
// straight away; unlike the relay it is not fatal to the agent, so the caller
// only logs it.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	if a, ok := ln.Addr().(*net.TCPAddr); ok {
		s.port.Store(int32(a.Port))
	}
	defer s.port.Store(0)
	log.Printf("localweb: listener page on http://%s", ln.Addr())

	errCh := make(chan error, 1)
	go func() { errCh <- s.srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.srv.Shutdown(shutCtx); err != nil {
			log.Printf("localweb: shutdown error: %v", err)
		}
		// Shutdown does not close hijacked (websocket) connections.
		_ = s.srv.Close()
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if !allowed(r) {
		http.NotFound(w, r)
		return
	}
	s.proxy.ServeHTTP(w, r)
}

// allowed is the whole policy: the listener page, its files, the public stats
// it reads, and its websocket. Nothing else reaches rdio.
func allowed(r *http.Request) bool {
	p := r.URL.Path
	// Only already-clean paths. Anything rdio's own mux would rewrite
	// (/assets/../api/admin/login) or that hides a separator behind an escape
	// (%2F) is refused rather than reasoned about.
	if r.URL.RawPath != "" || p == "" || path.Clean(p) != p {
		return false
	}

	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		// The page opens its websocket on its own URL: / or /stream.
		return r.Method == http.MethodGet && (p == "/" || p == "/stream")
	}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	switch {
	case p == "/", p == "/stream":
		return true
	// The page's public stats view. Read-only; the admin twins live under
	// /api/admin and stay blocked.
	case p == "/api/stats", p == "/api/stats/talkgroup-units":
		return true
	case strings.HasPrefix(p, "/api/"), p == "/api":
		return false
	case strings.HasPrefix(p, "/assets/"), strings.HasPrefix(p, "/plugins/"):
		return true
	case topLevelFile.MatchString(p):
		return true
	}
	return false
}
