// Package siteship ships P25 site snapshots from the local sdrtrunk-vce control
// server to the backend. Every ~60s it GETs the control server's /site/snapshots
// (which returns {"sites":[…]}) and POSTs that sites array to
// ${server_url}/api/node-ingest/site-snapshots with the node-ingest auth headers.
//
// Unlike the activity shipper there is no cursor: site metadata changes slowly
// and each poll is a full-snapshot replace — the backend UPSERTs on
// (node, system, rfss, site), so re-posting the same set is idempotent. The POST
// is skipped when the list is empty. Failures are fire-and-forget (logged, then
// retried next tick) exactly like the activity shipper: a control server that is
// down or still starting is a normal, silent condition.
package siteship

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/version"
)

const (
	// pollInterval / intervalJitter: tick every 60s +/- 5s. Site metadata
	// changes slowly, so a slow cadence is plenty and matches the backend's
	// per-node rate limit expectations.
	pollInterval   = 60 * time.Second
	intervalJitter = 10 * time.Second
	// warnEvery rate-limits the persistent-failure WARN log.
	warnEvery = time.Minute
	// authCooldown pauses shipping after a 401/403 so a bad/revoked node token
	// doesn't hammer the backend every tick.
	authCooldown = time.Minute
	// shipTimeout bounds one backend POST.
	shipTimeout = 30 * time.Second
)

// errAuth marks a backend 401/403 (auth problem — futile to retry hot).
var errAuth = errors.New("backend rejected node auth")

// FetchFunc fetches the current site snapshots from the control server,
// returning the raw JSON objects (sdrctl.Client.SiteSnapshots).
type FetchFunc func() ([]json.RawMessage, error)

// Options configures a Shipper.
type Options struct {
	// Fetch pulls the site snapshots from the control server.
	Fetch FetchFunc
	// ServerURL + NodeToken + InstallID are the backend base URL and the
	// node-ingest auth headers.
	ServerURL string
	NodeToken string
	InstallID string
}

// Shipper is the site-snapshot shipping loop. Not safe for concurrent use;
// Run owns all state.
type Shipper struct {
	opts Options
	hc   *http.Client

	// postedOnce gates the one INFO line on the first successful ship after a
	// (re)connect to the control server; cleared when a fetch fails.
	postedOnce bool
	lastWarn   time.Time
	authUntil  time.Time
}

// New builds a Shipper. Call Run to start it.
func New(opts Options) *Shipper {
	return &Shipper{
		opts: opts,
		hc:   &http.Client{Timeout: shipTimeout},
	}
}

// Run ticks until ctx is cancelled.
func (s *Shipper) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval + randDuration(intervalJitter)):
		}
		s.tick(ctx)
	}
}

// tick performs one poll round: fetch the current snapshots and, if non-empty,
// ship them.
func (s *Shipper) tick(ctx context.Context) {
	if time.Now().Before(s.authUntil) {
		return
	}
	sites, err := s.opts.Fetch()
	if err != nil {
		// Control server down/starting or endpoint absent — a normal condition.
		// Stay silent and re-arm the first-ship INFO for the next connect.
		s.postedOnce = false
		return
	}
	if len(sites) == 0 {
		return // nothing to ship — skip the POST
	}
	if err := s.ship(ctx, sites); err != nil {
		if errors.Is(err, errAuth) {
			s.authUntil = time.Now().Add(authCooldown)
		}
		s.warnf("site: ship failed (will retry): %v", err)
		return
	}
	if !s.postedOnce {
		log.Printf("site: shipping %d site snapshot(s)", len(sites))
		s.postedOnce = true
	}
}

// ship POSTs the sites array to the backend. A nil return means the backend
// accepted the batch (2xx).
func (s *Shipper) ship(ctx context.Context, sites []json.RawMessage) error {
	body, err := json.Marshal(sites)
	if err != nil {
		return fmt.Errorf("marshal sites: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.opts.ServerURL+"/api/node-ingest/site-snapshots", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Node-Token", s.opts.NodeToken)
	req.Header.Set("X-Node-Install", s.opts.InstallID)
	req.Header.Set("User-Agent", version.UserAgent())

	resp, err := s.hc.Do(req)
	if err != nil {
		return err
	}
	// Drain before close so the keep-alive connection is reused.
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return fmt.Errorf("%w (status %d)", errAuth, resp.StatusCode)
	default:
		return fmt.Errorf("backend returned %d", resp.StatusCode)
	}
}

// warnf logs a WARN line at most once per warnEvery.
func (s *Shipper) warnf(format string, args ...any) {
	if time.Since(s.lastWarn) < warnEvery {
		return
	}
	s.lastWarn = time.Now()
	log.Printf("WARN: "+format, args...)
}

// randDuration returns a uniformly random duration in [0, max) using
// crypto/rand (matching the codebase's no-math/rand convention).
func randDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return max / 2
	}
	return time.Duration(binary.BigEndian.Uint64(b[:]) % uint64(max))
}
