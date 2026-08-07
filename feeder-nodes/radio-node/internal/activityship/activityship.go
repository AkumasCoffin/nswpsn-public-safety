// Package activityship ships decode-activity events from the local
// sdrtrunk-vce control server to the backend. Every ~4s it fetches
// /activity/events after a persisted cursor, POSTs any new events to
// ${server_url}/api/node-ingest/activity in one batch, and advances the cursor
// only after the backend accepts (2xx) — at-least-once delivery; the backend
// dedupes on (streamId, event id).
//
// The cursor is bound to a specific generation of the vce SQLite database via
// a stable "dbKey" (Linux: device+inode; Windows: file creation time). When
// the database is recreated — dbKey changes, or a fetch's lastId regresses
// below the persisted cursor — the shipper starts a fresh random streamId at
// lastId=0 so the backend can distinguish the restarted id sequence.
package activityship

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/version"
)

const (
	// fetchLimit is the control server's per-request event cap.
	fetchLimit = 500
	// maxDrainRounds bounds how many back-to-back full fetches a single tick
	// performs when catching up (e.g. after backend downtime).
	maxDrainRounds = 5
	// maxBodyBytes is the backend's per-request body cap. A full 500-event
	// batch serializes well under this, but split defensively if ever exceeded.
	maxBodyBytes = 256 * 1024
	// baseInterval / intervalJitter: tick every 4s +/- 1s.
	baseInterval   = 3 * time.Second
	intervalJitter = 2 * time.Second
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

// cursorFile is the persisted cursor at <DataDir>/activity-cursor.json.
type cursorFile struct {
	StreamID string `json:"streamId"`
	LastID   int64  `json:"lastId"`
	DBKey    string `json:"dbKey"`
}

// FetchFunc fetches events after sinceID (up to limit) from the control
// server, returning the events and the response envelope's lastId.
type FetchFunc func(sinceID int64, limit int) ([]sdrctl.ActivityEvent, int64, error)

// Options configures a Shipper.
type Options struct {
	// DataDir is the agent data directory holding activity-cursor.json.
	DataDir string
	// DBPath is the vce SQLite database whose identity (dbKey) binds the
	// cursor to one database generation.
	DBPath string
	// Fetch pulls events from the control server (sdrctl.Client.ActivityEvents).
	Fetch FetchFunc
	// ServerURL + NodeToken + InstallID are the backend base URL and the
	// node-ingest auth headers.
	ServerURL string
	NodeToken string
	InstallID string
}

// Shipper is the activity-event shipping loop. Not safe for concurrent use;
// Run owns all state.
type Shipper struct {
	opts Options
	hc   *http.Client

	st cursorFile

	// shippedOnce gates the one INFO line on the first successful ship after a
	// (re)connect to the control server; cleared when a fetch fails.
	shippedOnce bool
	lastWarn    time.Time
	authUntil   time.Time
}

// New builds a Shipper. Call Run to start it.
func New(opts Options) *Shipper {
	return &Shipper{
		opts: opts,
		hc:   &http.Client{Timeout: shipTimeout},
	}
}

// cursorPath is the persisted-cursor file location.
func (s *Shipper) cursorPath() string {
	return filepath.Join(s.opts.DataDir, "activity-cursor.json")
}

// Run loads the cursor and ticks until ctx is cancelled.
func (s *Shipper) Run(ctx context.Context) {
	if err := s.loadState(); err != nil {
		log.Printf("activity: cursor load failed (%v); starting a fresh stream", err)
	}
	if s.st.StreamID == "" {
		dbKey, _ := statDBKey(s.opts.DBPath)
		if err := s.resetStream(dbKey); err != nil {
			log.Printf("activity: init cursor failed: %v", err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(baseInterval + randDuration(intervalJitter)):
		}
		s.tick(ctx)
	}
}

// tick performs one poll round: reconcile the dbKey, then fetch+ship, draining
// full batches up to maxDrainRounds.
func (s *Shipper) tick(ctx context.Context) {
	if key, ok := statDBKey(s.opts.DBPath); !s.reconcileDBKey(key, ok) {
		s.warnf("activity: cursor reset failed after database change")
		return
	}
	if time.Now().Before(s.authUntil) {
		return
	}

	for round := 0; round < maxDrainRounds; round++ {
		if ctx.Err() != nil {
			return
		}
		events, lastID, err := s.fetch()
		if err != nil {
			// Control server down/starting is a normal condition — stay silent
			// and re-arm the first-ship INFO for the next connect.
			s.shippedOnce = false
			return
		}
		if lastID < s.st.LastID {
			// Ids restarted underneath us (DB recreated between dbKey checks).
			log.Printf("activity: event ids restarted (lastId %d < cursor %d); starting a fresh stream", lastID, s.st.LastID)
			// Re-stat so the fresh stream binds to the NEW database generation.
			dbKey := s.st.DBKey
			if key, ok := statDBKey(s.opts.DBPath); ok {
				dbKey = key
			}
			if err := s.resetStream(dbKey); err != nil {
				s.warnf("activity: cursor reset failed: %v", err)
				return
			}
			continue // re-fetch from 0 this tick
		}
		if len(events) == 0 {
			return // nothing new
		}

		if err := s.ship(ctx, events); err != nil {
			if errors.Is(err, errAuth) {
				s.authUntil = time.Now().Add(authCooldown)
			}
			// Keep the cursor: next tick re-fetches the same rows.
			s.warnf("activity: ship failed (will retry): %v", err)
			return
		}

		// Advance to the highest shipped id (never past what we actually sent)
		// and persist only after the backend accepted.
		s.st.LastID = events[len(events)-1].ID
		if err := s.persistState(); err != nil {
			s.warnf("activity: cursor persist failed: %v", err)
		}
		if !s.shippedOnce {
			log.Printf("activity: shipping from id %d", s.st.LastID)
			s.shippedOnce = true
		}
		if len(events) < fetchLimit {
			return // drained
		}
	}
}

// fetch pulls the next batch after the cursor.
func (s *Shipper) fetch() ([]sdrctl.ActivityEvent, int64, error) {
	return s.opts.Fetch(s.st.LastID, fetchLimit)
}

// ship POSTs one batch to the backend, splitting recursively in the (never
// expected) case the serialized body exceeds the backend's size cap. A nil
// return means every event in the batch got a 2xx.
func (s *Shipper) ship(ctx context.Context, events []sdrctl.ActivityEvent) error {
	body, err := json.Marshal(map[string]any{
		"streamId": s.st.StreamID,
		"events":   events,
	})
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}
	if len(body) > maxBodyBytes && len(events) > 1 {
		half := len(events) / 2
		if err := s.ship(ctx, events[:half]); err != nil {
			return err
		}
		return s.ship(ctx, events[half:])
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.opts.ServerURL+"/api/node-ingest/activity", bytes.NewReader(body))
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

// ---- cursor state ------------------------------------------------------------

// loadState reads the persisted cursor. A missing file leaves the zero state
// (Run then initializes a fresh stream); a corrupt file is an error.
func (s *Shipper) loadState() error {
	raw, err := os.ReadFile(s.cursorPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var st cursorFile
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("parse %s: %w", s.cursorPath(), err)
	}
	s.st = st
	return nil
}

// persistState atomically (temp + rename) writes the cursor.
func (s *Shipper) persistState() error {
	b, err := json.Marshal(s.st)
	if err != nil {
		return err
	}
	path := s.cursorPath()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// reconcileDBKey resets the stream when a successfully-observed dbKey differs
// from the persisted one (database recreated). Unknown observations (ok=false,
// i.e. the DB file isn't statable yet) are ignored — absence is not a
// generation change. It reports whether the cursor state is usable afterwards
// (false only when a needed reset failed to persist).
func (s *Shipper) reconcileDBKey(key string, ok bool) bool {
	if !ok || key == s.st.DBKey {
		return true
	}
	log.Printf("activity: sdrtrunk database changed; starting a fresh stream")
	return s.resetStream(key) == nil
}

// resetStream starts a fresh stream: new random streamId, lastId=0, the given
// dbKey, persisted immediately.
func (s *Shipper) resetStream(dbKey string) error {
	id, err := randomHex16()
	if err != nil {
		return fmt.Errorf("generate streamId: %w", err)
	}
	s.st = cursorFile{StreamID: id, LastID: 0, DBKey: dbKey}
	return s.persistState()
}

// warnf logs a WARN line at most once per warnEvery.
func (s *Shipper) warnf(format string, args ...any) {
	if time.Since(s.lastWarn) < warnEvery {
		return
	}
	s.lastWarn = time.Now()
	log.Printf("WARN: "+format, args...)
}

// randomHex16 returns 16 random bytes hex-encoded (32 chars).
func randomHex16() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
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
