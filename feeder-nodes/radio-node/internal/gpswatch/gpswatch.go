// Package gpswatch watches the local sdrtrunk-vce control server for decoded
// radio positions (its /gps endpoint) and reports them in the agent's status
// frame.
//
// This exists to find out whether the network sends positions a node can
// decode at all. So it only watches and reports: each new position is written
// to the agent log, and the status frame carries the node's counts since
// sdrtrunk started plus the most recent positions. Nothing is posted to the
// backend's ingest routes and nothing is kept on disk.
//
// A runtime that predates /gps answers 404. That is a normal condition, not
// an error: the watcher then checks back rarely and reports Supported=false.
package gpswatch

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
)

const (
	pollInterval = 15 * time.Second
	// unsupportedInterval: how often to look again once the runtime has said
	// it has no /gps (it may be updated under us).
	unsupportedInterval = 10 * time.Minute
	// fetchLimit bounds one poll; a backlog is drained over several.
	fetchLimit = 500
	// recentKept is how many positions the status frame carries. The frame is
	// sent every few seconds, so this stays small.
	recentKept = 25
	// logBurst caps log lines per poll so a backlog cannot flood the log.
	logBurst = 20
)

// Options configures a Watcher.
type Options struct {
	// Fetch returns the positions after a sequence number, with the buffer's
	// summary. Normally (*sdrctl.Client).GPS.
	Fetch func(after int64, limit int) (sdrctl.GPSResponse, error)
}

// Snapshot is what the status frame carries under "gps".
type Snapshot struct {
	// Supported is false while the sdrtrunk runtime has no /gps endpoint.
	Supported bool `json:"supported"`
	// Positions and Invalid count since sdrtrunk started: usable positions,
	// and ones a radio sent without a fix (0/0).
	Positions int64 `json:"positions"`
	Invalid   int64 `json:"invalid"`
	// LastAtMs is when the newest usable position was decoded, 0 for none.
	LastAtMs int64 `json:"lastAtMs"`
	// SinceMs is when sdrtrunk's position buffer started counting.
	SinceMs int64 `json:"sinceMs"`
	// Recent holds the newest positions, oldest first.
	Recent []sdrctl.GPSPosition `json:"recent"`
}

// Watcher polls the control server for positions.
type Watcher struct {
	opts Options

	mu        sync.Mutex
	snap      Snapshot
	cursor    int64
	startedAt int64
}

// New returns a Watcher. Call Run to start it.
func New(opts Options) *Watcher {
	return &Watcher{opts: opts, snap: Snapshot{Recent: []sdrctl.GPSPosition{}}}
}

// Snapshot returns the current state for the status frame.
func (w *Watcher) Snapshot() any {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.snap
	s.Recent = append([]sdrctl.GPSPosition{}, w.snap.Recent...)
	return s
}

// Run polls until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context) {
	wait := pollInterval
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = pollInterval
		if !w.tick() {
			wait = unsupportedInterval
		}
	}
}

// tick does one poll. It returns false when the runtime has no /gps.
func (w *Watcher) tick() bool {
	w.mu.Lock()
	after := w.cursor
	w.mu.Unlock()

	resp, err := w.opts.Fetch(after, fetchLimit)
	if err != nil {
		if strings.Contains(err.Error(), "status 404") {
			w.mu.Lock()
			w.snap.Supported = false
			w.mu.Unlock()
			return false
		}
		// sdrtrunk down or starting: silent, like the other shippers.
		return true
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.snap.Supported = true
	// A new start time means sdrtrunk restarted and its sequence began again
	// at 1. What we asked for (everything after the old cursor) is not the
	// new run's positions, so drop it and ask again from the start.
	if resp.Summary.StartedAtMs != w.startedAt {
		w.startedAt = resp.Summary.StartedAtMs
		if after != 0 {
			w.cursor = 0
			w.snap.Recent = w.snap.Recent[:0]
			return true
		}
	}
	w.snap.Positions = resp.Summary.LastSeq
	w.snap.Invalid = resp.Summary.Invalid
	w.snap.LastAtMs = resp.Summary.LastAtMs
	w.snap.SinceMs = resp.Summary.StartedAtMs
	for i, p := range resp.Positions {
		if p.Seq > w.cursor {
			w.cursor = p.Seq
		}
		w.snap.Recent = append(w.snap.Recent, p)
		if i < logBurst {
			log.Printf("gps: position %.5f,%.5f from %s to %s via %s (%s)",
				p.Lat, p.Lon, orDash(p.From), orDash(p.To), orDash(p.Protocol), orDash(p.Channel))
		}
	}
	if n := len(resp.Positions); n > logBurst {
		log.Printf("gps: %d more positions in this poll, not listed", n-logBurst)
	}
	if n := len(w.snap.Recent); n > recentKept {
		w.snap.Recent = append([]sdrctl.GPSPosition{}, w.snap.Recent[n-recentKept:]...)
	}
	return true
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
