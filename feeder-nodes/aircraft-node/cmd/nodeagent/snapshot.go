package main

// The snapshot loop: read what the decoder can currently see, queue it for
// upload, and keep the heartbeat figures current.
//
// Every upload SUPERSEDES the previous one — a snapshot is a complete
// restatement of this receiver's view, not a delta — which is what lets the
// queue drop stale entries on age alone without any reconciliation.

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/decoderjson"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/queue"
	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/wsclient"
)

const (
	// snapshotInterval is how often the receiver reports.
	//
	// Three times fresher than the public aggregators' ~15s effective cadence,
	// which is much of the point of running our own hardware. Well inside the
	// queue's 90s age bound, so a network blip of ~18 snapshots still drains
	// rather than expiring. And 12 uploads/min, comfortably under the server's
	// 20/min limiter.
	snapshotInterval = 5 * time.Second

	// maxPositionAge matches the backend's own cutoff: it discards any position
	// older than this, so shipping them would be pure waste.
	maxPositionAge = 60.0

	// maxAircraftPerSnapshot matches the server's schema cap. Reaching it would
	// take an extraordinarily busy site; truncating is still better than having
	// the whole snapshot rejected as malformed.
	maxAircraftPerSnapshot = 500
)

// snapshotAircraft is one aircraft on the wire. Field names deliberately mirror
// dump1090/readsb so the backend can feed them straight through the same
// normaliser it uses for the public aggregators.
type snapshotAircraft struct {
	Hex      string   `json:"hex"`
	Flight   string   `json:"flight,omitempty"`
	Lat      float64  `json:"lat"`
	Lon      float64  `json:"lon"`
	AltBaro  any      `json:"alt_baro,omitempty"`
	GS       *float64 `json:"gs,omitempty"`
	Track    *float64 `json:"track,omitempty"`
	Squawk   string   `json:"squawk,omitempty"`
	Emerg    string   `json:"emergency,omitempty"`
	Category string   `json:"category,omitempty"`
	SeenPos  float64  `json:"seen_pos"`
}

type snapshotStats struct {
	MsgRate         float64  `json:"msgRate"`
	AircraftTotal   int      `json:"aircraftTotal"`
	AircraftWithPos int      `json:"aircraftWithPos"`
	TracksAll       int64    `json:"tracksAll"`
	MaxRangeKm      *float64 `json:"maxRangeKm,omitempty"`
}

type snapshotBody struct {
	At       string             `json:"at"`
	Aircraft []snapshotAircraft `json:"aircraft"`
	Stats    *snapshotStats     `json:"stats,omitempty"`
}

// runSnapshotLoop polls the decoder's JSON output until ctx is cancelled.
func runSnapshotLoop(ctx context.Context, m *aircraftManager, q *queue.Queue) {
	t := time.NewTicker(snapshotInterval)
	defer t.Stop()

	// warnedNotReady keeps the "decoder hasn't written anything yet" message to
	// one line per outage instead of one every 5 seconds.
	warnedNotReady := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		air, err := m.reader.Aircraft()
		if err != nil {
			if errors.Is(err, decoderjson.ErrNotReady) {
				if !warnedNotReady {
					log.Printf("snapshot: decoder has not written aircraft.json yet (%s)", m.cfg.JSONDir)
					warnedNotReady = true
				}
			} else {
				log.Printf("snapshot: reading aircraft.json failed: %v", err)
			}
			// No aircraft data means no usable heartbeat figures either.
			m.setStats(wsclient.AdsbStats{})
			continue
		}
		if warnedNotReady {
			log.Printf("snapshot: decoder output is flowing")
			warnedNotReady = false
		}

		// stats.json is optional: its absence degrades the figures but must
		// never stop positions being shipped.
		stats, statsErr := m.reader.Stats()
		if statsErr != nil && !errors.Is(statsErr, decoderjson.ErrNotReady) {
			log.Printf("snapshot: reading stats.json failed: %v", statsErr)
		}

		now := time.Now()
		list := buildAircraft(air)
		body := snapshotBody{
			At:       now.UTC().Format(time.RFC3339),
			Aircraft: list,
			Stats:    buildStats(air, stats, len(list)),
		}

		updateHeartbeat(m, len(list), stats)

		if stats != nil {
			// Adaptive gain runs off the same statistics, so it costs no extra
			// read. It decides at most once per its own interval.
			m.considerGain(&stats.Last1Min, now)
		}

		// Nothing to say: the heartbeat already carries the aircraft count, so
		// an empty snapshot would add nothing while churning the queue all
		// night at a quiet site.
		if len(list) == 0 {
			continue
		}
		if !m.feedEnabled() {
			// Feed paused: keep decoding and reporting health, ship nothing.
			continue
		}

		enc, err := json.Marshal(body)
		if err != nil {
			log.Printf("snapshot: marshal failed: %v", err)
			continue
		}
		if err := q.Enqueue("application/json", enc); err != nil {
			log.Printf("snapshot: enqueue failed: %v", err)
		}
	}
}

// buildAircraft filters the decoder's table down to what the backend can use:
// records with a position fresh enough to still be accepted.
func buildAircraft(air *decoderjson.AircraftFile) []snapshotAircraft {
	out := make([]snapshotAircraft, 0, len(air.Aircraft))
	for i := range air.Aircraft {
		a := &air.Aircraft[i]
		if !a.HasPosition() || *a.SeenPos > maxPositionAge {
			continue
		}
		rec := snapshotAircraft{
			Hex:      a.Hex,
			Flight:   trimSpace(a.Flag),
			Lat:      *a.Lat,
			Lon:      *a.Lon,
			GS:       a.GS,
			Track:    a.Track,
			Squawk:   a.Squawk,
			Emerg:    a.Emerg,
			Category: a.Category,
			SeenPos:  *a.SeenPos,
		}
		if alt, onGround, ok := a.AltBaroValue(); ok {
			if onGround {
				rec.AltBaro = "ground"
			} else {
				rec.AltBaro = alt
			}
		}
		out = append(out, rec)
		if len(out) >= maxAircraftPerSnapshot {
			log.Printf("snapshot: truncated at %d aircraft", maxAircraftPerSnapshot)
			break
		}
	}
	return out
}

func buildStats(air *decoderjson.AircraftFile, s *decoderjson.StatsFile, withPos int) *snapshotStats {
	if s == nil {
		return &snapshotStats{
			AircraftTotal:   len(air.Aircraft),
			AircraftWithPos: withPos,
		}
	}
	return &snapshotStats{
		MsgRate:         s.Last1Min.MsgRate(),
		AircraftTotal:   len(air.Aircraft),
		AircraftWithPos: withPos,
		TracksAll:       tracksAll(s),
		// Range over the whole run, not the last minute: the daily record is
		// "how far did this receiver reach", and a quiet minute must not
		// under-report it.
		MaxRangeKm: s.Total.MaxRangeKm(),
	}
}

func tracksAll(s *decoderjson.StatsFile) int64 {
	if s.Total.Tracks == nil {
		return 0
	}
	return s.Total.Tracks.All
}

// updateHeartbeat refreshes the figures the WS status frame reports.
func updateHeartbeat(m *aircraftManager, aircraftNow int, s *decoderjson.StatsFile) {
	out := wsclient.AdsbStats{AircraftNow: &aircraftNow}
	if s != nil {
		rate := s.Last1Min.MsgRate()
		out.MsgRate = &rate
		out.MaxRangeKm = s.Total.MaxRangeKm()
	}
	m.mu.Lock()
	if m.gain != nil {
		g := m.gain.Gain()
		out.GainNow = &g
	}
	m.mu.Unlock()
	m.setStats(out)
}

func trimSpace(s string) string {
	// dump1090 pads callsigns to 8 characters.
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	return s
}
