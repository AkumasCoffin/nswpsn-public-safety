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

	// sampleInterval is how often aircraft.json is READ, as against how often a
	// snapshot is sent.
	//
	// An airborne transponder reports position about twice a second, but
	// aircraft.json only ever holds each aircraft's CURRENT state — the
	// positions between two reads are gone before anyone can ask for them. So
	// reading once per upload threw away four fifths of what the decoder had,
	// and the map drew five-second straight lines through turns the aircraft
	// actually flew. Reading every second and carrying the positions forward
	// costs one more file read per second from tmpfs and no extra requests.
	sampleInterval = time.Second

	// maxPositionAge matches the backend's own cutoff: it discards any position
	// older than this, so shipping them would be pure waste.
	maxPositionAge = 60.0

	// maxTrailPerAircraft bounds the carried positions per upload. Five reads
	// fit in an interval; the slack absorbs a slow read without letting a
	// stalled uploader accumulate without bound.
	maxTrailPerAircraft = 16

	// maxTrailPointsTotal bounds the carried positions across the WHOLE
	// snapshot, which is what the server's 256 KB body cap actually sees.
	//
	// At the 500-aircraft schema limit, six positions each would put the
	// payload within sight of that cap, and a snapshot refused with 413 is a
	// snapshot lost. Positions are also worth least exactly when there are most
	// of them: a map showing five hundred aircraft is not short of detail. So
	// the busiest sites shed this first and keep sending every position.
	maxTrailPointsTotal = 2000

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

	// Positions seen since the last upload, oldest first, as
	// [secondsBeforeAt, lat, lon, altFt]. Relative to the snapshot's own `at`
	// rather than absolute, so the backend's correction for this node's clock
	// applies to them exactly as it does to seen_pos.
	//
	// Omitted when the aircraft produced only the position already carried
	// above, which is the common case for anything parked or barely moving.
	Positions [][]*float64 `json:"positions,omitempty"`
}

type snapshotStats struct {
	MsgRate         float64  `json:"msgRate"`
	AircraftTotal   int      `json:"aircraftTotal"`
	AircraftWithPos int      `json:"aircraftWithPos"`
	TracksAll       int64    `json:"tracksAll"`
	MaxRangeKm      *float64 `json:"maxRangeKm,omitempty"`
	// Receive level over the decoder's last minute, in dBFS — negative, closer
	// to zero being stronger. dump1090 has always reported these and this agent
	// has always parsed them for its own gain loop; they were simply never put
	// on the wire, so the backend could not chart the one figure that says
	// whether an antenna or a gain setting is actually any good.
	SignalDbfs     *float64 `json:"signalDbfs,omitempty"`
	SignalPeakDbfs *float64 `json:"signalPeakDbfs,omitempty"`
}

type snapshotBody struct {
	At       string             `json:"at"`
	Aircraft []snapshotAircraft `json:"aircraft"`
	Stats    *snapshotStats     `json:"stats,omitempty"`
}

// trailPoint is one position, timed by the node's own clock.
type trailPoint struct {
	atMs int64
	lat  float64
	lon  float64
	alt  *float64
}

// positionTrail carries the positions seen between uploads.
//
// Keyed on WHEN THE POSITION WAS MEASURED, not when it was read: seen_pos gives
// the age of the fix, so the same fix read three times in a row resolves to one
// timestamp and collapses on its own. No comparison of coordinates is needed,
// and an aircraft genuinely holding still contributes one point rather than
// five identical ones.
type positionTrail struct {
	byHex map[string][]trailPoint
}

func newPositionTrail() *positionTrail {
	return &positionTrail{byHex: make(map[string][]trailPoint)}
}

func (t *positionTrail) observe(air *decoderjson.AircraftFile, now time.Time) {
	nowMs := now.UnixMilli()
	for i := range air.Aircraft {
		a := &air.Aircraft[i]
		if !a.HasPosition() || a.SeenPos == nil || *a.SeenPos > maxPositionAge {
			continue
		}
		// Rounded to a tenth, which is the resolution seen_pos is reported at.
		atMs := (nowMs - int64(*a.SeenPos*1000)) / 100 * 100
		buf := t.byHex[a.Hex]
		if n := len(buf); n > 0 && buf[n-1].atMs >= atMs {
			continue // same fix, or an older one after a decoder restart
		}
		if len(buf) >= maxTrailPerAircraft {
			buf = buf[1:]
		}
		var alt *float64
		// A surface position has no barometric altitude to carry; the point is
		// still worth keeping, it simply has no height.
		if v, onGround, ok := a.AltBaroValue(); ok && !onGround {
			alt = &v
		}
		t.byHex[a.Hex] = append(buf, trailPoint{atMs: atMs, lat: *a.Lat, lon: *a.Lon, alt: alt})
	}
}

// take returns the buffered points and empties the buffer.
func (t *positionTrail) take() map[string][]trailPoint {
	out := t.byHex
	t.byHex = make(map[string][]trailPoint)
	return out
}

// runSnapshotLoop polls the decoder's JSON output until ctx is cancelled.
func runSnapshotLoop(ctx context.Context, m *aircraftManager, q *queue.Queue) {
	// Two rates from one ticker: read every second, send every fifth read. The
	// reads are what give a track its shape; the sends are what cost requests.
	t := time.NewTicker(sampleInterval)
	defer t.Stop()
	readsPerUpload := int(snapshotInterval / sampleInterval)
	if readsPerUpload < 1 {
		readsPerUpload = 1
	}
	reads := 0
	trail := newPositionTrail()

	// warnedNotReady keeps the "decoder hasn't written anything yet" message to
	// one line per outage instead of one every 5 seconds.
	warnedNotReady := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		reads++
		upload := reads >= readsPerUpload
		if upload {
			reads = 0
		}

		air, err := m.reader.Aircraft()
		if err != nil {
			if errors.Is(err, decoderjson.ErrNotReady) {
				if !warnedNotReady {
					log.Printf("snapshot: decoder has not written aircraft.json yet (%s)", m.cfg.JSONDir)
					warnedNotReady = true
				}
			} else {
				// Only on an upload tick, or a decoder that is down logs this
				// once a second forever.
				if upload {
					log.Printf("snapshot: reading aircraft.json failed: %v", err)
				}
			}
			// No aircraft data means no usable heartbeat figures either.
			if upload {
				m.setStats(wsclient.AdsbStats{})
			}
			continue
		}
		if warnedNotReady {
			log.Printf("snapshot: decoder output is flowing")
			warnedNotReady = false
		}

		// Every read contributes to the carried positions; only the fifth
		// builds and sends a snapshot.
		trail.observe(air, time.Now())
		if !upload {
			continue
		}

		// stats.json is optional: its absence degrades the figures but must
		// never stop positions being shipped.
		stats, statsErr := m.reader.Stats()
		if statsErr != nil && !errors.Is(statsErr, decoderjson.ErrNotReady) {
			log.Printf("snapshot: reading stats.json failed: %v", statsErr)
		}

		now := time.Now()
		list := buildAircraft(air)
		attachTrails(list, trail.take(), now)
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

// attachTrails hangs each aircraft's carried positions off its record.
//
// Ages are relative to the snapshot's `at`, matching seen_pos, so the backend's
// correction for this node's clock covers them without knowing they exist. A
// lone point is dropped: it is the position already on the record, and sending
// it again would be payload for nothing — which matters, since this is the part
// of the upload that can grow with traffic.
func attachTrails(list []snapshotAircraft, byHex map[string][]trailPoint, at time.Time) {
	atMs := at.UnixMilli()
	budget := maxTrailPointsTotal
	for i := range list {
		pts := byHex[list[i].Hex]
		if len(pts) < 2 || len(pts) > budget {
			continue
		}
		budget -= len(pts)
		out := make([][]*float64, 0, len(pts))
		for _, p := range pts {
			age := float64(atMs-p.atMs) / 1000
			if age < 0 {
				age = 0
			}
			lat, lon := p.lat, p.lon
			out = append(out, []*float64{&age, &lat, &lon, p.alt})
		}
		list[i].Positions = out
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
		// Signal, unlike range, is deliberately the LAST MINUTE: it is a
		// measure of current conditions, and a run-total mean would flatten out
		// exactly the change an operator is watching for after moving an
		// antenna or altering gain.
		SignalDbfs:     signalMean(s),
		SignalPeakDbfs: signalPeak(s),
	}
}

// signalMean and signalPeak read the last minute's receive level, or nil.
//
// `local` is a POINTER to an optional block: dump1090 omits it entirely when
// it has nothing to report, and a decoder that is not dump1090 may never write
// it at all. Reaching through it unguarded panics the agent on a field that is
// only ever nice to have.
func signalMean(s *decoderjson.StatsFile) *float64 {
	if s == nil || s.Last1Min.Local == nil {
		return nil
	}
	return s.Last1Min.Local.SignalStrength
}

func signalPeak(s *decoderjson.StatsFile) *float64 {
	if s == nil || s.Last1Min.Local == nil {
		return nil
	}
	return s.Last1Min.Local.PeakSignal
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
