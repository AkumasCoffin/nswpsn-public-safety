package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/decoderjson"
)

// The positions between two uploads used to be thrown away. aircraft.json holds
// only each aircraft's CURRENT state, so reading it once per five-second upload
// discarded four fifths of what the decoder had and the map drew straight lines
// through turns that were actually flown.

func ac(hex string, lat, lon, seenPos float64) decoderjson.Aircraft {
	sp := seenPos
	la, lo := lat, lon
	return decoderjson.Aircraft{
		Hex: hex, Lat: &la, Lon: &lo, SeenPos: &sp,
		AltBaro: json.RawMessage("3000"),
	}
}

func file(list ...decoderjson.Aircraft) *decoderjson.AircraftFile {
	return &decoderjson.AircraftFile{Aircraft: list}
}

func TestCarriesEachFixOnce(t *testing.T) {
	// The dedupe keys on WHEN THE FIX WAS TAKEN, not on when it was read, so
	// the same position read repeatedly resolves to one timestamp and collapses
	// without comparing any coordinates.
	tr := newPositionTrail()
	base := time.Unix(1_800_000_000, 0)

	// Three reads a second apart, but the decoder's fix is ageing in step —
	// it is one position, seen three times.
	tr.observe(file(ac("abc123", -33.0, 151.0, 0.2)), base)
	tr.observe(file(ac("abc123", -33.0, 151.0, 1.2)), base.Add(time.Second))
	tr.observe(file(ac("abc123", -33.0, 151.0, 2.2)), base.Add(2*time.Second))

	if got := len(tr.byHex["abc123"]); got != 1 {
		t.Fatalf("one fix seen three times became %d points", got)
	}
}

func TestCarriesEveryNewFix(t *testing.T) {
	// A moving aircraft: each read brings a fresh fix, and all of them matter —
	// this is the whole reason for reading faster than we upload.
	tr := newPositionTrail()
	base := time.Unix(1_800_000_000, 0)
	for i := 0; i < 5; i++ {
		tr.observe(
			file(ac("abc123", -33.0+float64(i)*0.01, 151.0, 0.2)),
			base.Add(time.Duration(i)*time.Second),
		)
	}
	if got := len(tr.byHex["abc123"]); got != 5 {
		t.Fatalf("five distinct fixes became %d points", got)
	}
}

func TestIgnoresAFixOlderThanTheLast(t *testing.T) {
	// A decoder restart can reset seen_pos so the "new" fix is older than one
	// already carried. Taking it would send the track backwards.
	tr := newPositionTrail()
	base := time.Unix(1_800_000_000, 0)
	tr.observe(file(ac("abc123", -33.0, 151.0, 0.2)), base)
	tr.observe(file(ac("abc123", -34.0, 152.0, 30.0)), base.Add(time.Second))
	if got := len(tr.byHex["abc123"]); got != 1 {
		t.Fatalf("an older fix was accepted: %d points", got)
	}
}

func TestSkipsStaleAndPositionlessAircraft(t *testing.T) {
	tr := newPositionTrail()
	now := time.Unix(1_800_000_000, 0)
	stale := ac("aaa111", -33, 151, maxPositionAge+1)
	noPos := decoderjson.Aircraft{Hex: "bbb222"}
	tr.observe(file(stale, noPos), now)
	if len(tr.byHex) != 0 {
		t.Fatalf("buffered something unusable: %v", tr.byHex)
	}
}

func TestBoundsPointsPerAircraft(t *testing.T) {
	// A stalled uploader must not let one aircraft accumulate without bound.
	tr := newPositionTrail()
	base := time.Unix(1_800_000_000, 0)
	for i := 0; i < maxTrailPerAircraft*3; i++ {
		tr.observe(
			file(ac("abc123", -33.0+float64(i)*0.001, 151.0, 0.2)),
			base.Add(time.Duration(i)*time.Second),
		)
	}
	if got := len(tr.byHex["abc123"]); got != maxTrailPerAircraft {
		t.Fatalf("points per aircraft = %d, want the cap %d", got, maxTrailPerAircraft)
	}
	// The cap keeps the NEWEST points: the oldest are the ones already least
	// useful by the time they are sent.
	pts := tr.byHex["abc123"]
	if pts[len(pts)-1].atMs <= pts[0].atMs {
		t.Fatal("points are not oldest-first")
	}
}

func TestTakeEmptiesTheBuffer(t *testing.T) {
	tr := newPositionTrail()
	now := time.Unix(1_800_000_000, 0)
	tr.observe(file(ac("abc123", -33, 151, 0.2)), now)
	if len(tr.take()) != 1 {
		t.Fatal("take returned nothing")
	}
	if len(tr.byHex) != 0 {
		t.Fatal("take left the buffer populated; the next upload would resend")
	}
}

func TestAttachTrailsUsesAgesRelativeToTheSnapshot(t *testing.T) {
	// Relative, not absolute, so the backend's correction for this node's clock
	// covers the carried positions without knowing they exist.
	at := time.Unix(1_800_000_000, 0)
	list := []snapshotAircraft{{Hex: "abc123"}}
	pts := []trailPoint{
		{atMs: at.Add(-4 * time.Second).UnixMilli(), lat: -33.0, lon: 151.0},
		{atMs: at.Add(-2 * time.Second).UnixMilli(), lat: -33.1, lon: 151.1},
	}
	attachTrails(list, map[string][]trailPoint{"abc123": pts}, at)

	if len(list[0].Positions) != 2 {
		t.Fatalf("attached %d positions", len(list[0].Positions))
	}
	if got := *list[0].Positions[0][0]; got != 4 {
		t.Fatalf("oldest age = %v, want 4 seconds before `at`", got)
	}
	if got := *list[0].Positions[1][0]; got != 2 {
		t.Fatalf("newest age = %v, want 2", got)
	}
}

func TestAttachTrailsSkipsALonePoint(t *testing.T) {
	// One point is the position already on the record; resending it is payload
	// for nothing, and this is the part of the upload that grows with traffic.
	at := time.Unix(1_800_000_000, 0)
	list := []snapshotAircraft{{Hex: "abc123"}}
	attachTrails(list, map[string][]trailPoint{
		"abc123": {{atMs: at.UnixMilli(), lat: -33, lon: 151}},
	}, at)
	if list[0].Positions != nil {
		t.Fatalf("a lone point was attached: %v", list[0].Positions)
	}
}

func TestAttachedPositionsMarshalAsCompactArrays(t *testing.T) {
	at := time.Unix(1_800_000_000, 0)
	list := []snapshotAircraft{{Hex: "abc123"}}
	attachTrails(list, map[string][]trailPoint{"abc123": {
		{atMs: at.Add(-2 * time.Second).UnixMilli(), lat: -33.0, lon: 151.0},
		{atMs: at.Add(-1 * time.Second).UnixMilli(), lat: -33.1, lon: 151.1},
	}}, at)

	enc, err := json.Marshal(list[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back struct {
		Positions [][]*float64 `json:"positions"`
	}
	if err := json.Unmarshal(enc, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(back.Positions) != 2 || len(back.Positions[0]) != 4 {
		t.Fatalf("wire shape is not [age,lat,lon,alt]: %s", enc)
	}
	// Altitude is nullable and must survive as null rather than 0 — sea level
	// is a real altitude.
	if back.Positions[0][3] != nil {
		t.Fatalf("absent altitude became %v", *back.Positions[0][3])
	}
}

func TestAttachTrailsStaysWithinTheBodyCap(t *testing.T) {
	// The server refuses a snapshot over 256 KB, and a refused snapshot is a
	// lost one. At the 500-aircraft schema limit, carrying positions for every
	// one of them would put the payload within sight of that cap.
	at := time.Unix(1_800_000_000, 0)
	list := make([]snapshotAircraft, 0, 500)
	byHex := map[string][]trailPoint{}
	for i := 0; i < 500; i++ {
		hex := "a" + itoaPad(i)
		list = append(list, snapshotAircraft{Hex: hex})
		pts := make([]trailPoint, 0, 6)
		for j := 0; j < 6; j++ {
			pts = append(pts, trailPoint{
				atMs: at.Add(-time.Duration(6-j) * time.Second).UnixMilli(),
				lat:  -33 + float64(j)*0.01, lon: 151,
			})
		}
		byHex[hex] = pts
	}
	attachTrails(list, byHex, at)

	total := 0
	for i := range list {
		total += len(list[i].Positions)
	}
	if total > maxTrailPointsTotal {
		t.Fatalf("carried %d points, over the budget of %d", total, maxTrailPointsTotal)
	}
	// And the ones that did fit are whole tracks, not fragments.
	for i := range list {
		if n := len(list[i].Positions); n != 0 && n != 6 {
			t.Fatalf("aircraft %d carried a partial track of %d points", i, n)
		}
	}
}

func itoaPad(v int) string {
	const digits = "0123456789abcdef"
	return string([]byte{digits[(v>>8)&15], digits[(v>>4)&15], digits[v&15]})
}
