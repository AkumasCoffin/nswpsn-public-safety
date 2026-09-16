package decoderjson

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Fixtures are trimmed but structurally real dump1090-fa output.
const aircraftJSON = `{
  "now": 1789457035.1,
  "messages": 4823991,
  "aircraft": [
    {"hex":"7c6db8","flight":"QFA123  ","lat":-33.71,"lon":150.92,
     "alt_baro":3500,"gs":210.1,"track":118.4,"squawk":"3601",
     "category":"A3","seen":0.1,"seen_pos":1.2,"rssi":-21.4,"messages":842},
    {"hex":"7c1234","flight":"JST45   ","lat":-34.01,"lon":151.18,
     "alt_baro":"ground","seen":0.4,"seen_pos":2.0,"rssi":-18.0},
    {"hex":"7cabcd","flight":"NOPOS   ","seen":3.0,"rssi":-30.1},
    {"hex":"7cstale","lat":-30.0,"lon":150.0,"alt_baro":10000,"seen_pos":400.0}
  ]
}`

const statsJSON = `{
  "last1min": {"start":1789456975,"end":1789457035,"messages":7104,
    "local":{"signal":-24.1,"peak_signal":-3.2,"strong_signals":355,"accepted":[7104,12]},
    "tracks":{"all":41,"single_message":3},
    "max_distance_in_metres":287400.0,"max_distance_out_metres":190100.0},
  "total": {"start":1789450000,"end":1789457035,"messages":4823991,
    "local":{"signal":-25.0,"peak_signal":-2.9,"strong_signals":20000,"accepted":[4823991,900]},
    "tracks":{"all":941,"single_message":77},
    "max_distance_in_metres":331200.0}
}`

func writeFixtures(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "aircraft.json"), []byte(aircraftJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stats.json"), []byte(statsJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestReadsAircraft(t *testing.T) {
	r := NewReader(writeFixtures(t))
	f, err := r.Aircraft()
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Aircraft) != 4 {
		t.Fatalf("got %d aircraft, want 4", len(f.Aircraft))
	}
	a := f.Aircraft[0]
	if a.Hex != "7c6db8" || *a.Lat != -33.71 {
		t.Fatalf("unexpected first record: %+v", a)
	}
	if a.Category != "A3" {
		// The reason for choosing JSON over SBS: SBS carries no category.
		t.Fatalf("category = %q, want A3", a.Category)
	}
}

func TestPositionPresence(t *testing.T) {
	r := NewReader(writeFixtures(t))
	f, _ := r.Aircraft()
	if !f.Aircraft[0].HasPosition() {
		t.Error("aircraft with lat/lon/seen_pos should have a position")
	}
	// Mode S only: heard, but not locatable, and must never be plotted.
	if f.Aircraft[2].HasPosition() {
		t.Error("aircraft with no lat/lon must not report a position")
	}
}

func TestAltBaroHandlesGround(t *testing.T) {
	r := NewReader(writeFixtures(t))
	f, _ := r.Aircraft()

	alt, ground, ok := f.Aircraft[0].AltBaroValue()
	if !ok || ground || alt != 3500 {
		t.Fatalf("airborne: alt=%v ground=%v ok=%v", alt, ground, ok)
	}
	// dump1090 sends the STRING "ground" here, not a number — decoding it as a
	// float would drop surface traffic entirely.
	_, ground, ok = f.Aircraft[1].AltBaroValue()
	if !ok || !ground {
		t.Fatalf("surface: ground=%v ok=%v", ground, ok)
	}
	// Absent altitude is not an error, just unknown.
	if _, _, ok := f.Aircraft[2].AltBaroValue(); ok {
		t.Error("missing alt_baro should report ok=false")
	}
}

func TestStatsDerivations(t *testing.T) {
	r := NewReader(writeFixtures(t))
	s, err := r.Stats()
	if err != nil {
		t.Fatal(err)
	}
	// 7104 messages over a 60s window.
	if got := s.Last1Min.MsgRate(); got < 118 || got > 119 {
		t.Fatalf("msg rate = %v, want ~118.4", got)
	}
	// The larger of in/out distance, in km.
	km := s.Last1Min.MaxRangeKm()
	if km == nil || *km < 287 || *km > 288 {
		t.Fatalf("max range = %v, want ~287.4 km", km)
	}
	if got := s.Last1Min.StrongFraction(); got < 0.049 || got > 0.051 {
		t.Fatalf("strong fraction = %v, want ~0.05", got)
	}
	if s.Total.Tracks == nil || s.Total.Tracks.All != 941 {
		t.Fatal("expected total unique-track count")
	}
}

func TestRangeAbsentWithoutReceiverPosition(t *testing.T) {
	// dump1090 omits max_distance entirely when it has no --lat/--lon. This is
	// exactly why the backend makes the antenna pin mandatory, and the caller
	// must be able to tell "no range configured" from "range is zero".
	var w StatsWindow
	if got := w.MaxRangeKm(); got != nil {
		t.Fatalf("expected nil range with no receiver position, got %v", *got)
	}
}

func TestRatesAreSafeOnDegenerateWindows(t *testing.T) {
	// A just-started decoder reports a zero-length window; dividing by it would
	// produce +Inf and poison the heartbeat.
	w := StatsWindow{Messages: 100, Start: 5, End: 5}
	if got := w.MsgRate(); got != 0 {
		t.Fatalf("msg rate on a zero-length window = %v, want 0", got)
	}
	// Silence is not overload.
	quiet := StatsWindow{Messages: 0}
	if got := quiet.StrongFraction(); got != 0 {
		t.Fatalf("strong fraction with no messages = %v, want 0", got)
	}
}

func TestMissingFilesAreNotReadyNotErrors(t *testing.T) {
	// The normal state for the first second or two after the decoder starts.
	r := NewReader(t.TempDir())
	if _, err := r.Aircraft(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("missing aircraft.json: got %v, want ErrNotReady", err)
	}
	if _, err := r.Stats(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("missing stats.json: got %v, want ErrNotReady", err)
	}
}

func TestEmptyFileIsNotReady(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "aircraft.json"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewReader(dir).Aircraft(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("empty file: got %v, want ErrNotReady", err)
	}
}
