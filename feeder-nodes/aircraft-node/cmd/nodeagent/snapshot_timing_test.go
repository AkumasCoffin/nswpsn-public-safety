package main

import (
	"testing"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/decoderjson"
)

// The decoder writes aircraft.json once a second and the agent reads it once a
// second, on unrelated beats. So the same file is sometimes read twice, and a
// file is always read some hundreds of milliseconds after it was written.
// seen_pos counts back from the WRITE; timing it from the read moved every fix
// by that gap and turned one fix into two.
func TestOneFileReadTwiceIsOneFix(t *testing.T) {
	tr := newPositionTrail()
	written := time.Unix(1_800_000_000, 0)
	f := file(ac("abc123", -33.0, 151.0, 0.4))
	f.Now = float64(written.UnixMilli()) / 1000

	tr.observe(f, written.Add(150*time.Millisecond))
	tr.observe(f, written.Add(900*time.Millisecond))

	pts := tr.byHex["abc123"]
	if len(pts) != 1 {
		t.Fatalf("one file read twice became %d points", len(pts))
	}
	if want := written.UnixMilli() - 400; pts[0].atMs != want {
		t.Fatalf("fix timed at %d, want %d (the write, less seen_pos)", pts[0].atMs, want)
	}
}

func TestDecoderNowIsNotBelievedWhenItCannotBeRight(t *testing.T) {
	read := time.Unix(1_800_000_000, 0)
	cases := []struct {
		name string
		now  float64
		want int64
	}{
		{"missing", 0, read.UnixMilli()},
		{"half a second ago", float64(read.UnixMilli()-500) / 1000, read.UnixMilli() - 500},
		{"ahead of this host", float64(read.UnixMilli()+2_000) / 1000, read.UnixMilli()},
		{"a minute behind", float64(read.UnixMilli()-60_000) / 1000, read.UnixMilli()},
	}
	for _, c := range cases {
		got := decoderNowMs(&decoderjson.AircraftFile{Now: c.now}, read)
		if got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

// seen_pos on the wire is measured to the snapshot's `at`, so the time the
// file sat before being read has to be added back.
func TestSeenPosIsRebasedToTheSnapshot(t *testing.T) {
	list := []snapshotAircraft{{Hex: "abc123", SeenPos: 0.4}}
	ageToSnapshot(list, 700)
	if got := list[0].SeenPos; got < 1.099 || got > 1.101 {
		t.Fatalf("seen_pos = %v, want 1.1", got)
	}
	ageToSnapshot(list, -50) // a clock that stepped: never subtract
	if got := list[0].SeenPos; got < 1.099 || got > 1.101 {
		t.Fatalf("seen_pos = %v after a negative lag, want it unchanged", got)
	}
}
