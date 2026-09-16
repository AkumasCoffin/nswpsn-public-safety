// Package decoderjson reads dump1090's JSON output files.
//
// dump1090-fa (and dump1090-mutability) can be told to write its state to disk
// with --write-json <dir>, rewriting two files about once a second:
//
//	aircraft.json — every aircraft it is currently tracking
//	stats.json    — decoder counters over several time windows
//
// This is the same contract tar1090 and graphs1090 consume, which is the
// reason for choosing it over the SBS-1 network output on port 30003: SBS
// delivers a stream of partial messages that a client must reassemble into
// aircraft state itself (and carries neither category nor signal strength),
// whereas aircraft.json IS the assembled state, already deduplicated and
// aged by the decoder that owns the radio.
//
// It also means the decoder needs no network stack at all: no --net, no
// listening sockets, nothing to firewall.
//
// Both files are written to a temp name and renamed into place, so a reader
// never observes a half-written file — but it CAN observe ENOENT while the
// decoder is starting, which callers treat as "not ready", never as an error.
package decoderjson

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Aircraft is one entry of aircraft.json. Field names are dump1090's own.
//
// Almost everything is optional and genuinely absent in practice: an aircraft
// heard only via Mode S has no position, a TIS-B target often has no category,
// and an aircraft just acquired has no callsign yet. Pointers where zero is a
// meaningful value (altitude 0 is sea level; a nil AltBaro means unknown).
type Aircraft struct {
	Hex  string   `json:"hex"`
	Flag string   `json:"flight"`
	Lat  *float64 `json:"lat"`
	Lon  *float64 `json:"lon"`
	// AltBaro is a number OR the string "ground" for a surface position, so it
	// has to be decoded as a raw message and interpreted (see AltBaroValue).
	AltBaro  json.RawMessage `json:"alt_baro"`
	GS       *float64        `json:"gs"`
	Track    *float64        `json:"track"`
	Squawk   string          `json:"squawk"`
	Emerg    string          `json:"emergency"`
	Category string          `json:"category"`
	// Seen is seconds since ANY message from this aircraft; SeenPos is seconds
	// since its last POSITION. Only SeenPos matters for plotting: an aircraft
	// transmitting steadily but without position updates is not locatable.
	Seen     float64  `json:"seen"`
	SeenPos  *float64 `json:"seen_pos"`
	RSSI     *float64 `json:"rssi"`
	Messages int      `json:"messages"`
}

// AircraftFile is the top level of aircraft.json.
type AircraftFile struct {
	Now      float64    `json:"now"`
	Messages int64      `json:"messages"`
	Aircraft []Aircraft `json:"aircraft"`
}

// HasPosition reports whether this record can be placed on a map.
func (a *Aircraft) HasPosition() bool {
	return a.Lat != nil && a.Lon != nil && a.SeenPos != nil
}

// AltBaroValue interprets the number-or-"ground" altitude field. Returns the
// altitude in feet, or onGround=true for a surface position.
func (a *Aircraft) AltBaroValue() (alt float64, onGround bool, ok bool) {
	if len(a.AltBaro) == 0 {
		return 0, false, false
	}
	var s string
	if err := json.Unmarshal(a.AltBaro, &s); err == nil {
		return 0, s == "ground", s == "ground"
	}
	var f float64
	if err := json.Unmarshal(a.AltBaro, &f); err == nil {
		return f, false, true
	}
	return 0, false, false
}

// StatsWindow is one time window's counters inside stats.json.
type StatsWindow struct {
	// Messages decoded in this window.
	Messages int64 `json:"messages"`
	// Start/End are unix seconds bounding the window — needed to turn the
	// message COUNT into a rate, since the window is nominally but not exactly
	// 60 seconds (and is short while the decoder is still warming up).
	Start float64 `json:"start"`
	End   float64 `json:"end"`
	// MaxDistance* are metres, and are present ONLY when the decoder was given
	// --lat/--lon. That is why an exact antenna position is mandatory for this
	// node kind: without it there is no range statistic at all.
	MaxDistanceIn  *float64 `json:"max_distance_in_metres"`
	MaxDistanceOut *float64 `json:"max_distance_out_metres"`
	Local          *struct {
		// SignalStrength is mean RSSI (dBFS, negative). Strong counts messages
		// loud enough to indicate the receiver is being over-driven — the input
		// to the autogain loop.
		SignalStrength *float64 `json:"signal"`
		PeakSignal     *float64 `json:"peak_signal"`
		Strong         int64    `json:"strong_signals"`
		Accepted       []int64  `json:"accepted"`
	} `json:"local"`
	Tracks *struct {
		All       int64 `json:"all"`
		SingleMsg int64 `json:"single_message"`
	} `json:"tracks"`
	CPU *struct {
		Demod      int64 `json:"demod"`
		Reader     int64 `json:"reader"`
		Background int64 `json:"background"`
	} `json:"cpu"`
}

// StatsFile is the top level of stats.json.
type StatsFile struct {
	Last1Min  StatsWindow `json:"last1min"`
	Last5Min  StatsWindow `json:"last5min"`
	Last15Min StatsWindow `json:"last15min"`
	Total     StatsWindow `json:"total"`
}

// MsgRate is messages per second over this window, or 0 when the window has no
// usable duration (decoder just started, or a clock step made it nonsensical).
func (w *StatsWindow) MsgRate() float64 {
	d := w.End - w.Start
	if d <= 0 || d > 3600 {
		return 0
	}
	return float64(w.Messages) / d
}

// MaxRangeKm is the furthest aircraft in this window, km, or nil when the
// decoder has no receiver position configured.
func (w *StatsWindow) MaxRangeKm() *float64 {
	m := w.MaxDistanceIn
	if w.MaxDistanceOut != nil && (m == nil || *w.MaxDistanceOut > *m) {
		m = w.MaxDistanceOut
	}
	if m == nil {
		return nil
	}
	km := *m / 1000
	return &km
}

// StrongFraction is the share of accepted messages in this window that arrived
// loud enough to suggest overload, in 0..1. Returns 0 when the window carries
// no messages — silence is not overload.
func (w *StatsWindow) StrongFraction() float64 {
	if w.Local == nil || w.Messages <= 0 {
		return 0
	}
	return float64(w.Local.Strong) / float64(w.Messages)
}

// Reader reads the decoder's JSON output from a directory.
type Reader struct{ dir string }

func NewReader(dir string) *Reader { return &Reader{dir: dir} }

// ErrNotReady is returned when a file is not there yet. The decoder writes
// nothing until it has opened the SDR and produced its first output, so this
// is the normal state for the first second or two after a (re)start and must
// never be logged as a failure.
var ErrNotReady = fmt.Errorf("decoder json not written yet")

func (r *Reader) read(name string, into any) error {
	b, err := os.ReadFile(filepath.Join(r.dir, name))
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotReady
		}
		return err
	}
	if len(b) == 0 {
		// A zero-length read is the same transient situation as ENOENT.
		return ErrNotReady
	}
	return json.Unmarshal(b, into)
}

// Aircraft reads and parses aircraft.json.
func (r *Reader) Aircraft() (*AircraftFile, error) {
	var f AircraftFile
	if err := r.read("aircraft.json", &f); err != nil {
		return nil, err
	}
	return &f, nil
}

// Stats reads and parses stats.json.
func (r *Reader) Stats() (*StatsFile, error) {
	var f StatsFile
	if err := r.read("stats.json", &f); err != nil {
		return nil, err
	}
	return &f, nil
}
