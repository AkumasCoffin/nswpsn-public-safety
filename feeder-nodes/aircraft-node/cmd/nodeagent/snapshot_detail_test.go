package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/AkumasCoffin/nswpsn-node/aircraft-node/internal/decoderjson"
)

// One aircraft as dump1090-fa writes it, with everything a modern airliner
// sends, and one heard only well enough to place.
const detailFixture = `{"now": 1800000000.0, "messages": 100, "aircraft": [
 {"hex":"7c6b2d","flight":"QFA421  ","alt_baro":5375,"alt_geom":5600,"gs":251.3,"ias":250,
  "tas":262,"mach":0.412,"track":291.4,"roll":-0.35,"mag_heading":279.1,"true_heading":291.0,
  "baro_rate":-1216,"geom_rate":-1184,"squawk":"3412","emergency":"none","category":"A3",
  "nav_qnh":1017.6,"nav_altitude_mcp":3008,"nav_heading":280.5,
  "nav_modes":["autopilot","approach","a-mode-name-far-too-long"],
  "lat":-33.9,"lon":151.0,"nic":8,"rc":186,"seen_pos":0.4,"version":2,"nac_p":10,"sil":3,
  "mlat":[],"tisb":[],"messages":4821,"seen":0.1,"rssi":-12.3},
 {"hex":"7c1234","lat":-33.5,"lon":150.5,"seen_pos":1.0,"mlat":["lat","lon","nic","rc"],"tisb":[],
  "messages":12,"seen":0.9,"rssi":-28.1}
]}`

func detailAircraft(t *testing.T) []snapshotAircraft {
	t.Helper()
	var air decoderjson.AircraftFile
	if err := json.Unmarshal([]byte(detailFixture), &air); err != nil {
		t.Fatal(err)
	}
	out := buildAircraft(&air)
	if len(out) != 2 {
		t.Fatalf("want both aircraft, got %d", len(out))
	}
	return out
}

// The backend reads these names straight through the normaliser it uses for
// the aggregators, so what matters is the JSON, not the Go struct.
func TestUploadsWhatTheAircraftReports(t *testing.T) {
	b, err := json.Marshal(detailAircraft(t)[0])
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"alt_geom": 5600, "baro_rate": -1216, "geom_rate": -1184, "ias": 250, "tas": 262,
		"mach": 0.412, "true_heading": 291.0, "mag_heading": 279.1, "roll": -0.35,
		"nav_altitude_mcp": 3008, "nav_heading": 280.5, "nav_qnh": 1017.6,
		"nic": 8, "nac_p": 10, "sil": 3, "rc": 186, "version": 2, "rssi": -12.3, "messages": 4821,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s: got %v, want %v", k, got[k], v)
		}
	}
	if got["type"] != "adsb_icao" {
		t.Errorf("type: got %v, want adsb_icao", got["type"])
	}
	// The over-long mode is dropped; the two real ones go.
	modes, _ := got["nav_modes"].([]any)
	if len(modes) != 2 || modes[0] != "autopilot" || modes[1] != "approach" {
		t.Errorf("nav_modes: got %v", got["nav_modes"])
	}
}

// Most aircraft send few of these. An absent value must be absent on the
// wire, not zero: a zero vertical rate is a claim that it is flying level.
func TestOmitsWhatTheAircraftDidNotReport(t *testing.T) {
	b, err := json.Marshal(detailAircraft(t)[1])
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, k := range []string{"alt_geom", "baro_rate", "ias", "mach", "nav_qnh", "nav_modes", "nic", "version"} {
		if strings.Contains(s, `"`+k+`"`) {
			t.Errorf("%s must be omitted, got %s", k, s)
		}
	}
	if !strings.Contains(s, `"type":"mlat"`) {
		t.Errorf("a position listed under mlat is an MLAT position, got %s", s)
	}
}

func TestPositionTypeForTISB(t *testing.T) {
	cases := []struct {
		a    decoderjson.Aircraft
		want string
	}{
		{decoderjson.Aircraft{Hex: "7c6b2d"}, "adsb_icao"},
		{decoderjson.Aircraft{Hex: "7c6b2d", TISB: []string{"lat", "lon"}}, "tisb_icao"},
		{decoderjson.Aircraft{Hex: "~2a01f0", TISB: []string{"lat"}}, "tisb_other"},
		{decoderjson.Aircraft{Hex: "~2a01f0"}, "tisb_other"},
		// MLAT wins: it describes where the position itself came from.
		{decoderjson.Aircraft{Hex: "7c6b2d", MLAT: []string{"lat"}, TISB: []string{"gs"}}, "mlat"},
	}
	for _, c := range cases {
		if got := c.a.PositionType(); got != c.want {
			t.Errorf("%s mlat=%v tisb=%v: got %s, want %s", c.a.Hex, c.a.MLAT, c.a.TISB, got, c.want)
		}
	}
}
