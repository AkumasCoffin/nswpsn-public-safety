package decoder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func argsOf(p Params) string { return strings.Join(p.Args(), " ") }

func TestOpensNoPorts(t *testing.T) {
	// The reason this agent reads JSON files instead of the SBS socket: the
	// decoder needs no network stack, so there is nothing to expose or
	// firewall. A --net creeping back in would silently change that.
	a := argsOf(Params{JSONDir: "/run/x"})
	for _, flag := range []string{"--net", "--net-only", "--net-bind-address"} {
		if strings.Contains(a, flag) {
			t.Fatalf("args must not enable networking, got %q", a)
		}
	}
	if !strings.Contains(a, "--write-json /run/x") {
		t.Fatalf("args must write JSON output, got %q", a)
	}
}

func TestGainOmittedWhenUnset(t *testing.T) {
	// No --gain at all is meaningfully different from --gain 0, which would
	// deafen the receiver.
	if strings.Contains(argsOf(Params{JSONDir: "/x"}), "--gain") {
		t.Fatal("expected no --gain flag when gain is unset")
	}
}

func TestGainRendersAndClamps(t *testing.T) {
	g := 42.1
	if !strings.Contains(argsOf(Params{JSONDir: "/x", GainDB: &g}), "--gain 42.1") {
		t.Fatal("expected the fixed gain to be passed through")
	}
	// librtlsdr clamps above the tuner's top step anyway; do it here so the
	// logged command matches what the hardware actually does.
	high := 99.0
	if !strings.Contains(argsOf(Params{JSONDir: "/x", GainDB: &high}), "--gain 49.6") {
		t.Fatal("expected gain above the tuner maximum to clamp")
	}
}

func TestPositionEnablesRangeStats(t *testing.T) {
	lat, lon := -33.8688, 151.2093
	a := argsOf(Params{JSONDir: "/x", Lat: &lat, Lon: &lon})
	// Without these dump1090 reports no max_distance at all, which is what
	// makes the antenna pin a functional requirement rather than metadata.
	if !strings.Contains(a, "--lat -33.868800") || !strings.Contains(a, "--lon 151.209300") {
		t.Fatalf("expected the antenna position to be passed, got %q", a)
	}
}

func TestPositionOmittedWhenIncomplete(t *testing.T) {
	lat := -33.8688
	if strings.Contains(argsOf(Params{JSONDir: "/x", Lat: &lat}), "--lat") {
		t.Fatal("half a position must not be passed to the decoder")
	}
}

func TestParseGain(t *testing.T) {
	cases := []struct {
		in       string
		wantAuto bool
		wantOK   bool
		wantVal  float64
	}{
		// "auto" means the AGENT adapts, not the dongle's hardware AGC.
		{"auto", true, true, 0},
		{"AUTO", true, true, 0},
		{"", true, true, 0},
		{"42.1", false, true, 42.1},
		{"99", false, true, 49.6}, // clamped
		{"banana", false, false, 0},
	}
	for _, c := range cases {
		g, auto, ok := ParseGain(c.in)
		if ok != c.wantOK || auto != c.wantAuto {
			t.Fatalf("ParseGain(%q) = auto %v ok %v, want auto %v ok %v", c.in, auto, ok, c.wantAuto, c.wantOK)
		}
		if c.wantOK && !c.wantAuto {
			if g == nil || *g != c.wantVal {
				t.Fatalf("ParseGain(%q) value = %v, want %v", c.in, g, c.wantVal)
			}
		}
	}
}

func TestWriteProducesAnExecutableScript(t *testing.T) {
	dir := t.TempDir()
	path, err := Write(dir, Params{Bin: "dump1090-fa", JSONDir: "/run/nswpsn-adsb"})
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(path) != "dump1090.sh" {
		t.Fatalf("script path = %q", path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	// exec so the supervised bash process IS the decoder: the supervisor's
	// process-group kill then reaches it, and the component's reported state
	// tracks the decoder rather than a wrapper outliving it.
	if !strings.Contains(s, "exec 'dump1090-fa'") {
		t.Fatalf("script should exec the decoder, got:\n%s", s)
	}
	if !strings.Contains(s, "mkdir -p '/run/nswpsn-adsb'") {
		t.Fatalf("script should ensure the JSON dir exists, got:\n%s", s)
	}
}

func TestWriteQuotesHostileValues(t *testing.T) {
	// These come from a server config push rather than an attacker, but a
	// launcher that pastes remote strings into a shell command is the kind of
	// thing that becomes a problem later.
	dir := t.TempDir()
	path, err := Write(dir, Params{Bin: "dump1090-fa", JSONDir: "/tmp/x'; touch /tmp/pwned; #"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	if strings.Contains(string(b), "; touch /tmp/pwned") && !strings.Contains(string(b), `'\''`) {
		t.Fatalf("injection was not neutralised:\n%s", string(b))
	}
}
