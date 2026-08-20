package configapply

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/sdrctl"
)

// testClient stands up a fake vce control server exposing GET /tuners and
// POST /tuners/{id}/samplerate, and returns a client pointed at it plus a
// func yielding the tuner ids a samplerate change was issued for.
func testClient(t *testing.T, live []map[string]any) (*sdrctl.Client, func() []string) {
	t.Helper()

	var mu sync.Mutex
	var rateCalls []string

	mux := http.NewServeMux()
	mux.HandleFunc("/tuners", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"tuners": live})
	})
	mux.HandleFunc("/tuners/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/samplerate") {
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/tuners/"), "/samplerate")
			mu.Lock()
			rateCalls = append(rateCalls, id)
			mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server url: %v", err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("test server port: %v", err)
	}

	return sdrctl.New(port, "test-token"), func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(rateCalls))
		copy(out, rateCalls)
		return out
	}
}

// A sample rate that already matches must NOT be re-sent. SDR-Trunk rebuilds
// the polyphase channelizer on ANY samplerate call — including one setting the
// rate the tuner already has — and that drops every traffic channel sourced
// from it. Re-importing an unchanged config used to churn the whole node.
func TestApplyTunersSkipsUnchangedSampleRate(t *testing.T) {
	sdr, calls := testClient(t, []map[string]any{
		{"id": "SAME", "sampleRate": 2400000.0},
		{"id": "DIFF", "sampleRate": 2400000.0},
	})

	Deps{SDR: sdr}.applyTuners([]TunerSettings{
		{Serial: "SAME", SampleRate: 2400000},
		{Serial: "DIFF", SampleRate: 2560000},
	})

	got := calls()
	if len(got) != 1 || got[0] != "DIFF" {
		t.Fatalf("expected a samplerate change for DIFF only, got %v", got)
	}
}

// Sub-Hz float noise between the config value and the tuner's reported rate is
// the same discrete hardware rate, not a change worth a channelizer rebuild.
func TestApplyTunersTreatsSubHzDriftAsUnchanged(t *testing.T) {
	sdr, calls := testClient(t, []map[string]any{
		{"id": "A", "sampleRate": 2400000.4},
	})

	Deps{SDR: sdr}.applyTuners([]TunerSettings{{Serial: "A", SampleRate: 2400000}})

	if got := calls(); len(got) != 0 {
		t.Fatalf("expected no samplerate change for sub-Hz drift, got %v", got)
	}
}

// An unset (zero) sample rate means "leave it alone" and must never be pushed
// as a literal 0, which would be an invalid rate.
func TestApplyTunersIgnoresUnsetSampleRate(t *testing.T) {
	sdr, calls := testClient(t, []map[string]any{
		{"id": "A", "sampleRate": 2400000.0},
	})

	Deps{SDR: sdr}.applyTuners([]TunerSettings{{Serial: "A"}})

	if got := calls(); len(got) != 0 {
		t.Fatalf("expected no samplerate call when unset, got %v", got)
	}
}

// The "*" wildcard entry applies to every live tuner, and the unchanged/changed
// decision is still made per tuner against that tuner's own reported rate.
func TestApplyTunersWildcardIsPerTuner(t *testing.T) {
	sdr, calls := testClient(t, []map[string]any{
		{"id": "ALREADY", "sampleRate": 2560000.0},
		{"id": "NEEDS", "sampleRate": 2400000.0},
	})

	Deps{SDR: sdr}.applyTuners([]TunerSettings{{Serial: "*", SampleRate: 2560000}})

	got := calls()
	if len(got) != 1 || got[0] != "NEEDS" {
		t.Fatalf("expected a samplerate change for NEEDS only, got %v", got)
	}
}
