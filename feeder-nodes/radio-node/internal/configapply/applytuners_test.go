package configapply

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// A control server that is not LISTENING yet must be waited out, not written
// off. A cold JVM runs its first-run CPU calibration and installs JMBE before
// the control server binds, and the three quick attempts this used to make
// were all spent inside a few seconds of a reboot — so a config push that
// arrived in that window failed at the "playlist" stage, and a failed stage is
// neither persisted nor retried: the node went on running the previous config.
func TestImportWaitsOutAColdControlServer(t *testing.T) {
	// Reserve a port and release it, so the first attempts land on a closed
	// port exactly as they do while the JVM is still starting.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	var imports atomic.Int32
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/config/import" {
			imports.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channels":2}`))
	})}
	t.Cleanup(func() { _ = srv.Close() })

	// Bind a moment later, as a slow JVM does.
	go func() {
		time.Sleep(1500 * time.Millisecond)
		late, lerr := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if lerr != nil {
			return
		}
		_ = srv.Serve(late)
	}()

	d := Deps{SDR: sdrctl.New(port, "test-token")}
	if err := d.importWithRetry([]byte(`{}`)); err != nil {
		t.Fatalf("import gave up on a control server that was still starting: %v", err)
	}
	if got := imports.Load(); got != 1 {
		t.Fatalf("want exactly one accepted import, got %d", got)
	}
}

// A server that ANSWERS and refuses the body has given a settled answer, not a
// start-up condition. It gets the three attempts and then reports, rather than
// holding the apply lock for the full start-up budget.
func TestImportStopsRetryingARefusedBody(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server url: %v", err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("test server port: %v", err)
	}

	d := Deps{SDR: sdrctl.New(port, "test-token")}
	started := time.Now()
	if err := d.importWithRetry([]byte(`{}`)); err == nil {
		t.Fatal("a refused import must surface as an error")
	}
	if got := attempts.Load(); got != importAttempts {
		t.Fatalf("want %d attempts against an answering server, got %d", importAttempts, got)
	}
	if elapsed := time.Since(started); elapsed >= importStartupWait {
		t.Fatalf("a refused body waited out the start-up budget (%s)", elapsed)
	}
}
