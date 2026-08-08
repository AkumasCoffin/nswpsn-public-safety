package siteship

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestTickShipsSitesWithAuthHeaders verifies a non-empty snapshot set is POSTed
// as a bare JSON array carrying the node-ingest auth headers.
func TestTickShipsSitesWithAuthHeaders(t *testing.T) {
	sites := []json.RawMessage{
		json.RawMessage(`{"systemId":1,"rfss":1,"siteId":3,"systemName":"NSWPSN"}`),
		json.RawMessage(`{"systemId":1,"rfss":1,"siteId":4}`),
	}
	fetch := func() ([]json.RawMessage, error) { return sites, nil }

	var calls int
	var gotBody []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/api/node-ingest/site-snapshots" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.Header.Get("X-Node-Token") != "tok" || r.Header.Get("X-Node-Install") != "inst" {
			t.Errorf("missing node auth headers: %v", r.Header)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type = %q, want application/json", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		_, _ = w.Write([]byte(`{"ok":true,"written":2}`))
	}))
	defer srv.Close()

	s := New(Options{
		Fetch:     fetch,
		ServerURL: srv.URL,
		NodeToken: "tok",
		InstallID: "inst",
	})

	s.tick(context.Background())

	if calls != 1 {
		t.Fatalf("expected 1 backend call, got %d", calls)
	}
	if len(gotBody) != 2 {
		t.Fatalf("expected 2 sites in body, got %d: %v", len(gotBody), gotBody)
	}
	if gotBody[0]["systemName"] != "NSWPSN" {
		t.Errorf("first site systemName = %v, want NSWPSN", gotBody[0]["systemName"])
	}
	if !s.postedOnce {
		t.Error("postedOnce should be set after a successful ship")
	}
}

// TestTickSkipsPostWhenEmpty verifies an empty snapshot set never hits the
// backend.
func TestTickSkipsPostWhenEmpty(t *testing.T) {
	fetch := func() ([]json.RawMessage, error) { return nil, nil }

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	s := New(Options{
		Fetch:     fetch,
		ServerURL: srv.URL,
		NodeToken: "tok",
		InstallID: "inst",
	})
	s.tick(context.Background())

	if calls != 0 {
		t.Fatalf("empty snapshot set must not POST, got %d calls", calls)
	}
}

// TestTickFetchErrorIsSilent verifies a control-server fetch error re-arms the
// first-ship INFO and never POSTs.
func TestTickFetchErrorIsSilent(t *testing.T) {
	fetch := func() ([]json.RawMessage, error) { return nil, http.ErrServerClosed }

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
	}))
	defer srv.Close()

	s := New(Options{
		Fetch:     fetch,
		ServerURL: srv.URL,
		NodeToken: "tok",
		InstallID: "inst",
	})
	s.postedOnce = true
	s.tick(context.Background())

	if calls != 0 {
		t.Fatalf("fetch error must not POST, got %d calls", calls)
	}
	if s.postedOnce {
		t.Error("fetch error should re-arm the first-ship INFO (clear postedOnce)")
	}
}

// TestTickBackendErrorDoesNotMarkPosted verifies a backend 500 is not treated
// as a successful ship.
func TestTickBackendErrorDoesNotMarkPosted(t *testing.T) {
	fetch := func() ([]json.RawMessage, error) {
		return []json.RawMessage{json.RawMessage(`{"systemId":1}`)}, nil
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := New(Options{
		Fetch:     fetch,
		ServerURL: srv.URL,
		NodeToken: "tok",
		InstallID: "inst",
	})
	s.tick(context.Background())

	if s.postedOnce {
		t.Error("backend 500 must not mark the batch shipped")
	}
}
