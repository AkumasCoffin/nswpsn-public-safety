package rawlogship

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type upload struct {
	name    string
	content string
	token   string
	install string
}

type backend struct {
	mu      sync.Mutex
	status  int
	uploads []upload
}

func (b *backend) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/node-ingest/raw-log" || r.Method != http.MethodPost {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.ContentLength <= 0 {
			t.Errorf("upload must carry a Content-Length, got %d", r.ContentLength)
		}
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("body is not gzip: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		raw, _ := io.ReadAll(zr)
		b.mu.Lock()
		b.uploads = append(b.uploads, upload{
			name: r.URL.Query().Get("name"), content: string(raw),
			token: r.Header.Get("X-Node-Token"), install: r.Header.Get("X-Node-Install"),
		})
		status := b.status
		b.mu.Unlock()
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
	})
}

func setup(t *testing.T) (*Shipper, *backend, string, time.Time) {
	t.Helper()
	b := &backend{}
	srv := httptest.NewServer(b.handler(t))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	s := New(Options{
		Dir: dir, TempDir: t.TempDir(), ServerURL: srv.URL + "/",
		NodeToken: "tok", InstallID: "inst", Now: func() time.Time { return now },
	})
	return s, b, dir, now
}

func write(t *testing.T, dir, name, content string, modTime time.Time) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, modTime, modTime); err != nil {
		t.Fatal(err)
	}
	return p
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func TestUploadsAFinishedLogAndDeletesIt(t *testing.T) {
	s, b, dir, now := setup(t)
	p := write(t, dir, "20261120_T-NSW_decoded_messages.log", "TS1 DATCH-SC MSG:ABCDEF\n", now.Add(-10*time.Minute))
	s.Tick(context.Background())
	if len(b.uploads) != 1 {
		t.Fatalf("want 1 upload, got %d", len(b.uploads))
	}
	u := b.uploads[0]
	if u.name != "20261120_T-NSW_decoded_messages.log" || u.content != "TS1 DATCH-SC MSG:ABCDEF\n" || u.token != "tok" || u.install != "inst" {
		t.Fatalf("unexpected upload: %+v", u)
	}
	if exists(p) {
		t.Fatal("an uploaded file should be deleted")
	}
}

func TestLeavesAFileThatIsStillBeingWritten(t *testing.T) {
	s, b, dir, now := setup(t)
	p := write(t, dir, "live_decoded_messages.log", "x\n", now.Add(-30*time.Second))
	s.Tick(context.Background())
	if len(b.uploads) != 0 || !exists(p) {
		t.Fatal("a file written to in the last two minutes is not finished")
	}
}

func TestIgnoresOtherFilesInTheDirectory(t *testing.T) {
	s, b, dir, now := setup(t)
	p := write(t, dir, "20261120_call_events.log", "x\n", now.Add(-time.Hour))
	s.Tick(context.Background())
	if len(b.uploads) != 0 || !exists(p) {
		t.Fatal("only decoded-message logs are shipped or touched")
	}
}

func TestKeepsFilesWhenTheBackendIsDown(t *testing.T) {
	s, b, dir, now := setup(t)
	b.status = http.StatusServiceUnavailable
	p1 := write(t, dir, "a_decoded_messages.log", "1\n", now.Add(-20*time.Minute))
	p2 := write(t, dir, "b_decoded_messages.log", "2\n", now.Add(-10*time.Minute))
	s.Tick(context.Background())
	if len(b.uploads) != 1 {
		t.Fatalf("should stop after the first failure, made %d attempts", len(b.uploads))
	}
	if !exists(p1) || !exists(p2) {
		t.Fatal("nothing may be deleted while the backend is failing")
	}
}

func TestDiscardsWhenTheBackendIsNotCapturing(t *testing.T) {
	s, b, dir, now := setup(t)
	b.status = http.StatusConflict
	p1 := write(t, dir, "a_decoded_messages.log", "1\n", now.Add(-20*time.Minute))
	p2 := write(t, dir, "b_decoded_messages.log", "2\n", now.Add(-10*time.Minute))
	s.Tick(context.Background())
	if len(b.uploads) != 2 || exists(p1) || exists(p2) {
		t.Fatal("a 409 means the backend will never take the file: discard it and move on")
	}
}

func TestOldestFirstAndEmptyFilesDropped(t *testing.T) {
	s, b, dir, now := setup(t)
	write(t, dir, "new_decoded_messages.log", "new\n", now.Add(-5*time.Minute))
	write(t, dir, "old_decoded_messages.log", "old\n", now.Add(-50*time.Minute))
	empty := write(t, dir, "empty_decoded_messages.log", "", now.Add(-30*time.Minute))
	s.Tick(context.Background())
	if len(b.uploads) != 2 || b.uploads[0].content != "old\n" || b.uploads[1].content != "new\n" {
		t.Fatalf("unexpected order: %+v", b.uploads)
	}
	if exists(empty) {
		t.Fatal("an empty log is removed without an upload")
	}
}

func TestNoDirectoryIsNotAnError(t *testing.T) {
	s := New(Options{Dir: filepath.Join(t.TempDir(), "missing"), TempDir: t.TempDir(), ServerURL: "http://127.0.0.1:1"})
	s.Tick(context.Background()) // must not panic or block
}

func TestCompressedCopyIsRemoved(t *testing.T) {
	s, _, dir, now := setup(t)
	write(t, dir, "a_decoded_messages.log", string(bytes.Repeat([]byte("line\n"), 1000)), now.Add(-10*time.Minute))
	s.Tick(context.Background())
	left, _ := os.ReadDir(s.opts.TempDir)
	if len(left) != 0 {
		t.Fatalf("temporary gzip left behind: %d file(s)", len(left))
	}
}
