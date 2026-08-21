// Package relay runs a localhost HTTP listener that impersonates rdio-scanner's
// call-upload endpoint. The local rdio-scanner is configured with a downstream
// pointing here; every decoded call is POSTed as multipart/form-data. This
// listener MUST always respond 200 (the local rdio drops the call on any
// non-200) and enqueues the raw request to the disk-backed queue.
package relay

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/queue"
)

// maxUploadBytes caps a single inbound call-upload body.
const maxUploadBytes = 25 << 20 // 25 MiB

// Listener is the localhost HTTP server that captures inbound calls.
type Listener struct {
	addr string
	q    *queue.Queue
	srv  *http.Server
	// dropped counts calls accepted from rdio that we then failed to persist.
	// Those calls are GONE — rdio's downstream Send does not retry, it logs the
	// error and moves on (rdio-scanner server/downstream.go) — so the only
	// thing left to do is make the loss visible instead of silent. Surfaced in
	// the status frame so a node shedding calls doesn't look healthy.
	dropped atomic.Uint64
}

// Dropped returns the number of calls accepted but not persisted this run.
func (l *Listener) Dropped() uint64 { return l.dropped.Load() }

// New builds a Listener bound to addr (must be a loopback addr) enqueuing to q.
func New(addr string, q *queue.Queue) *Listener {
	l := &Listener{addr: addr, q: q}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/call-upload", l.handleCallUpload)
	mux.HandleFunc("/api/capabilities", l.handleCapabilities)
	mux.HandleFunc("/", l.handleCatchAll)
	l.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}
	return l
}

// Run starts the HTTP server and blocks until ctx is cancelled, then performs a
// graceful shutdown.
func (l *Listener) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		log.Printf("relay: listening on http://%s", l.addr)
		err := l.srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := l.srv.Shutdown(shutCtx); err != nil {
			log.Printf("relay: shutdown error: %v", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}

// handleCallUpload reads the raw body + Content-Type, enqueues it, and ALWAYS
// responds 200 — even if reading failed or the queue had to drop-oldest.
func (l *Listener) handleCallUpload(w http.ResponseWriter, r *http.Request) {
	// The rdio downstream is fire-and-forget: it drops the call on ANY non-200 or
	// closed connection. So this handler must always 200 — including on an
	// unexpected panic, which net/http would otherwise surface as a dropped
	// connection with no response written. `ok` is idempotent so we 200 exactly
	// once regardless of which path (or the recover) reaches it.
	responded := false
	ok := func() {
		if !responded {
			responded = true
			writeOK(w)
		}
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("relay: recovered panic in call-upload handler: %v", rec)
			ok()
		}
	}()

	if r.Method != http.MethodPost {
		// Be lenient — still 200 so probes succeed.
		ok()
		return
	}
	contentType := r.Header.Get("Content-Type")

	limited := http.MaxBytesReader(w, r.Body, maxUploadBytes)
	body, err := io.ReadAll(limited)
	if err != nil {
		// Still respond 200; just log and drop this one.
		log.Printf("relay: failed reading call-upload body: %v", err)
		l.dropped.Add(1)
		ok()
		return
	}

	if err := l.q.Enqueue(contentType, body); err != nil {
		// The call is lost. Responding non-200 would NOT save it: rdio's
		// downstream sender logs the failure and moves on without retrying, so
		// the only difference is which side records the loss. Keep the 200 (the
		// contract this listener is built on) and count it — a disk-full or
		// read-only queue dir otherwise sheds every call with nothing but a log
		// line while the node reports QueueDepth 0 and looks perfectly fine.
		n := l.dropped.Add(1)
		log.Printf("relay: DROPPED call — enqueue failed (%d dropped this run): %v", n, err)
	}
	ok()
}

// handleCapabilities tells the rdio downstream probe we exist but advertise no
// features (no transcript-forward).
func (l *Listener) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"features":[]}`)
}

// handleCatchAll leniently answers any other probe path with 200.
func (l *Listener) handleCatchAll(w http.ResponseWriter, r *http.Request) {
	writeOK(w)
}

func writeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"status":"ok"}`)
}
