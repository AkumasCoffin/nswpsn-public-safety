// Package relay runs a localhost HTTP listener that receives decoded pager lines
// from the reader.sh scripts. Each reader POSTs one raw multimon-ng line to
// POST /pager with X-Pager-Source and X-Pager-Freq headers; this listener parses
// the line, and on a decodable POCSAG page enqueues a normalized JSON message to
// the disk-backed queue for upload. It MUST always respond 200 so the fire-and-
// forget reader curls never block or retry.
package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/pagerdecode"
	"github.com/AkumasCoffin/nswpsn-node/pager-node/internal/queue"
)

// maxUploadBytes caps a single inbound pager line. A POCSAG page is tiny; this is
// just a sanity bound against a runaway body.
const maxUploadBytes = 64 << 10 // 64 KiB

// pagerMessage is the normalized JSON payload enqueued for upload.
type pagerMessage struct {
	Address   string  `json:"address"`
	Function  int     `json:"function"`
	Message   string  `json:"message"`
	Timestamp string  `json:"timestamp"`
	Source    string  `json:"source"`
	FreqMHz   float64 `json:"freqMhz"`
}

// Listener is the localhost HTTP server that captures decoded pager lines.
type Listener struct {
	addr string
	q    *queue.Queue
	srv  *http.Server
}

// New builds a Listener bound to addr (must be a loopback addr) enqueuing to q.
func New(addr string, q *queue.Queue) *Listener {
	l := &Listener{addr: addr, q: q}
	mux := http.NewServeMux()
	mux.HandleFunc("/pager", l.handlePager)
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

// handlePager reads one raw multimon-ng line plus its source/freq headers, and
// on a decodable POCSAG page enqueues a normalized JSON message. It ALWAYS
// responds 200 — a non-POCSAG line, a read error, or an enqueue failure is logged
// and dropped, never surfaced to the fire-and-forget reader curl.
func (l *Listener) handlePager(w http.ResponseWriter, r *http.Request) {
	// The reader curls are fire-and-forget (|| true), so this handler must always
	// 200 — including on an unexpected panic, which net/http would otherwise
	// surface as a dropped connection with no response written.
	responded := false
	ok := func() {
		if !responded {
			responded = true
			writeOK(w)
		}
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("relay: recovered panic in pager handler: %v", rec)
			ok()
		}
	}()

	if r.Method != http.MethodPost {
		// Be lenient — still 200 so probes succeed.
		ok()
		return
	}

	source := r.Header.Get("X-Pager-Source")
	freqMHz := parseFreq(r.Header.Get("X-Pager-Freq"))

	limited := http.MaxBytesReader(w, r.Body, maxUploadBytes)
	body, err := io.ReadAll(limited)
	if err != nil {
		log.Printf("relay: failed reading pager body: %v", err)
		ok()
		return
	}

	msg, decoded := pagerdecode.ParseLine(string(body))
	if !decoded {
		// Not a POCSAG page (multimon banner / noise). Drop silently.
		ok()
		return
	}

	payload := pagerMessage{
		Address:   msg.Address,
		Function:  msg.Function,
		Message:   msg.Text,
		Timestamp: time.Now().Format(time.RFC3339),
		Source:    source,
		FreqMHz:   freqMHz,
	}
	enc, err := json.Marshal(payload)
	if err != nil {
		log.Printf("relay: marshal pager message failed: %v", err)
		ok()
		return
	}
	if err := l.q.Enqueue("application/json", enc); err != nil {
		log.Printf("relay: enqueue failed: %v", err)
	}
	ok()
}

// handleCatchAll leniently answers any other probe path with 200.
func (l *Listener) handleCatchAll(w http.ResponseWriter, r *http.Request) {
	writeOK(w)
}

// parseFreq parses the X-Pager-Freq header (MHz) into a float, returning 0 when
// it is missing or malformed.
func parseFreq(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

func writeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"status":"ok"}`)
}
