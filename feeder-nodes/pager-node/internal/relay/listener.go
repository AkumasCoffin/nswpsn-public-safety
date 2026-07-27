// Package relay runs a localhost HTTP listener that receives decoded pager lines
// from the reader.sh scripts. Each reader POSTs one raw multimon-ng line to
// POST /pager with X-Pager-Source and X-Pager-Freq headers; this listener parses
// the line, and on a decodable POCSAG page enqueues a normalized JSON message to
// the disk-backed queue for upload. It MUST always respond 200 so the fire-and-
// forget reader curls never block or retry.
package relay

import (
	"context"
	"encoding/binary"
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

// Audio monitor stream constants. The reader tees rtl_fm's 22050 Hz s16le mono
// PCM to /audio; we decimate by 3 (block-average) to ~7350 Hz to cut the
// bandwidth of the ~voice-grade browser monitor, and emit binary frames the
// backend relays verbatim to staff.
const (
	audioInRate       = 22050
	audioDecim        = 3                       // 22050/3 = 7350 Hz out
	audioOutRate      = audioInRate / audioDecim // 7350
	audioFrameSamples = 512                     // ~70 ms per frame at 7350 Hz
	audioFrameType    = 0x02                    // 1st byte discriminates from spectrum (0x01)
)

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
	addr      string
	q         *queue.Queue
	srv       *http.Server
	audioSink func([]byte) // set via SetAudioSink; forwards framed monitor audio
}

// SetAudioSink wires the destination for framed monitor audio (the WS binary
// sender). Called once at startup, before the server accepts /audio streams.
func (l *Listener) SetAudioSink(fn func([]byte)) { l.audioSink = fn }

// New builds a Listener bound to addr (must be a loopback addr) enqueuing to q.
func New(addr string, q *queue.Queue) *Listener {
	l := &Listener{addr: addr, q: q}
	mux := http.NewServeMux()
	mux.HandleFunc("/pager", l.handlePager)
	mux.HandleFunc("/audio", l.handleAudio)
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

// handleAudio streams the reader's tee'd rtl_fm PCM (22050 Hz s16le mono),
// decimates it to ~7350 Hz by block-averaging, and forwards framed chunks to the
// audio sink (WS → staff). It reads until the reader stops (curl closes the
// body), so it blocks for the whole monitor session. Always 200 (the reader curl
// is fire-and-forget). A nil sink drains + discards.
func (l *Listener) handleAudio(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeOK(w)
		return
	}
	sink := l.audioSink
	if sink == nil {
		_, _ = io.Copy(io.Discard, r.Body)
		writeOK(w)
		return
	}
	log.Printf("relay: audio monitor stream opened")

	out := make([]int16, 0, audioFrameSamples)
	var acc, accN int // running block-average accumulator for decimation
	feed := func(s int16) {
		acc += int(s)
		accN++
		if accN == audioDecim {
			out = append(out, int16(acc/audioDecim))
			acc, accN = 0, 0
			if len(out) >= audioFrameSamples {
				sink(buildAudioFrame(out))
				out = out[:0]
			}
		}
	}

	buf := make([]byte, 8192)
	var haveHalf bool
	var halfByte byte
	for {
		n, err := r.Body.Read(buf)
		if n > 0 {
			data := buf[:n]
			idx := 0
			if haveHalf {
				feed(int16(uint16(halfByte) | uint16(data[0])<<8))
				idx = 1
				haveHalf = false
			}
			for ; idx+1 < len(data); idx += 2 {
				feed(int16(uint16(data[idx]) | uint16(data[idx+1])<<8))
			}
			if idx < len(data) {
				halfByte = data[idx]
				haveHalf = true
			}
		}
		if err != nil {
			break
		}
	}
	if len(out) > 0 {
		sink(buildAudioFrame(out))
	}
	log.Printf("relay: audio monitor stream closed")
	writeOK(w)
}

// buildAudioFrame packs decimated samples into a binary monitor frame:
//
//	u8 type(0x02) | u8 reserved | u32 sampleRate LE | u16 sampleCount LE | int16[] LE
func buildAudioFrame(samples []int16) []byte {
	b := make([]byte, 8+len(samples)*2)
	b[0] = audioFrameType
	b[1] = 0
	binary.LittleEndian.PutUint32(b[2:], uint32(audioOutRate))
	binary.LittleEndian.PutUint16(b[6:], uint16(len(samples)))
	for i, s := range samples {
		binary.LittleEndian.PutUint16(b[8+i*2:], uint16(s))
	}
	return b
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
