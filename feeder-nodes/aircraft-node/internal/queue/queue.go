// Package queue implements a disk-backed, bounded, FIFO queue of buffered radio
// calls awaiting upload to the backend relay.
//
// On-disk format: one file per call named "<20-digit-zero-padded-unixnano>-<rand4>.call"
// so that a lexical sort of filenames equals FIFO order. Each file contains:
//
//	line 1: the Content-Type header value
//	line 2: (blank)
//	rest:   the raw request body bytes, verbatim
//
// Writes are atomic (temp file + rename). Files persist across restarts so the
// sender resumes where it left off.
package queue

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SendResult tells the sender loop what to do with an item after send().
type SendResult int

const (
	// SendOK: upload succeeded — delete the item.
	SendOK SendResult = iota
	// SendDrop: permanent failure (401/403/404/413) — delete without retry.
	SendDrop
	// SendRetry: transient failure — keep the item and retry after backoff.
	SendRetry
)

const (
	fileExt = ".snapshot"

	// maxItemAge bounds how long a snapshot may sit undelivered before it is
	// discarded. Far shorter than the radio agent's 5 minutes, because an
	// aircraft position decays much faster than a recorded call: the backend
	// drops any position older than 60s outright and bridges gaps for at most
	// 90s, so a snapshot older than that cannot contribute anything at all.
	// Delivering it would be pure waste — and during a long outage, a retry
	// storm of data the backend is guaranteed to discard.
	maxItemAge = 90 * time.Second
	tmpExt     = ".tmp"
	// Bounds sized for snapshots, not audio. At 2s cadence, 50 items is ~100
	// seconds of backlog — well past the age bound above, so the age check is
	// what actually trims the queue and these are just a hard ceiling. A
	// snapshot is ~100 KB at worst, so 32 MiB covers the count with room over.
	defaultMaxBytes = 32 << 20 // 32 MiB
	defaultMaxCount = 50
	backoffInitial  = 1 * time.Second
	backoffMax      = 60 * time.Second
)

// Queue is a disk-backed, bounded buffer of snapshots, sent newest first.
// Safe for concurrent use.
type Queue struct {
	dir      string
	maxBytes int64
	maxCount int

	mu       sync.Mutex // guards on-disk mutations to keep bound enforcement consistent
	lastNano int64      // last filename timestamp used; monotonic guard against clock step-back

	// Cached Depth(), guarded by mu. Invalidated on every enqueue/remove so it
	// can only ever be stale by depthCacheTTL of no activity.
	depthVal int
	depthAt  time.Time
	// expired counts snapshots discarded for exceeding maxItemAge — undeliverable,
	// not undeliverable-yet. Reported in the status heartbeat so a node losing
	// calls to a long outage is visible rather than silent.
	expired atomic.Uint64
	// superseded counts snapshots dropped because a newer one was already
	// queued behind them (see newest).
	superseded atomic.Uint64
}

// Superseded returns how many snapshots were dropped in favour of a newer one
// this run.
func (q *Queue) Superseded() uint64 { return q.superseded.Load() }

// Expired returns how many calls were discarded for age this run.
func (q *Queue) Expired() uint64 { return q.expired.Load() }

// itemNano recovers the enqueue time from a queue filename
// ("<20-digit-unixnano>-<rand4>.call"). Returns 0 when unparseable, which the
// caller treats as "age unknown" rather than "infinitely old".
func itemNano(name string) int64 {
	i := strings.IndexByte(name, '-')
	if i <= 0 {
		return 0
	}
	v, err := strconv.ParseInt(name[:i], 10, 64)
	if err != nil || v <= 0 {
		return 0
	}
	return v
}

// Open initializes a queue rooted at dir. maxBytes<=0 and maxCount<=0 fall back
// to defaults (2 GiB / 5000 items).
func Open(dir string, maxBytes int64, maxCount int) (*Queue, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create queue dir %q: %w", dir, err)
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}
	if maxCount <= 0 {
		maxCount = defaultMaxCount
	}
	q := &Queue{dir: dir, maxBytes: maxBytes, maxCount: maxCount}
	// Clean up stale temp files from a crashed write, and any items left by a
	// different node agent.
	q.cleanupTemps()
	return q, nil
}

// cleanupTemps removes crashed-write temp files, and items belonging to another
// agent.
//
// A host runs one node kind at a time but can be switched between them by
// re-running the other installer, which reuses this data dir. The radio and
// pager agents both name their items .call, so leftovers are picked up, sent,
// refused by the backend's kind gate and deleted — the queue cleans itself.
// This agent's items are .snapshot, so .call leftovers would be invisible to
// every scan and sit here for good. Dropping them is safe in both directions:
// an item for a node kind this host no longer serves can never be delivered.
func (q *Queue) cleanupTemps() {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return
	}
	foreign := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		switch {
		case strings.HasSuffix(n, tmpExt):
			_ = os.Remove(filepath.Join(q.dir, n))
		case !strings.HasSuffix(n, fileExt):
			_ = os.Remove(filepath.Join(q.dir, n))
			foreign++
		}
	}
	if foreign > 0 {
		log.Printf("queue: discarded %d item(s) left by another node agent", foreign)
	}
}

// Enqueue atomically persists one call. If the queue is over either bound after
// adding, the oldest items are deleted until it is under both bounds.
func (q *Queue) Enqueue(contentType string, body []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Filenames sort by timestamp for FIFO / drop-oldest. Guard against a wall-clock
	// step-back (NTP) by clamping strictly forward, so a newer call can never sort
	// before an older queued one and get mis-dropped or drained out of order.
	nano := time.Now().UnixNano()
	if nano <= q.lastNano {
		nano = q.lastNano + 1
	}
	q.lastNano = nano

	name := fmt.Sprintf("%020d-%s%s", nano, rand4(), fileExt)
	final := filepath.Join(q.dir, name)
	tmp := final + tmpExt

	var buf bytes.Buffer
	buf.WriteString(contentType)
	buf.WriteByte('\n')
	buf.WriteByte('\n')
	buf.Write(body)

	// Write + fsync BEFORE the atomic rename so a hard power loss can't surface a
	// zero/partial .call file that was already 200-ACKed to rdio (the package's
	// durability contract).
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("write queue temp: %w", err)
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write queue temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("fsync queue temp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close queue temp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit queue item: %w", err)
	}

	q.invalidateDepth()
	q.enforceBounds()
	return nil
}

// enforceBounds deletes the oldest items while over maxCount or maxBytes.
// Caller must hold q.mu.
func (q *Queue) enforceBounds() {
	files := q.sortedFiles()
	var total int64
	for _, f := range files {
		total += f.size
	}

	dropped := 0
	i := 0
	for len(files)-i > q.maxCount || total > q.maxBytes {
		if i >= len(files) {
			break
		}
		f := files[i]
		if err := os.Remove(filepath.Join(q.dir, f.name)); err == nil {
			q.invalidateDepth()
			total -= f.size
			dropped++
		}
		i++
	}
	if dropped > 0 {
		log.Printf("queue: bounds exceeded, dropped %d oldest item(s) (maxCount=%d maxBytes=%d)", dropped, q.maxCount, q.maxBytes)
	}
}

type fileInfo struct {
	name string
	size int64
}

// sortedFiles returns .call files sorted lexically (== FIFO oldest-first).
func (q *Queue) sortedFiles() []fileInfo {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil
	}
	var out []fileInfo
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), fileExt) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		out = append(out, fileInfo{name: e.Name(), size: info.Size()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// depthCacheTTL bounds how stale a reported depth may be. Depth is a display
// figure on the status heartbeat, not a control input, so a second of staleness
// costs nothing.
const depthCacheTTL = 2 * time.Second

// Depth returns the number of queued .call files.
//
// Cached: the underlying scan is os.ReadDir + sort over the whole queue
// directory (up to maxCount entries) and it runs while holding q.mu — the same
// lock every Enqueue needs. The status heartbeat calls this, and that heartbeat
// now runs once a SECOND while staff watch the Live view, so on a node with a
// backlog it was a per-second 5000-entry scan serialised against call ingest.
func (q *Queue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.depthAt.IsZero() && time.Since(q.depthAt) < depthCacheTTL {
		return q.depthVal
	}
	n := len(q.sortedFiles())
	q.depthVal, q.depthAt = n, time.Now()
	return n
}

// invalidateDepth drops the cached depth. Called under q.mu whenever the
// directory contents change, so a caller never sees a count that predates its
// own enqueue or send.
func (q *Queue) invalidateDepth() {
	q.depthAt = time.Time{}
}

// newest returns the newest queued item's filename ("" if empty) and the names
// of every older one.
//
// A snapshot is the whole picture at one instant, so a newer one supersedes
// everything before it. The queue used to drain oldest first: after an outage
// that sent up to ~45 stale snapshots back to back, which used the server's
// whole per-minute allowance, and the server then silently dropped the fresh
// ones for the rest of that minute. The map showed minute-old positions just
// as the link came back. Newest first, the first thing delivered after an
// outage is the present.
func (q *Queue) newest() (name string, older []string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	files := q.sortedFiles()
	if len(files) == 0 {
		return "", nil
	}
	older = make([]string, 0, len(files)-1)
	for _, f := range files[:len(files)-1] {
		older = append(older, f.name)
	}
	return files[len(files)-1].name, older
}

// readItem parses a queue file into its Content-Type and body.
func (q *Queue) readItem(name string) (contentType string, body []byte, err error) {
	raw, err := os.ReadFile(filepath.Join(q.dir, name))
	if err != nil {
		return "", nil, err
	}
	// Split on the first blank line: header line, blank, body.
	idx := bytes.Index(raw, []byte("\n\n"))
	if idx < 0 {
		return "", nil, fmt.Errorf("malformed queue item %q", name)
	}
	contentType = string(raw[:idx])
	body = raw[idx+2:]
	return contentType, body, nil
}

func (q *Queue) remove(name string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	_ = os.Remove(filepath.Join(q.dir, name))
	q.invalidateDepth()
}

// RunSender sends the newest snapshot until ctx is cancelled, dropping the ones
// it supersedes (see newest). On SendOK/SendDrop the item is deleted and it
// continues immediately; on SendRetry it sleeps with exponential backoff
// (+jitter) and then sends whatever is newest by then. When the queue is empty
// it polls briefly.
func (q *Queue) RunSender(ctx context.Context, send func(contentType string, body []byte) SendResult) {
	backoff := backoffInitial
	for {
		if ctx.Err() != nil {
			return
		}

		name, older := q.newest()
		if len(older) > 0 {
			for _, o := range older {
				q.remove(o)
			}
			total := q.superseded.Add(uint64(len(older)))
			log.Printf("queue: dropped %d snapshot(s) superseded by a newer one (%d this run)", len(older), total)
		}
		if name == "" {
			// Empty — wait a bit before polling again.
			if !sleepCtx(ctx, 500*time.Millisecond) {
				return
			}
			backoff = backoffInitial
			continue
		}

		// Age bound: a snapshot nobody could deliver inside maxItemAge is discarded
		// rather than retried forever. Checked before the read so an expired item
		// costs no I/O. Counted, not silent — see Expired().
		if nano := itemNano(name); nano > 0 && time.Since(time.Unix(0, nano)) > maxItemAge {
			q.remove(name)
			total := q.expired.Add(1)
			log.Printf("queue: discarding snapshot older than %s (%d expired this run)", maxItemAge, total)
			backoff = backoffInitial
			continue
		}

		contentType, body, err := q.readItem(name)
		if err != nil {
			// Corrupt/unreadable item — drop it so we don't wedge the queue.
			log.Printf("queue: dropping unreadable item %q: %v", name, err)
			q.remove(name)
			continue
		}

		switch send(contentType, body) {
		case SendOK:
			q.remove(name)
			backoff = backoffInitial
		case SendDrop:
			log.Printf("queue: dropping item %q (permanent send failure)", name)
			q.remove(name)
			backoff = backoffInitial
		case SendRetry:
			d := jitter(backoff)
			log.Printf("queue: retryable send failure for %q, backing off %s (depth=%d)", name, d.Round(time.Millisecond), q.Depth())
			if !sleepCtx(ctx, d) {
				return
			}
			backoff *= 2
			if backoff > backoffMax {
				backoff = backoffMax
			}
		}
	}
}

// sleepCtx sleeps for d or until ctx is cancelled. Returns false if cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// jitter adds up to +25% random jitter to d.
func jitter(d time.Duration) time.Duration {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return d
	}
	frac := float64(uint16(b[0])<<8|uint16(b[1])) / 65535.0 // 0..1
	return d + time.Duration(float64(d)*0.25*frac)
}

func rand4() string {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000"
	}
	return hex.EncodeToString(b[:])
}
