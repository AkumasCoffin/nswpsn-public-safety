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
	fileExt = ".call"

	// maxItemAge bounds how long a call may sit undelivered before it is
	// discarded. The queue exists so a backend restart, a deploy or a tunnel
	// blip does not lose calls — all of which resolve in seconds. Retrying
	// beyond that delivers audio nobody is waiting for and, during a long
	// outage, turns every queued call into a retry storm against the backend
	// the moment it returns.
	maxItemAge      = 5 * time.Minute
	tmpExt          = ".tmp"
	defaultMaxBytes = 2 << 30 // 2 GiB
	defaultMaxCount = 5000
	backoffInitial  = 1 * time.Second
	backoffMax      = 60 * time.Second
)

// Queue is a FIFO, disk-backed, bounded call buffer. Safe for concurrent use.
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
	// Remembered tail of the last sortedFiles scan, consumed in order by
	// oldest(); see there. Cleared by invalidateDepth.
	oldestRun []string

	// expired counts calls discarded for exceeding maxItemAge — undeliverable,
	// not undeliverable-yet. Reported in the status heartbeat so a node losing
	// calls to a long outage is visible rather than silent.
	expired atomic.Uint64
}

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
	// Clean up any stale temp files from a crashed write.
	q.cleanupTemps()
	return q, nil
}

func (q *Queue) cleanupTemps() {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), tmpExt) {
			_ = os.Remove(filepath.Join(q.dir, e.Name()))
		}
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
	// The directory changed, so a remembered run of "next oldest" names may
	// no longer be in order (an Enqueue can land before them after a clock
	// step, and a Purge can remove them).
	q.oldestRun = nil
}

// oldest returns the oldest queued item's filename, or "" if empty.
//
// Returns a short RUN of names, cached, rather than re-scanning per item. The
// underlying sortedFiles is os.ReadDir + sort over the whole queue directory
// under q.mu, and RunSender called it once per item drained — so clearing a
// 5000-item backlog cost 5000 scans of 5000 entries, serialised against every
// Enqueue, exactly when throughput matters most. The cache is dropped whenever
// the directory changes (invalidateDepth), and a name that has since been sent
// simply fails to open and is skipped.
func (q *Queue) oldest() string {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.oldestRun) > 0 {
		name := q.oldestRun[0]
		q.oldestRun = q.oldestRun[1:]
		if _, err := os.Stat(filepath.Join(q.dir, name)); err == nil {
			return name
		}
	}

	files := q.sortedFiles()
	if len(files) == 0 {
		return ""
	}
	// Keep the next few so the common case — draining in order — does not
	// re-scan for every one.
	const runLen = 64
	q.oldestRun = make([]string, 0, runLen)
	for i := 1; i < len(files) && i < runLen; i++ {
		q.oldestRun = append(q.oldestRun, files[i].name)
	}
	return files[0].name
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

// RunSender drains the queue in FIFO order until ctx is cancelled. It repeatedly
// takes the oldest item and calls send(): on SendOK/SendDrop the item is deleted
// and it continues immediately; on SendRetry it sleeps with exponential backoff
// (+jitter) and retries the same item. When the queue is empty it polls briefly.
func (q *Queue) RunSender(ctx context.Context, send func(contentType string, body []byte) SendResult) {
	backoff := backoffInitial
	for {
		if ctx.Err() != nil {
			return
		}

		name := q.oldest()
		if name == "" {
			// Empty — wait a bit before polling again.
			if !sleepCtx(ctx, 500*time.Millisecond) {
				return
			}
			backoff = backoffInitial
			continue
		}

		// Age bound: a call nobody could deliver inside maxItemAge is discarded
		// rather than retried forever. Checked before the read so an expired item
		// costs no I/O. Counted, not silent — see Expired().
		if nano := itemNano(name); nano > 0 && time.Since(time.Unix(0, nano)) > maxItemAge {
			q.remove(name)
			total := q.expired.Add(1)
			log.Printf("queue: discarding call older than %s (%d expired this run)", maxItemAge, total)
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
