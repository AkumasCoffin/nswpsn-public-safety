// Package rawlogship uploads SDR-Trunk's decoded-message logs to the backend.
//
// When staff start a raw message capture on a node, its config tells SDR-Trunk
// to write a decoded-message log for every channel (configapply sets the
// loggers). SDR-Trunk opens one file each time a channel starts and closes it
// when the channel stops, so a busy node produces a steady stream of finished
// files in <app root>/event_logs. This package gzips each finished file,
// POSTs it to ${server_url}/api/node-ingest/raw-log and deletes it.
//
// It needs no switch of its own: with logging off there are no files, and the
// loop does nothing. A file counts as finished once it has not been written
// to for a couple of minutes; an idle control channel's file therefore goes
// up in pieces only when the channel restarts, which is fine for a capture.
//
// The node's disk is protected twice: files are deleted as soon as the
// backend has them or refuses them, and if the directory still grows past a
// cap (the backend unreachable for a long time) the oldest files are dropped.
package rawlogship

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/AkumasCoffin/nswpsn-node/radio-node/internal/version"
)

const (
	pollInterval = time.Minute
	// quietFor: a file not written to for this long is treated as finished.
	quietFor = 2 * time.Minute
	// maxPerTick bounds one pass so a backlog is drained over several.
	maxPerTick = 40
	// maxRawBytes: a single log larger than this is dropped, not uploaded.
	// It would gzip to more than the backend accepts.
	maxRawBytes = 400 << 20
	// maxDirBytes caps the log directory when uploads are not getting through.
	maxDirBytes   = 2 << 30
	uploadTimeout = 5 * time.Minute
	logSuffix     = "_decoded_messages.log"
)

// Options configures a Shipper.
type Options struct {
	// Dir is SDR-Trunk's event log directory.
	Dir string
	// TempDir is where a file is gzipped before upload (the agent data dir).
	TempDir   string
	ServerURL string
	NodeToken string
	InstallID string
	// Now and Client are for tests.
	Now    func() time.Time
	Client *http.Client
}

// Shipper uploads finished log files.
type Shipper struct {
	opts Options
}

// New returns a Shipper. Call Run to start it.
func New(opts Options) *Shipper {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Client == nil {
		opts.Client = &http.Client{Timeout: uploadTimeout}
	}
	return &Shipper{opts: opts}
}

// Run uploads on a timer until ctx is cancelled.
func (s *Shipper) Run(ctx context.Context) {
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Tick(ctx)
		}
	}
}

type logFile struct {
	path    string
	size    int64
	modTime time.Time
}

// finished lists the log files that are no longer being written, oldest first.
func (s *Shipper) list() (all []logFile, finished []logFile) {
	entries, err := os.ReadDir(s.opts.Dir)
	if err != nil {
		return nil, nil // no directory: logging has never been on
	}
	cutoff := s.opts.Now().Add(-quietFor)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), logSuffix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		f := logFile{path: filepath.Join(s.opts.Dir, e.Name()), size: info.Size(), modTime: info.ModTime()}
		all = append(all, f)
		if info.ModTime().Before(cutoff) {
			finished = append(finished, f)
		}
	}
	byAge := func(fs []logFile) {
		sort.Slice(fs, func(i, j int) bool { return fs[i].modTime.Before(fs[j].modTime) })
	}
	byAge(all)
	byAge(finished)
	return all, finished
}

// Tick does one pass: upload what is finished, then enforce the directory cap.
func (s *Shipper) Tick(ctx context.Context) {
	_, finished := s.list()
	sent := 0
	for _, f := range finished {
		if sent >= maxPerTick || ctx.Err() != nil {
			break
		}
		if f.size == 0 || f.size > maxRawBytes {
			_ = os.Remove(f.path)
			continue
		}
		keep, err := s.upload(ctx, f)
		if err != nil {
			log.Printf("rawlogship: %s: %v", filepath.Base(f.path), err)
		}
		if keep {
			break // backend unreachable or busy: try again next pass
		}
		_ = os.Remove(f.path)
		sent++
	}
	s.enforceCap()
}

// enforceCap deletes the oldest log files while the directory is over its cap.
func (s *Shipper) enforceCap() {
	all, _ := s.list()
	var total int64
	for _, f := range all {
		total += f.size
	}
	for _, f := range all {
		if total <= maxDirBytes {
			return
		}
		if err := os.Remove(f.path); err == nil {
			total -= f.size
			log.Printf("rawlogship: log directory over its cap, dropped %s", filepath.Base(f.path))
		}
	}
}

// upload gzips one file and POSTs it. keep reports whether the file should be
// left for another attempt: true when the backend could not be reached or
// asked us to wait, false when it has the file or will never take it.
func (s *Shipper) upload(ctx context.Context, f logFile) (keep bool, err error) {
	gz, size, err := s.compress(f.path)
	if err != nil {
		return true, fmt.Errorf("compress: %w", err)
	}
	defer os.Remove(gz)

	body, err := os.Open(gz)
	if err != nil {
		return true, err
	}
	defer body.Close()

	u := strings.TrimRight(s.opts.ServerURL, "/") + "/api/node-ingest/raw-log?name=" + url.QueryEscape(filepath.Base(f.path))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, body)
	if err != nil {
		return true, err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/gzip")
	req.Header.Set("X-Node-Token", s.opts.NodeToken)
	req.Header.Set("X-Node-Install", s.opts.InstallID)
	req.Header.Set("User-Agent", version.UserAgent())

	resp, err := s.opts.Client.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return false, nil
	case resp.StatusCode == http.StatusConflict:
		// No capture is running and none just ended: the backend will never
		// want this file.
		return false, fmt.Errorf("backend is not capturing, discarded")
	case resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusRequestEntityTooLarge:
		return false, fmt.Errorf("backend refused it (status %d), discarded", resp.StatusCode)
	default:
		return true, fmt.Errorf("status %d", resp.StatusCode)
	}
}

// compress writes a gzip copy of path into TempDir and returns its path and size.
func (s *Shipper) compress(path string) (string, int64, error) {
	in, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer in.Close()
	out, err := os.CreateTemp(s.opts.TempDir, "rawlog-*.gz")
	if err != nil {
		return "", 0, err
	}
	zw := gzip.NewWriter(out)
	_, err = io.Copy(zw, in)
	if cerr := zw.Close(); err == nil {
		err = cerr
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(out.Name())
		return "", 0, err
	}
	info, err := os.Stat(out.Name())
	if err != nil {
		os.Remove(out.Name())
		return "", 0, err
	}
	return out.Name(), info.Size(), nil
}
