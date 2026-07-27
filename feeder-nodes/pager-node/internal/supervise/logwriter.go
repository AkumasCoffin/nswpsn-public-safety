package supervise

import (
	"os"
	"sync"
)

// logWriter is a minimal append writer that rotates the file to "<path>.1" once
// it grows past logMaxBytes, keeping exactly one previous generation. It is safe
// for concurrent writes (stdout+stderr both point at it).
type logWriter struct {
	path string
	mu   sync.Mutex
	f    *os.File
	size int64
}

func newLogWriter(path string) (*logWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &logWriter{path: path, f: f, size: info.Size()}, nil
}

func (w *logWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.size+int64(len(p)) > logMaxBytes {
		w.rotate()
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	return n, err
}

// rotate closes the current file, renames it to <path>.1 (replacing any prior
// rotation), and opens a fresh file. Caller must hold w.mu.
func (w *logWriter) rotate() {
	_ = w.f.Close()
	_ = os.Remove(w.path + ".1")
	_ = os.Rename(w.path, w.path+".1")

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		// Reopen original in append mode as a fallback; keep writing.
		f, _ = os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	}
	w.f = f
	w.size = 0
}

func (w *logWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
