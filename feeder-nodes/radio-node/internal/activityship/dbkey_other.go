//go:build !linux && !windows

package activityship

import "os"

// statDBKey on platforms without a portable birth-time/inode accessor: the
// file's existence is confirmed but its generation is indistinguishable
// ("0"), so DB-recreation detection falls back to the lastId-regression rule
// alone. ok is false when the file can't be statted (typically not created
// yet) — callers must NOT treat that as a generation change.
func statDBKey(path string) (key string, ok bool) {
	if _, err := os.Stat(path); err != nil {
		return "", false
	}
	return "0", true
}
