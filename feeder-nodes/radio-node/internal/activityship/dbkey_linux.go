//go:build linux

package activityship

import (
	"fmt"
	"os"
	"syscall"
)

// statDBKey derives a stable identity for the database file generation from
// its device + inode: recreating the file (vce --fresh after a wipe) allocates
// a new inode even at the same path. ok is false when the file can't be
// statted (typically not created yet) — callers must NOT treat that as a
// generation change.
func statDBKey(path string) (key string, ok bool) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", false
	}
	if st, isStat := fi.Sys().(*syscall.Stat_t); isStat {
		return fmt.Sprintf("dev%d-ino%d", st.Dev, st.Ino), true
	}
	return "0", true
}
