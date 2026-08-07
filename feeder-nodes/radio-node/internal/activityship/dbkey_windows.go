//go:build windows

package activityship

import (
	"fmt"
	"os"
	"syscall"
)

// statDBKey derives a stable identity for the database file generation from
// its creation (birth) time: recreating the file gets a fresh creation time
// while mere writes/renames keep it. ok is false when the file can't be
// statted (typically not created yet) — callers must NOT treat that as a
// generation change.
func statDBKey(path string) (key string, ok bool) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", false
	}
	if st, isStat := fi.Sys().(*syscall.Win32FileAttributeData); isStat {
		return fmt.Sprintf("btime%d", st.CreationTime.Nanoseconds()), true
	}
	return "0", true
}
