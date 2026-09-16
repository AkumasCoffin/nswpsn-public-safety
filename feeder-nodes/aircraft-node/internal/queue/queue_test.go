package queue

import (
	"os"
	"path/filepath"
	"testing"
)

// A host runs one node kind at a time but can be switched between them by
// re-running the other installer, which reuses this data dir. The radio and
// pager agents both name their items .call, so their leftovers are picked up,
// sent, refused by the backend's kind gate and deleted — their queues clean
// themselves. This agent's items are .snapshot, so without an explicit sweep
// a switched host would carry .call files it can never see or send.
func TestOpenDiscardsAnotherAgentsItems(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "00000000000000000001-abcd.call")
	if err := os.WriteFile(stale, []byte("application/json\n\n{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	q, err := Open(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("a .call item from another agent should have been discarded, stat err = %v", err)
	}
	if d := q.Depth(); d != 0 {
		t.Fatalf("depth = %d, want 0", d)
	}
}

func TestOpenKeepsOurOwnItems(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue("application/json", []byte(`{"at":"now"}`)); err != nil {
		t.Fatal(err)
	}
	if d := q.Depth(); d != 1 {
		t.Fatalf("depth after enqueue = %d, want 1", d)
	}
	// Reopening must not eat what this agent just wrote — the sweep keys on the
	// extension, so an over-broad match would silently empty a live queue.
	q2, err := Open(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if d := q2.Depth(); d != 1 {
		t.Fatalf("depth after reopen = %d, want 1", d)
	}
}

func TestOpenStillClearsCrashedWrites(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, "00000000000000000002-efgh.snapshot.tmp")
	if err := os.WriteFile(tmp, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file from a crashed write should be removed, stat err = %v", err)
	}
}
