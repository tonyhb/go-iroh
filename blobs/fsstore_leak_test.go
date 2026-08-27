package blobs

import (
	"context"
	"os"
	"testing"
)

// TestCommitFailureLeavesNoTempTag pins an invariant that is invisible from
// outside the package: a Commit that fails must not leave a temp tag behind.
//
// finishImport creates the tag and installs the files under one lock. If the
// tag were created before the installs, a failing rename would return a nil
// tag to the caller while leaving the entry in s.temp, where nothing can ever
// release it: the caller has no handle, and TempTag.Close takes s.mu, so it
// cannot be closed from inside the locked region either. Every failed Commit
// would pin a hash as a GC root for the life of the process.
func TestCommitFailureLeavesNoTempTag(t *testing.T) {
	ctx := context.Background()
	store, err := NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	data := []byte("content whose install will fail")

	// A directory where the outboard file belongs makes the second rename fail
	// after the first has succeeded.
	hash := NewHash(data)
	if err := os.MkdirAll(store.outboardPath(hash), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	w, err := store.NewBlob(ctx)
	if err != nil {
		t.Fatalf("NewBlob: %v", err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := w.Commit(); err == nil {
		t.Fatal("Commit succeeded, want install failure")
	}
	_ = w.Close()

	store.mu.RLock()
	leaked := len(store.temp)
	store.mu.RUnlock()
	if leaked != 0 {
		t.Fatalf("after failed Commit: %d temp tag(s) still registered and unreleasable, want 0", leaked)
	}
}
