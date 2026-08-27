package blobs

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestFSStoreConcurrentStress(t *testing.T) {
	store, err := NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}

	const workers = 12
	const rounds = 32
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				data := []byte(fmt.Sprintf("worker=%02d round=%02d", worker, i))
				// Write through the protected path: Commit installs the blob
				// and its temp tag under one lock, so no concurrent GC can
				// collect it before this worker holds a claim on it.
				w, err := store.NewBlob(context.Background())
				if err != nil {
					t.Errorf("NewBlob: %v", err)
					return
				}
				if _, err := w.Write(data); err != nil {
					_ = w.Close()
					t.Errorf("Write: %v", err)
					return
				}
				temp, err := w.Commit()
				if err != nil {
					_ = w.Close()
					t.Errorf("Commit: %v", err)
					return
				}
				hash := temp.Hash()
				value := RawHash(hash)
				tag := fmt.Sprintf("worker-%02d", worker)
				if err := store.SetTag(tag, value); err != nil {
					t.Errorf("SetTag: %v", err)
					_ = temp.Close()
					return
				}
				if _, err := store.BlobStatus(context.Background(), hash); err != nil {
					t.Errorf("BlobStatus: %v", err)
					_ = temp.Close()
					return
				}
				// The worker holds a temp tag on hash for this whole round, so
				// a concurrent GC must not be able to collect it. Any error
				// here, absence included, is a failure.
				if _, err := store.Open(context.Background(), hash); err != nil {
					t.Errorf("Open: %v", err)
					_ = temp.Close()
					return
				}
				if _, err := store.Tags(); err != nil {
					t.Errorf("Tags: %v", err)
					_ = temp.Close()
					return
				}
				if _, err := store.GC(context.Background()); err != nil {
					t.Errorf("GC: %v", err)
					_ = temp.Close()
					return
				}
				if err := temp.Close(); err != nil {
					t.Errorf("TempTag.Close: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
