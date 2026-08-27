package blobs

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"lukechampine.com/blake3/bao"
)

func TestFSStorePersistsBlob(t *testing.T) {
	dir := t.TempDir()
	data := bytes.Repeat([]byte("persistent blob"), 2000)

	store, err := NewFSStore(dir)
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	hash, err := store.Add(data)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	reopened, err := NewFSStore(dir)
	if err != nil {
		t.Fatalf("reopen NewFSStore: %v", err)
	}
	status, err := reopened.BlobStatus(context.Background(), hash)
	if err != nil {
		t.Fatalf("BlobStatus: %v", err)
	}
	if !status.IsComplete() || status.Size != int64(len(data)) {
		t.Fatalf("BlobStatus = %+v, want complete size %d", status, len(data))
	}
	got, err := ReadBlob(context.Background(), reopened, hash)
	if err != nil {
		t.Fatalf("ReadBlob: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("ReadBlob data mismatch")
	}

	entry, err := reopened.Open(context.Background(), hash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	r, err := entry.DataReader(context.Background())
	if err != nil {
		t.Fatalf("DataReader: %v", err)
	}
	buf := make([]byte, len(data))
	if _, err := r.ReadAt(buf, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadAt data: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("DataReader data mismatch")
	}
	outboard, err := entry.Outboard(context.Background())
	if err != nil {
		t.Fatalf("Outboard: %v", err)
	}
	out := make([]byte, outboard.Size())
	if _, err := outboard.ReadAt(out, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadAt outboard: %v", err)
	}
	if !bao.VerifyBuf(data, out, 4, hash.Bytes()) {
		t.Fatal("outboard does not verify data")
	}
}

func TestFSStoreServesBlob(t *testing.T) {
	store, err := NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	data := []byte("serve persistent blob")
	hash, err := store.Add(data)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	client, server := newTestBidiStreamPair()
	errc := make(chan error, 1)
	go func() {
		errc <- ServeBlob(context.Background(), server, store)
	}()
	got, err := GetBlobBytes(context.Background(), client, hash)
	if err != nil {
		t.Fatalf("GetBlobBytes: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("transfer = %q, want %q", got, data)
	}
	if err := <-errc; err != nil {
		t.Fatalf("ServeBlob: %v", err)
	}
}

func TestFSStoreImportFileCopy(t *testing.T) {
	store, err := NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	data := bytes.Repeat([]byte("import file copy"), 4096)
	path := writeTempBlobFile(t, data)

	hash, err := store.ImportFile(path, ImportCopy)
	if err != nil {
		t.Fatalf("ImportFile: %v", err)
	}
	if want := NewHash(data); hash != want {
		t.Fatalf("ImportFile hash = %s, want %s", hash, want)
	}
	got, err := ReadBlob(context.Background(), store, hash)
	if err != nil {
		t.Fatalf("ReadBlob: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("ReadBlob data mismatch")
	}
}

func TestFSStoreImportFileTryReference(t *testing.T) {
	store, err := NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	data := bytes.Repeat([]byte("import file reference"), 4096)
	path := writeTempBlobFile(t, data)

	hash, err := store.ImportFile(path, ImportTryReference)
	if err != nil {
		t.Fatalf("ImportFile: %v", err)
	}
	if want := NewHash(data); hash != want {
		t.Fatalf("ImportFile hash = %s, want %s", hash, want)
	}
	got, err := ReadBlob(context.Background(), store, hash)
	if err != nil {
		t.Fatalf("ReadBlob: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("ReadBlob data mismatch")
	}
}

func TestFSStoreHonorsCanceledContext(t *testing.T) {
	store, err := NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	hash, err := store.Add([]byte("x"))
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Open(ctx, hash); err == nil {
		t.Fatal("Get canceled context error = nil")
	}
}

func writeTempBlobFile(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blob.data")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestGCKeepsUncommittedBlob pins the invariant that FSStore.NewBlob relies on:
// an in-flight blob has no hash yet, so no tag can protect it, and GC must skip
// it because its temporary name does not parse as a Hash.
func TestGCKeepsUncommittedBlob(t *testing.T) {
	ctx := context.Background()
	store, err := NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	w, err := store.NewBlob(ctx)
	if err != nil {
		t.Fatalf("NewBlob: %v", err)
	}
	defer w.Close()
	data := []byte("still being written")
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if _, err := store.GC(ctx); err != nil {
		t.Fatalf("GC: %v", err)
	}

	tag, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit after GC: %v", err)
	}
	defer tag.Close()
	got, err := ReadBlob(ctx, store, tag.Hash())
	if err != nil {
		t.Fatalf("ReadBlob: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("blob = %q, want %q", got, data)
	}
}

// TestCloseAfterCommitIsNoop pins the database/sql.Tx contract documented on
// BlobWriter: defer w.Close() is correct whether or not Commit is reached.
func TestCloseAfterCommitIsNoop(t *testing.T) {
	ctx := context.Background()
	store, err := NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	w, err := store.NewBlob(ctx)
	if err != nil {
		t.Fatalf("NewBlob: %v", err)
	}
	data := []byte("committed")
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	tag, err := w.Commit()
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	defer tag.Close()
	if err := w.Close(); err != nil {
		t.Fatalf("Close after Commit = %v, want nil", err)
	}
	if _, err := ReadBlob(ctx, store, tag.Hash()); err != nil {
		t.Fatalf("blob gone after Close: %v", err)
	}
}
