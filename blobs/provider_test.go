package blobs

import (
	"bytes"
	"context"
	"io"
	"testing"

	"lukechampine.com/blake3/bao"
)

func TestMemStoreEntry(t *testing.T) {
	data := bytes.Repeat([]byte("a"), BlockSize+17)
	m, err := NewMemStore(data)
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	hash := NewHash(data)
	entry, err := m.Open(context.Background(), hash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := entry.Hash(); got != hash {
		t.Fatalf("Hash = %s, want %s", got, hash)
	}
	if size, verified := entry.Size(); size != uint64(len(data)) || !verified {
		t.Fatalf("Size = %d, %v, want %d, true", size, verified, len(data))
	}
	if !entry.IsComplete() {
		t.Fatal("IsComplete = false, want true")
	}

	r, err := entry.DataReader(context.Background())
	if err != nil {
		t.Fatalf("DataReader: %v", err)
	}
	got := make([]byte, len(data))
	if _, err := r.ReadAt(got, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadAt data: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data round trip mismatch")
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

func TestMapStoreServesBlob(t *testing.T) {
	data := []byte("provider map store")
	m, err := NewMemStore(data)
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	hash := NewHash(data)
	got, err := ReadBlob(context.Background(), m, hash)
	if err != nil {
		t.Fatalf("ReadBlob: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("GetBlob = %q, want %q", got, data)
	}

	client, server := newTestBidiStreamPair()
	errc := make(chan error, 1)
	go func() {
		errc <- ServeBlob(context.Background(), server, m)
	}()
	got, err = GetBlobBytes(context.Background(), client, hash)
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

func TestMemStoreHonorsCanceledContext(t *testing.T) {
	m, err := NewMemStore([]byte("x"))
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Open(ctx, NewHash([]byte("x"))); err == nil {
		t.Fatal("Get canceled context error = nil")
	}
}
