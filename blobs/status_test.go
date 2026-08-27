package blobs

import (
	"context"
	"testing"
)

func TestStatus(t *testing.T) {
	data := []byte("stored blob")
	hash := NewHash(data)
	store := mustStore(t, data)

	tests := []struct {
		name string
		hash Hash
		want BlobStatus
	}{
		{name: "complete", hash: hash, want: CompleteBlobStatus(int64(len(data)))},
		{name: "empty", hash: EmptyHash, want: CompleteBlobStatus(0)},
		{name: "missing", hash: NewHash([]byte("missing")), want: NotFoundBlobStatus()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Status(context.Background(), store, tt.hash)
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if got != tt.want {
				t.Fatalf("Status = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestStatusUsesStater(t *testing.T) {
	hash := NewHash([]byte("partial"))
	store := statusStore{hash: hash, status: PartialBlobStatus(123)}

	got, err := Status(context.Background(), store, hash)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got != store.status {
		t.Fatalf("Status = %+v, want %+v", got, store.status)
	}
}

type statusStore struct {
	hash   Hash
	status BlobStatus
}

func (s statusStore) Open(context.Context, Hash) (Blob, error) { return nil, ErrBlobNotFound }

func (s statusStore) BlobStatus(_ context.Context, hash Hash) (BlobStatus, error) {
	if hash != s.hash {
		return NotFoundBlobStatus(), nil
	}
	return s.status, nil
}
