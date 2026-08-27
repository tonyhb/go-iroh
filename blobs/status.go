package blobs

import (
	"context"
	"errors"
)

// BlobState is the local storage state of a blob.
type BlobState uint8

const (
	// BlobNotFound means the blob is not stored.
	BlobNotFound BlobState = iota
	// BlobPartial means the blob is only partially stored.
	BlobPartial
	// BlobComplete means the blob is stored completely.
	BlobComplete
)

const unknownBlobSize int64 = -1

// BlobStatus describes the local storage state of a blob.
//
// Size is -1 when the size is unknown.
type BlobStatus struct {
	State BlobState
	Size  int64
}

// NotFoundBlobStatus returns the status for a missing blob.
func NotFoundBlobStatus() BlobStatus { return BlobStatus{State: BlobNotFound, Size: unknownBlobSize} }

// PartialBlobStatus returns the status for a partial blob.
func PartialBlobStatus(size int64) BlobStatus {
	if size < 0 {
		size = unknownBlobSize
	}
	return BlobStatus{State: BlobPartial, Size: size}
}

// CompleteBlobStatus returns the status for a complete blob.
func CompleteBlobStatus(size int64) BlobStatus {
	if size < 0 {
		size = 0
	}
	return BlobStatus{State: BlobComplete, Size: size}
}

// SizeKnown reports whether s has a known size.
func (s BlobStatus) SizeKnown() bool { return s.Size >= 0 }

// IsComplete reports whether s describes a complete blob.
func (s BlobStatus) IsComplete() bool { return s.State == BlobComplete }

// IsPartial reports whether s describes a partial blob.
func (s BlobStatus) IsPartial() bool { return s.State == BlobPartial }

// IsNotFound reports whether s describes a missing blob.
func (s BlobStatus) IsNotFound() bool { return s.State == BlobNotFound }

// Status reports the local storage status for hash in store.
//
// Status uses [Stater] when store implements it, and otherwise falls back to
// [Store.Open]. Both paths report the same status; the upgrade changes only
// what the query costs.
//
// A missing blob is [NotFoundBlobStatus] with a nil error. A non-nil error
// means the status could not be determined, which is not the same as the blob
// being absent.
func Status(ctx context.Context, store Store, hash Hash) (BlobStatus, error) {
	if hash == EmptyHash {
		return CompleteBlobStatus(0), nil
	}
	if store == nil {
		return NotFoundBlobStatus(), nil
	}
	if st, ok := store.(Stater); ok {
		return st.BlobStatus(ctx, hash)
	}
	blob, err := store.Open(ctx, hash)
	if errors.Is(err, ErrBlobNotFound) {
		return NotFoundBlobStatus(), nil
	}
	if err != nil {
		return NotFoundBlobStatus(), err
	}
	size, verified := blob.Size()
	if !verified || size > maxInt64 {
		return PartialBlobStatus(unknownBlobSize), nil
	}
	if !blob.IsComplete() {
		return PartialBlobStatus(int64(size)), nil
	}
	return CompleteBlobStatus(int64(size)), nil
}
