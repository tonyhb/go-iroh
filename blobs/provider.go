package blobs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"lukechampine.com/blake3/bao"
)

// Store holds blobs addressable by root hash.
//
// Open reports a missing blob as [ErrBlobNotFound]. A Store may also implement
// [Stater] to answer status queries without opening the blob, and [Sink] to
// accept new blobs; callers should use the package-level [Status] and
// [WriteBlob] rather than asserting for those interfaces themselves.
type Store interface {
	Open(ctx context.Context, hash Hash) (Blob, error)
}

// Blob is one stored blob, complete or partial.
type Blob interface {
	Hash() Hash
	Size() (size uint64, verified bool)
	IsComplete() bool
	DataReader(ctx context.Context) (BlobReader, error)
	Outboard(ctx context.Context) (Outboard, error)
}

// BlobReader reads a blob's data at arbitrary offsets. Callers must close it:
// a filesystem-backed store hands out an open file, and a caller that does not
// close it leaks a descriptor per read.
type BlobReader interface {
	io.ReaderAt
	io.Closer
}

// Outboard is a BAO outboard reader. Callers must close it, for the same
// reason as [BlobReader].
type Outboard interface {
	io.ReaderAt
	io.Closer
	Size() int64
}

// Stater is an optional [Store] upgrade that reports blob status without
// opening the blob. Use [Status], which falls back to Open.
//
// A Stater must report the same status Open would: an upgrade may change what
// a status query costs, never what it answers.
type Stater interface {
	BlobStatus(ctx context.Context, hash Hash) (BlobStatus, error)
}

// Sink is an optional [Store] upgrade that accepts new blobs. Use [WriteBlob]
// or [ReadFromBlob], which report a Store that cannot add content.
type Sink interface {
	NewBlob(ctx context.Context) (BlobWriter, error)
}

// BlobWriter accumulates one blob. Write the content, then call Commit to
// store it and learn its root hash.
//
// Commit returns a [TempTag] rather than a bare [Hash] because a blob is
// collectible the instant it is stored. A caller that received only a hash
// could not protect the blob it names: the hash does not exist until the blob
// does, so every add-then-protect sequence has a window in which a concurrent
// [FSStore.GC] may delete the blob, and the tag the caller then creates names a
// file that is already gone. Commit closes that window by installing the blob
// and its tag together. Close the tag once durable ownership is established,
// usually by naming the blob with [FSStore.SetTag]:
//
//	tag, err := w.Commit()
//	if err != nil {
//		return err
//	}
//	defer tag.Close()
//
// Close releases the writer's resources. Close after a successful Commit is a
// no-op, so
//
//	w, err := s.NewBlob(ctx)
//	if err != nil {
//		return err
//	}
//	defer w.Close()
//
// is always correct, whether or not Commit is reached. A writer that is closed
// without committing discards its content.
//
// An uncommitted writer has no hash yet, so it cannot be protected by a
// TempTag. Implementations must keep in-flight content out of reach of GC by
// other means; see [FSStore.NewBlob] for how the filesystem store does it.
type BlobWriter interface {
	io.Writer
	Commit() (*TempTag, error)
	Close() error
}

// ReadBlob returns the complete contents of hash from s.
//
// ReadBlob buffers the whole blob in memory. Prefer [Store.Open] and the
// returned [Blob]'s DataReader for blobs of unknown size.
func ReadBlob(ctx context.Context, s Store, hash Hash) ([]byte, error) {
	blob, err := s.Open(ctx, hash)
	if err != nil {
		return nil, err
	}
	if !blob.IsComplete() {
		return nil, ErrBlobNotFound
	}
	size, verified := blob.Size()
	if !verified {
		return nil, ErrBlobNotFound
	}
	if size > maxInt64 {
		return nil, ErrUnsupportedRequest
	}
	r, err := blob.DataReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	// ReadFull rather than ReadAt: ReadAt reports a short read as io.EOF, so
	// discarding that error would return a zero-padded buffer as success when
	// the stored data is shorter than the size Open reported.
	data := make([]byte, size)
	if _, err := io.ReadFull(io.NewSectionReader(r, 0, int64(size)), data); err != nil {
		return nil, fmt.Errorf("blobs: read data: %w", err)
	}
	return data, nil
}

// WriteBlob stores data in s and returns its root hash.
//
// WriteBlob releases the blob's [TempTag] before returning, so the blob is
// unprotected by the time the caller sees its hash. Callers that run
// [FSStore.GC] concurrently should use [Sink.NewBlob] and hold the tag Commit
// returns.
func WriteBlob(ctx context.Context, s Sink, data []byte) (Hash, error) {
	return ReadFromBlob(ctx, s, bytes.NewReader(data))
}

// ReadFromBlob streams r into s and returns the stored blob's root hash.
//
// ReadFromBlob does not buffer the whole blob. It releases the blob's [TempTag]
// before returning; see [WriteBlob] for what that means for callers that
// collect garbage.
func ReadFromBlob(ctx context.Context, s Sink, r io.Reader) (Hash, error) {
	w, err := s.NewBlob(ctx)
	if err != nil {
		return Hash{}, err
	}
	defer w.Close()
	if _, err := io.Copy(w, r); err != nil {
		return Hash{}, err
	}
	tag, err := w.Commit()
	if err != nil {
		return Hash{}, err
	}
	defer tag.Close()
	return tag.Hash(), nil
}

// MemStore is an in-memory [Store] holding complete raw blobs.
type MemStore struct {
	mu      sync.RWMutex
	entries map[Hash]*MemBlob
}

// NewMemStore returns an in-memory store holding data as complete raw blobs.
func NewMemStore(blobs ...[]byte) (*MemStore, error) {
	m := &MemStore{entries: make(map[Hash]*MemBlob)}
	for _, data := range blobs {
		if _, err := m.Add(data); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Add stores data as a complete raw blob and returns its hash.
func (m *MemStore) Add(data []byte) (Hash, error) {
	if m == nil {
		return Hash{}, errors.New("blobs: nil store")
	}
	entry := NewMemBlob(data)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = make(map[Hash]*MemBlob)
	}
	m.entries[entry.hash] = entry
	return entry.hash, nil
}

// Open returns the blob for hash, or [ErrBlobNotFound].
func (m *MemStore) Open(ctx context.Context, hash Hash) (Blob, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, ErrBlobNotFound
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.entries[hash]
	if !ok {
		return nil, ErrBlobNotFound
	}
	return entry, nil
}

// NewBlob returns a writer that stores a blob in memory on Commit.
func (m *MemStore) NewBlob(ctx context.Context) (BlobWriter, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("blobs: nil store")
	}
	return &memBlobWriter{m: m}, nil
}

type memBlobWriter struct {
	m    *MemStore
	buf  bytes.Buffer
	done bool
}

func (w *memBlobWriter) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("blobs: write to finished blob")
	}
	return w.buf.Write(p)
}

func (w *memBlobWriter) Commit() (*TempTag, error) {
	if w.done {
		return nil, errors.New("blobs: commit after commit")
	}
	hash, err := w.m.Add(w.buf.Bytes())
	if err != nil {
		return nil, err
	}
	w.done = true
	w.buf.Reset()
	// A MemStore never collects, so the tag protects nothing and holds no store.
	return &TempTag{value: RawHash(hash)}, nil
}

func (w *memBlobWriter) Close() error {
	w.done = true
	w.buf.Reset()
	return nil
}

// MemBlob is a complete in-memory raw blob.
type MemBlob struct {
	hash     Hash
	data     []byte
	outboard []byte
}

// NewMemBlob returns a complete in-memory raw blob holding data.
func NewMemBlob(data []byte) *MemBlob {
	outboard, root := bao.EncodeBuf(data, 4, true)
	return &MemBlob{
		hash:     Hash(root),
		data:     append([]byte(nil), data...),
		outboard: outboard,
	}
}

// Hash returns e's root hash.
func (e *MemBlob) Hash() Hash { return e.hash }

// Size returns e's verified data size.
func (e *MemBlob) Size() (uint64, bool) { return uint64(len(e.data)), true }

// IsComplete reports whether e has all data.
func (e *MemBlob) IsComplete() bool { return true }

// DataReader returns an immutable reader for e's data.
func (e *MemBlob) DataReader(ctx context.Context) (BlobReader, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return byteReader{Reader: bytes.NewReader(e.data)}, nil
}

// Outboard returns an immutable reader for e's BAO outboard.
func (e *MemBlob) Outboard(ctx context.Context) (Outboard, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return byteOutboard{byteReader: byteReader{Reader: bytes.NewReader(e.outboard)}, size: int64(len(e.outboard))}, nil
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

// byteReader adapts a bytes.Reader to the closable reader interfaces. Nothing
// needs releasing, so Close is a no-op.
type byteReader struct {
	*bytes.Reader
}

func (byteReader) Close() error { return nil }

type byteOutboard struct {
	byteReader
	size int64
}

func (o byteOutboard) Size() int64 { return o.size }
