package blobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"lukechampine.com/blake3/bao"
)

// FSStore is a filesystem-backed store for complete raw blobs.
type FSStore struct {
	dir     string
	dataDir string
	baoDir  string
	tags    map[string]HashAndFormat
	temp    map[uint64]HashAndFormat
	nextTag uint64
	mu      sync.RWMutex
}

// NewFSStore opens or creates a filesystem-backed blob store rooted at dir.
func NewFSStore(dir string) (*FSStore, error) {
	if dir == "" {
		return nil, errors.New("blobs: empty store directory")
	}
	s := &FSStore{
		dir:     dir,
		dataDir: filepath.Join(dir, "data"),
		baoDir:  filepath.Join(dir, "outboard"),
	}
	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("blobs: create data dir: %w", err)
	}
	if err := os.MkdirAll(s.baoDir, 0o755); err != nil {
		return nil, fmt.Errorf("blobs: create outboard dir: %w", err)
	}
	if err := s.loadTags(); err != nil {
		return nil, err
	}
	return s, nil
}

// Add stores data as a complete raw blob and returns its hash.
//
// The blob is unprotected when Add returns, so a [FSStore.GC] running
// concurrently may collect it before the caller can name it with
// [FSStore.SetTag]. Callers that collect garbage should use [FSStore.NewBlob]
// and hold the [TempTag] that [BlobWriter.Commit] returns.
func (s *FSStore) Add(data []byte) (Hash, error) {
	if s == nil {
		return Hash{}, errors.New("blobs: nil fs store")
	}
	outboard, root := bao.EncodeBuf(data, 4, true)
	hash := Hash(root)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := writeFileAtomic(s.dataPath(hash), data, 0o644); err != nil {
		return Hash{}, fmt.Errorf("blobs: write data: %w", err)
	}
	if err := writeFileAtomic(s.outboardPath(hash), outboard, 0o644); err != nil {
		_ = os.Remove(s.dataPath(hash))
		return Hash{}, fmt.Errorf("blobs: write outboard: %w", err)
	}
	return hash, nil
}

// Open returns the blob for hash, or [ErrBlobNotFound].
func (s *FSStore) Open(ctx context.Context, hash Hash) (Blob, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, ErrBlobNotFound
	}
	if hash == EmptyHash {
		return NewMemBlob(nil), nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, err := os.Stat(s.dataPath(hash))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrBlobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blobs: stat data: %w", err)
	}
	return fsEntry{store: s, hash: hash, size: uint64(info.Size())}, nil
}

// NewBlob returns a writer that streams content to a temporary file and
// installs it under its root hash on Commit.
//
// The temporary file is named so that it cannot parse as a [Hash], which is
// what keeps in-flight content invisible to [FSStore.GC]: GC walks the data
// directory and skips every name that [ParseHash] rejects. An uncommitted blob
// has no hash yet and so cannot be protected by a [TempTag].
func (s *FSStore) NewBlob(ctx context.Context) (BlobWriter, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errors.New("blobs: nil store")
	}
	tmp, err := os.CreateTemp(s.dataDir, importTempPrefix+"*")
	if err != nil {
		return nil, fmt.Errorf("blobs: create blob data: %w", err)
	}
	return &fsBlobWriter{store: s, tmp: tmp}, nil
}

type fsBlobWriter struct {
	store *FSStore
	tmp   *os.File
	done  bool
}

func (w *fsBlobWriter) Write(p []byte) (int, error) {
	if w.done {
		return 0, errors.New("blobs: write to finished blob")
	}
	return w.tmp.Write(p)
}

func (w *fsBlobWriter) Commit() (*TempTag, error) {
	if w.done {
		return nil, errors.New("blobs: commit after commit")
	}
	if err := w.tmp.Sync(); err != nil {
		return nil, fmt.Errorf("blobs: sync blob data: %w", err)
	}
	if _, err := w.tmp.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("blobs: rewind blob data: %w", err)
	}
	name := w.tmp.Name()
	_, tag, err := w.store.finishImport(w.tmp, false, true)
	if err != nil {
		return nil, err
	}
	w.done = true
	removeTemp(name)
	return tag, nil
}

func (w *fsBlobWriter) Close() error {
	if w.done {
		return nil
	}
	w.done = true
	name := w.tmp.Name()
	err := w.tmp.Close()
	removeTemp(name)
	if errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}

// BlobStatus reports the local storage state for hash.
func (s *FSStore) BlobStatus(ctx context.Context, hash Hash) (BlobStatus, error) {
	if err := ctxErr(ctx); err != nil {
		return NotFoundBlobStatus(), err
	}
	if hash == EmptyHash {
		return CompleteBlobStatus(0), nil
	}
	if s == nil {
		return NotFoundBlobStatus(), nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, err := os.Stat(s.dataPath(hash))
	if errors.Is(err, os.ErrNotExist) {
		return NotFoundBlobStatus(), nil
	}
	if err != nil {
		return NotFoundBlobStatus(), fmt.Errorf("blobs: stat data: %w", err)
	}
	return CompleteBlobStatus(info.Size()), nil
}

func (s *FSStore) dataPath(hash Hash) string {
	return filepath.Join(s.dataDir, hash.String())
}

func (s *FSStore) outboardPath(hash Hash) string {
	return filepath.Join(s.baoDir, hash.String())
}

func (s *FSStore) tagsPath() string {
	return filepath.Join(s.dir, "tags.json")
}

type fsEntry struct {
	store *FSStore
	hash  Hash
	size  uint64
}

func (e fsEntry) Hash() Hash { return e.hash }

func (e fsEntry) Size() (uint64, bool) { return e.size, true }

func (e fsEntry) IsComplete() bool { return true }

func (e fsEntry) DataReader(ctx context.Context) (BlobReader, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	e.store.mu.RLock()
	defer e.store.mu.RUnlock()
	f, err := os.Open(e.store.dataPath(e.hash))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrBlobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blobs: open data: %w", err)
	}
	return f, nil
}

func (e fsEntry) Outboard(ctx context.Context) (Outboard, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	e.store.mu.RLock()
	defer e.store.mu.RUnlock()
	f, err := os.Open(e.store.outboardPath(e.hash))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrBlobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blobs: open outboard: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("blobs: stat outboard: %w", err)
	}
	return fileOutboard{File: f, size: info.Size()}, nil
}

type fileOutboard struct {
	*os.File
	size int64
}

func (o fileOutboard) Size() int64 { return o.size }

func writeFileAtomic(name string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(name), "."+filepath.Base(name)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	if err := os.Rename(tmpName, name); err != nil {
		return err
	}
	ok = true
	return nil
}

func (s *FSStore) loadTags() error {
	s.tags = make(map[string]HashAndFormat)
	s.temp = make(map[uint64]HashAndFormat)
	data, err := os.ReadFile(s.tagsPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("blobs: read tags: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &s.tags); err != nil {
		return fmt.Errorf("blobs: decode tags: %w", err)
	}
	return nil
}

func (s *FSStore) saveTagsLocked() error {
	data, err := json.MarshalIndent(s.tags, "", "\t")
	if err != nil {
		return fmt.Errorf("blobs: encode tags: %w", err)
	}
	return writeFileAtomic(s.tagsPath(), data, 0o644)
}
