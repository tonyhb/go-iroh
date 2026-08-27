package blobs_test

import (
	"bytes"
	"context"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/tmc/go-iroh/blobs"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

func TestBlobTransferIroh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	data := testData(blobs.BlockSize + 1)
	hash := blobs.NewHash(data)

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatalf("bind server: %v", err)
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		blobs.ALPN: iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
			s, err := conn.AcceptStream(ctx)
			if err != nil {
				return err
			}
			return blobs.ServeBlob(ctx, s, mustBlobStore(t, data))
		}),
	}, nil)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	defer router.Shutdown(ctx)

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatalf("bind client: %v", err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, blobs.ALPN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.CloseWithError(0, "")

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	got, err := blobs.GetBlobBytes(ctx, s, hash)
	if err != nil {
		t.Fatalf("get blob: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("data length = %d, want %d", len(got), len(data))
	}
}

func TestDownloadBlobIrohStreamsLargeBlob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	data := testData(8 << 20)
	hash := blobs.NewHash(data)
	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
	)
	if err != nil {
		t.Fatalf("bind server: %v", err)
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		blobs.ALPN: iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
			s, err := conn.AcceptStream(ctx)
			if err != nil {
				return err
			}
			return blobs.ServeBlob(ctx, s, mustBlobStore(t, data))
		}),
	}, nil)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	defer router.Shutdown(ctx)

	client, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
	)
	if err != nil {
		t.Fatalf("bind client: %v", err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, blobs.ALPN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.CloseWithError(0, "")

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := s.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatalf("set stream deadline: %v", err)
	}
	var got bytes.Buffer
	start := time.Now()
	if err := blobs.DownloadBlob(ctx, s, hash, &got); err != nil {
		t.Fatalf("download blob: %v", err)
	}
	if !bytes.Equal(got.Bytes(), data) {
		t.Fatalf("downloaded %d bytes, want %d", got.Len(), len(data))
	}
	mbps := float64(len(data)) / 1024 / 1024 / time.Since(start).Seconds()
	t.Logf("downloaded %d bytes at %.1f MiB/s", len(data), mbps)
}

func TestDownloadBlobRangeIrohResumesLargeBlob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store, err := blobs.NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	data := testData(8<<20 + 123)
	hash, err := store.Add(data)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
	)
	if err != nil {
		t.Fatalf("bind server: %v", err)
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		blobs.ALPN: iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
			s, err := conn.AcceptStream(ctx)
			if err != nil {
				return err
			}
			return blobs.ServeBlob(ctx, s, store)
		}),
	}, nil)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	defer router.Shutdown(ctx)

	client, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
	)
	if err != nil {
		t.Fatalf("bind client: %v", err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	var got bytes.Buffer
	const resumeAt = 4 << 20
	start := time.Now()
	downloadRange := func(offset, length uint64) {
		t.Helper()
		conn, err := client.Connect(ctx, addr, blobs.ALPN)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer conn.CloseWithError(0, "")
		s, err := conn.OpenStreamSync(ctx)
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}
		if err := s.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
			t.Fatalf("set stream deadline: %v", err)
		}
		if err := blobs.DownloadBlobRange(ctx, s, hash, offset, length, &got); err != nil {
			t.Fatalf("download range [%d,%d): %v", offset, offset+length, err)
		}
	}
	downloadRange(0, resumeAt)
	downloadRange(resumeAt, uint64(len(data)-resumeAt))

	if !bytes.Equal(got.Bytes(), data) {
		t.Fatalf("resumed download wrote %d bytes, want %d", got.Len(), len(data))
	}
	if blobs.NewHash(got.Bytes()) != hash {
		t.Fatal("resumed download hash mismatch")
	}
	mbps := float64(len(data)) / 1024 / 1024 / time.Since(start).Seconds()
	t.Logf("resumed %d bytes at %.1f MiB/s", len(data), mbps)
}

func TestDownloadBlobParallelIrohLargeBlob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store, err := blobs.NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	data := testData(8<<20 + 123)
	hash, err := store.Add(data)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
	)
	if err != nil {
		t.Fatalf("bind server: %v", err)
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		blobs.ALPN: iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
			s, err := conn.AcceptStream(ctx)
			if err != nil {
				return err
			}
			return blobs.ServeBlob(ctx, s, store)
		}),
	}, nil)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	defer router.Shutdown(ctx)

	client, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
	)
	if err != nil {
		t.Fatalf("bind client: %v", err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	open := func(ctx context.Context) (blobs.BidiStream, error) {
		conn, err := client.Connect(ctx, addr, blobs.ALPN)
		if err != nil {
			return nil, err
		}
		s, err := conn.OpenStreamSync(ctx)
		if err != nil {
			_ = conn.CloseWithError(0, "")
			return nil, err
		}
		if err := s.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
			_ = conn.CloseWithError(0, "")
			return nil, err
		}
		return &irohRangeStream{Stream: s, conn: conn}, nil
	}

	out := newWriterAt(len(data))
	start := time.Now()
	if err := blobs.DownloadBlobParallel(ctx, open, hash, out, blobs.ParallelDownloadOptions{
		Size:        uint64(len(data)),
		RangeSize:   4 << 20,
		Parallelism: 2,
		Retries:     1,
	}); err != nil {
		t.Fatalf("download parallel: %v", err)
	}
	got := out.Bytes()
	if !bytes.Equal(got, data) {
		t.Fatalf("parallel download wrote %d bytes, want %d", len(got), len(data))
	}
	if blobs.NewHash(got) != hash {
		t.Fatal("parallel download hash mismatch")
	}
	mbps := float64(len(data)) / 1024 / 1024 / time.Since(start).Seconds()
	t.Logf("parallel fetched %d bytes at %.1f MiB/s; baselines: single-range 43.3 MiB/s, full-stream 171 MiB/s", len(data), mbps)
}

func TestServeBlobStreamsParallelIrohLargeBlob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store, err := blobs.NewFSStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFSStore: %v", err)
	}
	data := testData(8<<20 + 123)
	hash, err := store.Add(data)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatalf("bind server: %v", err)
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		blobs.ALPN: iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
			return blobs.ServeBlobStreams(ctx, func(ctx context.Context) (blobs.BidiStream, error) {
				return conn.AcceptStream(ctx)
			}, store)
		}),
	}, nil)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	defer router.Shutdown(ctx)

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatalf("bind client: %v", err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, blobs.ALPN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.CloseWithError(0, "")

	open := func(ctx context.Context) (blobs.BidiStream, error) {
		s, err := conn.OpenStreamSync(ctx)
		if err != nil {
			return nil, err
		}
		if err := s.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
			_ = s.Close()
			return nil, err
		}
		return sharedIrohRangeStream{Stream: s}, nil
	}

	out := newWriterAt(len(data))
	start := time.Now()
	if err := blobs.DownloadBlobParallel(ctx, open, hash, out, blobs.ParallelDownloadOptions{
		Size:        uint64(len(data)),
		RangeSize:   2 << 20,
		Parallelism: 4,
		Retries:     1,
	}); err != nil {
		t.Fatalf("download parallel from one provider connection: %v", err)
	}
	got := out.Bytes()
	if !bytes.Equal(got, data) {
		t.Fatalf("parallel download wrote %d bytes, want %d", len(got), len(data))
	}
	if blobs.NewHash(got) != hash {
		t.Fatal("parallel download hash mismatch")
	}
	mbps := float64(len(data)) / 1024 / 1024 / time.Since(start).Seconds()
	t.Logf("single-provider parallel fetched %d bytes at %.1f MiB/s", len(data), mbps)
}

func TestGetManyBlobTransferIroh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	data := [][]byte{
		testData(1024),
		testData(blobs.BlockSize + 1),
	}
	var hashes []blobs.Hash
	store := make(map[blobs.Hash][]byte)
	for _, b := range data {
		hash := blobs.NewHash(b)
		hashes = append(hashes, hash)
		store[hash] = append([]byte(nil), b...)
	}

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatalf("bind server: %v", err)
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		blobs.ALPN: iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
			s, err := conn.AcceptStream(ctx)
			if err != nil {
				return err
			}
			return blobs.ServeBlob(ctx, s, mustBlobStore(t, blobValues(store)...))
		}),
	}, nil)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	defer router.Shutdown(ctx)

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatalf("bind client: %v", err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, blobs.ALPN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.CloseWithError(0, "")

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	got, err := blobs.GetManyBlobBytes(ctx, s, hashes)
	if err != nil {
		t.Fatalf("get many blobs: %v", err)
	}
	if len(got) != len(data) {
		t.Fatalf("got %d blobs, want %d", len(got), len(data))
	}
	for i := range data {
		if string(got[i]) != string(data[i]) {
			t.Fatalf("blob %d length = %d, want %d", i, len(got[i]), len(data[i]))
		}
	}
}

func TestGetHashSequenceBytesIroh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	data := [][]byte{
		testData(1024),
		testData(blobs.BlockSize + 1),
	}
	var hashes []blobs.Hash
	store := make(map[blobs.Hash][]byte)
	for _, b := range data {
		hash := blobs.NewHash(b)
		hashes = append(hashes, hash)
		store[hash] = append([]byte(nil), b...)
	}
	seq := blobs.NewHashSequence(hashes)
	root := blobs.NewHash(seq.Bytes())
	store[root] = seq.Bytes()

	server, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatalf("bind server: %v", err)
	}
	router, err := iroh.NewRouter(server, map[string]iroh.ProtocolHandler{
		blobs.ALPN: iroh.ProtocolHandlerFunc(func(ctx context.Context, conn *iroh.Conn) error {
			s, err := conn.AcceptStream(ctx)
			if err != nil {
				return err
			}
			return blobs.ServeBlob(ctx, s, mustBlobStore(t, blobValues(store)...))
		}),
	}, nil)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	defer router.Shutdown(ctx)

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatalf("bind client: %v", err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, blobs.ALPN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.CloseWithError(0, "")

	s, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	gotSeq, got, err := blobs.GetHashSequenceBytes(ctx, s, root)
	if err != nil {
		t.Fatalf("get hash sequence: %v", err)
	}
	if gotSeq.Len() != len(hashes) {
		t.Fatalf("hash seq len = %d, want %d", gotSeq.Len(), len(hashes))
	}
	if len(got) != len(data) {
		t.Fatalf("got %d blobs, want %d", len(got), len(data))
	}
	for i := range data {
		if string(got[i]) != string(data[i]) {
			t.Fatalf("blob %d length = %d, want %d", i, len(got[i]), len(data[i]))
		}
	}
}

func testData(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i*31 + 7)
	}
	return out
}

type irohRangeStream struct {
	*iroh.Stream
	conn *iroh.Conn
}

func (s *irohRangeStream) CloseWrite() error {
	return s.Stream.Close()
}

func (s *irohRangeStream) Close() error {
	err := s.Stream.Close()
	if closeErr := s.conn.CloseWithError(0, ""); err == nil {
		err = closeErr
	}
	return err
}

type sharedIrohRangeStream struct {
	*iroh.Stream
}

func (s sharedIrohRangeStream) CloseWrite() error {
	return s.Stream.Close()
}

type writerAt struct {
	mu sync.Mutex
	b  []byte
}

func newWriterAt(n int) *writerAt {
	return &writerAt{b: make([]byte, n)}
}

func (w *writerAt) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if off < 0 || int(off)+len(p) > len(w.b) {
		return 0, io.ErrShortWrite
	}
	return copy(w.b[off:], p), nil
}

func (w *writerAt) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.b...)
}

// mustBlobStore returns an in-memory blob store holding data.
func mustBlobStore(t *testing.T, data ...[]byte) *blobs.MemStore {
	t.Helper()
	m, err := blobs.NewMemStore(data...)
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	return m
}

func blobValues(m map[blobs.Hash][]byte) [][]byte {
	out := make([][]byte, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}
