package iroh

import (
	"context"
	"io"
	"net"
	"net/netip"
	"runtime"
	"testing"
	"time"

	quic "github.com/tmc/go-iroh/internal/qng"
	"golang.org/x/net/ipv6"
)

const layerLadderTransferSize = 64 << 20

type layerLadderZeroReader struct{}

func (layerLadderZeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func BenchmarkMemoryCopyThroughput(b *testing.B) {
	src := make([]byte, 64*1024)
	for i := range src {
		src[i] = 0xab
	}
	dst := make([]byte, len(src))
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		copy(dst, src)
	}
	b.StopTimer()
	emitLayerLadderSample(b, "memcpy", int64(b.N)*int64(len(src)))
	runtime.KeepAlive(dst)
}

func BenchmarkRawUDPBatchSendThroughput(b *testing.B) {
	client, server := benchmarkUDPConnPair(b)
	const batch = 16
	messages := make([]ipv6.Message, batch)
	for i := range messages {
		messages[i].Buffers = [][]byte{make([]byte, 1200)}
	}

	done := make(chan error, 1)
	go func() {
		var buf [1200]byte
		for {
			if _, _, err := server.ReadFromUDPAddrPort(buf[:]); err != nil {
				done <- err
				return
			}
		}
	}()

	packetConn := ipv6.NewPacketConn(client)
	b.SetBytes(batch * 1200)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for sent := 0; sent < len(messages); {
			n, err := packetConn.WriteBatch(messages[sent:], 0)
			if err != nil {
				b.Fatalf("write UDP batch: %v", err)
			}
			if n == 0 {
				b.Fatal("UDP batch write made no progress")
			}
			sent += n
		}
	}
	b.StopTimer()
	client.Close()
	server.Close()
	<-done
}

func BenchmarkConnStreamThroughputIPv4(b *testing.B) {
	client, server := benchmarkConnPairAddr(b, "iroh-bench-throughput-ipv4/0", netip.MustParseAddr("127.0.0.1"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.OpenStreamSync(ctx)
	if err != nil {
		b.Fatalf("open stream: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		peer, err := server.AcceptStream(ctx)
		if err == nil {
			_, err = io.Copy(io.Discard, peer)
		}
		done <- err
	}()
	buf := make([]byte, 64*1024)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := stream.Write(buf); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
	b.StopTimer()
	emitLayerLadderSample(b, "full-steady", int64(b.N)*int64(len(buf)))
	if err := stream.Close(); err != nil {
		b.Fatalf("close stream: %v", err)
	}
	if err := <-done; err != nil {
		b.Fatalf("drain stream: %v", err)
	}
}

// BenchmarkConnDownload64MiB matches the download clock in iroh v1.0.3's
// iroh/bench/src/iroh.rs:251-253: stream setup and request completion precede
// the clock, which covers the first receive wait through payload EOF.
func BenchmarkConnDownload64MiB(b *testing.B) {
	client, server := benchmarkConnPairAddr(b, "iroh-bench-download-64m/0", netip.MustParseAddr("127.0.0.1"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		for {
			peer, err := server.AcceptStream(ctx)
			if err != nil {
				done <- err
				return
			}
			if _, err := io.Copy(io.Discard, peer); err != nil {
				done <- err
				return
			}
			if _, err := io.CopyN(peer, layerLadderZeroReader{}, layerLadderTransferSize); err != nil {
				done <- err
				return
			}
			if err := peer.Close(); err != nil {
				done <- err
				return
			}
		}
	}()

	b.SetBytes(layerLadderTransferSize)
	b.ReportAllocs()
	b.StopTimer()
	for range b.N {
		stream, err := client.OpenStreamSync(ctx)
		if err != nil {
			b.Fatalf("open stream: %v", err)
		}
		if err := stream.Close(); err != nil {
			b.Fatalf("finish request: %v", err)
		}
		b.StartTimer()
		if n, err := io.CopyN(io.Discard, stream, layerLadderTransferSize); err != nil {
			b.Fatalf("read payload after %d bytes: %v", n, err)
		}
		var extra [1]byte
		if n, err := stream.Read(extra[:]); n != 0 || err != io.EOF {
			b.Fatalf("read payload end: n=%d err=%v", n, err)
		}
		b.StopTimer()
	}
	emitLayerLadderSample(b, "full-64m", int64(b.N)*layerLadderTransferSize)
	client.CloseWithError(0, "done")
	select {
	case <-done:
	case <-time.After(time.Second):
	}
}

// BenchmarkQUICDownload64MiB applies the same clock to qng without iroh's
// endpoint and magic-socket layers.
func BenchmarkQUICDownload64MiB(b *testing.B) {
	client, server := benchmarkQUICConnPairWithConfigAddr(b, "iroh-bench-raw-download-64m/0", &quic.Config{InitialPacketSize: 1200}, net.IPv4(127, 0, 0, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		for {
			peer, err := server.AcceptStream(ctx)
			if err != nil {
				done <- err
				return
			}
			if _, err := io.Copy(io.Discard, peer); err != nil {
				done <- err
				return
			}
			if _, err := io.CopyN(peer, layerLadderZeroReader{}, layerLadderTransferSize); err != nil {
				done <- err
				return
			}
			if err := peer.Close(); err != nil {
				done <- err
				return
			}
		}
	}()

	b.SetBytes(layerLadderTransferSize)
	b.ReportAllocs()
	b.StopTimer()
	for range b.N {
		stream, err := client.OpenStreamSync(ctx)
		if err != nil {
			b.Fatalf("open stream: %v", err)
		}
		if err := stream.Close(); err != nil {
			b.Fatalf("finish request: %v", err)
		}
		b.StartTimer()
		if n, err := io.CopyN(io.Discard, stream, layerLadderTransferSize); err != nil {
			b.Fatalf("read payload after %d bytes: %v", n, err)
		}
		var extra [1]byte
		if n, err := stream.Read(extra[:]); n != 0 || err != io.EOF {
			b.Fatalf("read payload end: n=%d err=%v", n, err)
		}
		b.StopTimer()
	}
	emitLayerLadderSample(b, "quic-64m", int64(b.N)*layerLadderTransferSize)
	client.CloseWithError(0, "done")
	select {
	case <-done:
	case <-time.After(time.Second):
	}
}
