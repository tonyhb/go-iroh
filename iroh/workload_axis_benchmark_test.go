package iroh

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	quic "github.com/tmc/go-iroh/internal/qng"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"golang.org/x/sys/unix"
)

var workloadMessageSizes = [...]int{32, 256, 1024, 8192}

const defaultWorkloadScalingBytes = 64 << 20

func TestWorkloadSocketConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	serverKey, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	server, err := Bind(ctx, WithSecretKey(serverKey), WithALPNs("iroh-workload-config/0"),
		WithTransportConfig(&QUICTransportConfig{InitialPacketSize: 1200, MaxIncomingStreams: 64}),
		WithBindAddr(netip.AddrPortFrom(workloadIPv4(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	client, err := Bind(ctx,
		WithTransportConfig(&QUICTransportConfig{InitialPacketSize: 1200, MaxIncomingStreams: 64}),
		WithBindAddr(netip.AddrPortFrom(workloadIPv4(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())
	accepted := make(chan acceptedConn, 1)
	go func() {
		conn, err := server.Accept(ctx)
		accepted <- acceptedConn{conn: conn, err: err}
	}()
	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, "iroh-workload-config/0")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(0, "")
	serverConn := <-accepted
	if serverConn.err != nil {
		t.Fatal(serverConn.err)
	}
	defer serverConn.conn.CloseWithError(0, "")
	receive, send := workloadEndpointBuffers(t, client)
	serverReceive, serverSend := workloadEndpointBuffers(t, server)
	t.Logf("initial_packet_size=1200 max_incoming_streams=64 requested_receive_buffer=7340032 requested_send_buffer=7340032 client_receive_buffer=%d client_send_buffer=%d server_receive_buffer=%d server_send_buffer=%d loopback=127.0.0.1 congestion_control=cubic", receive, send, serverReceive, serverSend)
}

func workloadEndpointBuffers(t *testing.T, endpoint *Endpoint) (receive, send int) {
	t.Helper()
	raw, err := endpoint.magic.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	receive, err = workloadSocketBuffer(raw, unix.SO_RCVBUF)
	if err != nil {
		t.Fatal(err)
	}
	send, err = workloadSocketBuffer(raw, unix.SO_SNDBUF)
	if err != nil {
		t.Fatal(err)
	}
	return receive, send
}

func workloadSocketBuffer(raw syscall.RawConn, option int) (int, error) {
	var value int
	var socketErr error
	err := raw.Control(func(fd uintptr) {
		value, socketErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, option)
	})
	if err != nil {
		return 0, err
	}
	return value, socketErr
}

func BenchmarkConnStreamScaling(b *testing.B) {
	for _, streams := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("streams=%d", streams), func(b *testing.B) {
			client, server := benchmarkConnPairAddr(b, "iroh-workload-stream-scaling/0", workloadIPv4())
			benchmarkConcurrentDownloads(b, []*Conn{client}, []*Conn{server}, streams)
		})
	}
}

func BenchmarkConnConnectionScaling(b *testing.B) {
	for _, connections := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("connections=%d", connections), func(b *testing.B) {
			clients, servers := benchmarkConnFanout(b, connections)
			benchmarkConcurrentDownloads(b, clients, servers, 1)
		})
	}
}

func benchmarkConnFanout(b *testing.B, connections int) (clients, servers []*Conn) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	b.Cleanup(cancel)
	const alpn = "iroh-workload-connection-scaling/0"
	transportConfig := WithTransportConfig(&QUICTransportConfig{InitialPacketSize: 1200, MaxIncomingStreams: 64})
	serverKey, err := key.GenerateSecretKey()
	if err != nil {
		b.Fatal(err)
	}
	serverEndpoint, err := Bind(ctx, WithSecretKey(serverKey), WithALPNs(alpn), transportConfig,
		WithBindAddr(netip.AddrPortFrom(workloadIPv4(), 0)))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { serverEndpoint.Shutdown(context.Background()) })
	accepted := make(chan acceptedConn, connections)
	go func() {
		for range connections {
			conn, err := serverEndpoint.Accept(ctx)
			accepted <- acceptedConn{conn, err}
		}
	}()
	addr := netaddr.NewEndpointAddr(serverEndpoint.ID()).WithIP(serverEndpoint.LocalAddr())
	for range connections {
		clientEndpoint, err := Bind(ctx, transportConfig, WithBindAddr(netip.AddrPortFrom(workloadIPv4(), 0)))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { clientEndpoint.Shutdown(context.Background()) })
		client, err := clientEndpoint.Connect(ctx, addr, alpn)
		if err != nil {
			b.Fatal(err)
		}
		clients = append(clients, client)
	}
	for range connections {
		result := <-accepted
		if result.err != nil {
			b.Fatal(result.err)
		}
		servers = append(servers, result.conn)
	}
	return clients, servers
}

type acceptedConn struct {
	conn *Conn
	err  error
}

// requireBenchtime1x calls b.Fatalf unless the benchmark is running with
// -benchtime=1x. It must not call b.Helper: helper frames are skipped in
// failure reports, which would attribute the failure to the caller.
func requireBenchtime1x(b *testing.B) {
	if b.N != 1 {
		b.Fatalf("scaling benchmark requires -benchtime=1x; b.N=%d", b.N)
	}
}

// benchmarkConcurrentDownloads runs clients*streamsPerConn downloads at once
// and reports them as a single operation.
//
// The total byte count is fixed and divided evenly among the downloads, so
// raising the stream or connection count splits the same work instead of
// adding more. The two axes are therefore comparable with each other and
// with the single-download case.
func benchmarkConcurrentDownloads(b *testing.B, clients, servers []*Conn, streamsPerConn int) {
	b.Helper()
	requireBenchtime1x(b)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	flows := len(clients) * streamsPerConn
	totalBytes := workloadScalingBytes(b)
	bytesPerFlow := totalBytes / int64(flows)
	serverDone := make(chan error, flows)
	for _, server := range servers {
		go benchmarkConnDownloadServer(ctx, server, streamsPerConn, bytesPerFlow, serverDone)
	}
	flowDurationNS := make([]int64, flows)
	flowBytes := make([]int64, flows)
	flowErr := make([]error, flows)
	var wg sync.WaitGroup
	wg.Add(flows)
	b.SetBytes(totalBytes)
	b.ReportAllocs()
	cpuStart := readBenchmarkCPUTime(b)
	b.ResetTimer()
	flow := 0
	for _, client := range clients {
		for range streamsPerConn {
			i := flow
			flow++
			go func(client *Conn) {
				defer wg.Done()
				start := time.Now()
				stream, err := client.OpenStreamSync(ctx)
				if err == nil {
					_, err = stream.Write([]byte{1})
				}
				var n int64
				if err == nil {
					n, err = io.CopyN(io.Discard, stream, bytesPerFlow)
				}
				flowErr[i] = err
				flowBytes[i] = n
				flowDurationNS[i] = time.Since(start).Nanoseconds()
			}(client)
		}
	}
	wg.Wait()
	for i, err := range flowErr {
		if err != nil {
			b.Errorf("flow %d: %v", i, err)
		}
	}
	for range flows {
		if err := <-serverDone; err != nil {
			b.Errorf("server flow: %v", err)
		}
	}
	b.StopTimer()
	rung := fmt.Sprintf("full-scale-streams-%d-connections-%d", streamsPerConn, len(clients))
	emitLayerLadderSampleRecord(b, layerLadderSample{
		Rung:           rung,
		Bytes:          totalBytes,
		FlowBytes:      flowBytes,
		FlowDurationNS: flowDurationNS,
	}, cpuStart)
}

func workloadScalingBytes(b *testing.B) int64 {
	b.Helper()
	value := os.Getenv("IROH_WORKLOAD_SCALING_BYTES")
	if value == "" {
		return defaultWorkloadScalingBytes
	}
	bytes, err := strconv.ParseInt(value, 10, 64)
	if err != nil || bytes <= 0 {
		b.Fatalf("invalid IROH_WORKLOAD_SCALING_BYTES %q", value)
	}
	return bytes
}

// benchmarkConnDownloadServer serves one connection's streams for
// benchmarkConcurrentDownloads.
func benchmarkConnDownloadServer(ctx context.Context, server *Conn, streams int, bytes int64, done chan<- error) {
	for i := range streams {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			for range streams - i {
				done <- err
			}
			return
		}
		go func() {
			var request [1]byte
			_, err := io.ReadFull(stream, request[:])
			if err == nil {
				_, err = io.CopyN(stream, layerLadderZeroReader{}, bytes)
			}
			if err == nil {
				err = stream.Close()
			}
			done <- err
		}()
	}
}

func BenchmarkRawTCPMessageRate(b *testing.B) {
	for _, size := range workloadMessageSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			client, server := benchmarkTCPConnPair(b)
			done := make(chan error, 1)
			go func() {
				if _, err := io.CopyN(io.Discard, server, int64(b.N*size)); err != nil {
					done <- err
					return
				}
				_, err := server.Write([]byte{1})
				done <- err
			}()
			buf := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			cpuStart := readBenchmarkCPUTime(b)
			b.ResetTimer()
			for range b.N {
				if _, err := client.Write(buf); err != nil {
					b.Fatal(err)
				}
			}
			var ack [1]byte
			if _, err := io.ReadFull(client, ack[:]); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if err := <-done; err != nil {
				b.Fatal(err)
			}
			emitWorkloadSample(b, fmt.Sprintf("tcp-stream-msg-%d", size), size, cpuStart)
		})
	}
}

func BenchmarkQUICMessageRate(b *testing.B) {
	for _, size := range workloadMessageSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			client, server := benchmarkQUICConnPairWithConfig(b, "iroh-workload-qng-message/0", &quic.Config{InitialPacketSize: 1200})
			benchmarkQUICMessages(b, client, server, size, fmt.Sprintf("quic-stream-msg-%d", size))
		})
	}
}

func BenchmarkConnMessageRate(b *testing.B) {
	for _, size := range workloadMessageSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			client, server := benchmarkConnPairAddr(b, "iroh-workload-conn-message/0", workloadIPv4())
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			stream, err := client.OpenStreamSync(ctx)
			if err != nil {
				b.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				peer, err := server.AcceptStream(ctx)
				if err == nil {
					_, err = io.CopyN(io.Discard, peer, int64(b.N*size))
				}
				if err == nil {
					_, err = peer.Write([]byte{1})
				}
				done <- err
			}()
			buf := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			clientStart := snapshotConnStats(client)
			cpuStart := readBenchmarkCPUTime(b)
			b.ResetTimer()
			for range b.N {
				if _, err := stream.Write(buf); err != nil {
					b.Fatal(err)
				}
			}
			var ack [1]byte
			if _, err := io.ReadFull(stream, ack[:]); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			if err := <-done; err != nil {
				b.Fatal(err)
			}
			transport := reportConnSenderStats(b, client, clientStart)
			emitLayerLadderSampleRecord(b, layerLadderSample{
				Rung:      fmt.Sprintf("full-stream-msg-%d", size),
				Bytes:     int64(b.N * size),
				Messages:  int64(b.N),
				Transport: transport,
			}, cpuStart)
		})
	}
}

func BenchmarkConnStreamBurstPingPong(b *testing.B) {
	const (
		writesPerBurst = 64
		writeSize      = 32
	)
	client, server := benchmarkConnPairAddr(b, "iroh-workload-burst-ping/0", workloadIPv4())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.OpenStreamSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		peer, err := server.AcceptStream(ctx)
		if err != nil {
			done <- err
			return
		}
		buf := make([]byte, writesPerBurst*writeSize)
		for {
			if _, err := io.ReadFull(peer, buf); err != nil {
				done <- err
				return
			}
			if _, err := peer.Write([]byte{1}); err != nil {
				done <- err
				return
			}
		}
	}()

	buf := make([]byte, writeSize)
	b.SetBytes(writesPerBurst * writeSize)
	b.ReportAllocs()
	clientStart := snapshotConnStats(client)
	cpuStart := readBenchmarkCPUTime(b)
	captureLatency := os.Getenv("IROH_CAPTURE_OP_LATENCY") == "1"
	var opDurationNS []int64
	if captureLatency {
		opDurationNS = make([]int64, 0, b.N)
	}
	b.ResetTimer()
	for range b.N {
		var start time.Time
		if captureLatency {
			start = time.Now()
		}
		for range writesPerBurst {
			if _, err := stream.Write(buf); err != nil {
				b.Fatal(err)
			}
		}
		var ack [1]byte
		if _, err := io.ReadFull(stream, ack[:]); err != nil {
			b.Fatal(err)
		}
		if captureLatency {
			opDurationNS = append(opDurationNS, time.Since(start).Nanoseconds())
		}
	}
	b.StopTimer()
	transport := reportConnSenderStats(b, client, clientStart)
	emitLayerLadderSampleRecord(b, layerLadderSample{
		Rung:         "full-burst-ping",
		Bytes:        int64(b.N * writesPerBurst * writeSize),
		Messages:     int64(b.N * writesPerBurst),
		OpDurationNS: opDurationNS,
		Transport:    transport,
	}, cpuStart)
	stream.CancelRead(0)
	stream.CancelWrite(0)
	<-done
}

func BenchmarkConnStreamBurstThenSinglePingPong(b *testing.B) {
	const (
		writesPerBurst = 64
		writeSize      = 32
	)
	client, server := benchmarkConnPairAddr(b, "iroh-workload-burst-single-ping/0", workloadIPv4())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.OpenStreamSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		peer, err := server.AcceptStream(ctx)
		if err != nil {
			done <- err
			return
		}
		burst := make([]byte, writesPerBurst*writeSize)
		single := make([]byte, writeSize)
		for {
			if _, err := io.ReadFull(peer, burst); err != nil {
				done <- err
				return
			}
			if _, err := peer.Write([]byte{1}); err != nil {
				done <- err
				return
			}
			if _, err := io.ReadFull(peer, single); err != nil {
				done <- err
				return
			}
			if _, err := peer.Write([]byte{1}); err != nil {
				done <- err
				return
			}
		}
	}()

	buf := make([]byte, writeSize)
	b.SetBytes(writeSize)
	b.ReportAllocs()
	clientStart := snapshotConnStats(client)
	cpuStart := readBenchmarkCPUTime(b)
	captureLatency := os.Getenv("IROH_CAPTURE_OP_LATENCY") == "1"
	var opDurationNS []int64
	if captureLatency {
		opDurationNS = make([]int64, 0, b.N)
	}
	b.ResetTimer()
	b.StopTimer()
	for range b.N {
		for range writesPerBurst {
			if _, err := stream.Write(buf); err != nil {
				b.Fatal(err)
			}
		}
		var ack [1]byte
		if _, err := io.ReadFull(stream, ack[:]); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		var start time.Time
		if captureLatency {
			start = time.Now()
		}
		if _, err := stream.Write(buf); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(stream, ack[:]); err != nil {
			b.Fatal(err)
		}
		if captureLatency {
			opDurationNS = append(opDurationNS, time.Since(start).Nanoseconds())
		}
		b.StopTimer()
	}
	transport := reportConnSenderStats(b, client, clientStart)
	emitLayerLadderSampleRecord(b, layerLadderSample{
		Rung:         "full-burst-single-ping",
		Bytes:        int64(b.N * writeSize),
		Messages:     int64(b.N * (writesPerBurst + 1)),
		OpDurationNS: opDurationNS,
		Transport:    transport,
	}, cpuStart)
	stream.CancelRead(0)
	stream.CancelWrite(0)
	<-done
}

func benchmarkQUICMessages(b *testing.B, client, server *quic.Conn, size int, rung string) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.OpenStreamSync(ctx)
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		peer, err := server.AcceptStream(ctx)
		if err == nil {
			_, err = io.CopyN(io.Discard, peer, int64(b.N*size))
		}
		if err == nil {
			_, err = peer.Write([]byte{1})
		}
		done <- err
	}()
	buf := make([]byte, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	cpuStart := readBenchmarkCPUTime(b)
	b.ResetTimer()
	for range b.N {
		if _, err := stream.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
	var ack [1]byte
	if _, err := io.ReadFull(stream, ack[:]); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	emitWorkloadSample(b, rung, size, cpuStart)
}

func BenchmarkRawUDPMessageRate(b *testing.B) {
	for _, size := range workloadMessageSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			client, server := benchmarkUDPConnPair(b)
			benchmarkUDPMessageEcho(b, client, server, size, fmt.Sprintf("udp-datagram-msg-%d", size))
		})
	}
}

func BenchmarkConnDatagramMessageRate(b *testing.B) {
	for _, size := range workloadMessageSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			client, server := benchmarkConnPairAddr(b, "iroh-workload-conn-datagram/0", workloadIPv4())
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				for sequence := 0; sequence < b.N; {
					p, err := server.ReadDatagram(ctx)
					if err != nil {
						done <- err
						return
					}
					if err := server.SendDatagram(p); err != nil {
						done <- err
						return
					}
					if len(p) >= 8 && binary.BigEndian.Uint64(p) == uint64(sequence) {
						sequence++
					}
				}
				done <- nil
			}()
			buf := make([]byte, size)
			echoes := make(chan []byte, 1)
			receiveErr := make(chan error, 1)
			go func() {
				for {
					got, err := client.ReadDatagram(ctx)
					if err != nil {
						select {
						case receiveErr <- err:
						case <-ctx.Done():
						}
						return
					}
					select {
					case echoes <- got:
					case <-ctx.Done():
						return
					}
				}
			}()
			retry := time.NewTimer(time.Hour)
			if !retry.Stop() {
				<-retry.C
			}
			defer retry.Stop()
			b.SetBytes(int64(size))
			b.ReportAllocs()
			cpuStart := readBenchmarkCPUTime(b)
			b.ResetTimer()
			for sequence := range b.N {
				binary.BigEndian.PutUint64(buf, uint64(sequence))
				if err := client.SendDatagram(buf); err != nil {
					b.Fatal(err)
				}
				retry.Reset(100 * time.Millisecond)
			awaitEcho:
				for {
					select {
					case got := <-echoes:
						if len(got) >= 8 && binary.BigEndian.Uint64(got) == uint64(sequence) {
							if !retry.Stop() {
								<-retry.C
							}
							break awaitEcho
						}
					case err := <-receiveErr:
						b.Fatal(err)
					case <-retry.C:
						if err := client.SendDatagram(buf); err != nil {
							b.Fatal(err)
						}
						retry.Reset(100 * time.Millisecond)
					}
				}
			}
			b.StopTimer()
			if err := <-done; err != nil {
				b.Fatal(err)
			}
			emitWorkloadSample(b, fmt.Sprintf("full-datagram-msg-%d", size), size, cpuStart)
		})
	}
}

func benchmarkUDPMessageEcho(b *testing.B, client, server *net.UDPConn, size int, rung string) {
	b.Helper()
	done := make(chan error, 1)
	go func() {
		buf := make([]byte, size)
		for range b.N {
			n, addr, err := server.ReadFromUDP(buf)
			if err == nil {
				_, err = server.WriteToUDP(buf[:n], addr)
			}
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	buf := make([]byte, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	cpuStart := readBenchmarkCPUTime(b)
	b.ResetTimer()
	for range b.N {
		if _, err := client.Write(buf); err != nil {
			b.Fatal(err)
		}
		if _, err := client.Read(buf); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	emitWorkloadSample(b, rung, size, cpuStart)
}

func emitWorkloadSample(b *testing.B, rung string, size int, cpuStart benchmarkCPUTime) {
	b.Helper()
	emitLayerLadderSampleRecord(b, layerLadderSample{
		Rung:     rung,
		Bytes:    int64(b.N * size),
		Messages: int64(b.N),
	}, cpuStart)
}

func workloadIPv4() netip.Addr {
	return netip.MustParseAddr("127.0.0.1")
}

// BenchmarkConnMessageRateWritev is the Stage 35 primary cell: 32-byte
// messages sent batch-at-a-time through SendStream.Writev, against the
// per-message Write cell in BenchmarkConnMessageRate.
func BenchmarkConnMessageRateWritev(b *testing.B) {
	for _, size := range workloadMessageSizes {
		for _, batch := range []int{2, 8} {
			b.Run(fmt.Sprintf("size=%d/batch=%d", size, batch), func(b *testing.B) {
				client, server := benchmarkConnPairAddr(b, "iroh-workload-conn-message/0", workloadIPv4())
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				stream, err := client.OpenStreamSync(ctx)
				if err != nil {
					b.Fatal(err)
				}
				done := make(chan error, 1)
				go func() {
					peer, err := server.AcceptStream(ctx)
					if err == nil {
						_, err = io.CopyN(io.Discard, peer, int64(b.N*size))
					}
					if err == nil {
						_, err = peer.Write([]byte{1})
					}
					done <- err
				}()
				msgs := make([][]byte, batch)
				for i := range msgs {
					msgs[i] = make([]byte, size)
				}
				scratch := make([][]byte, batch)
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i += batch {
					n := batch
					if rem := b.N - i; rem < n {
						n = rem
					}
					copy(scratch, msgs)
					bufs := net.Buffers(scratch[:n])
					if _, err := stream.Writev(&bufs); err != nil {
						b.Fatal(err)
					}
				}
				var ack [1]byte
				if _, err := io.ReadFull(stream, ack[:]); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := <-done; err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}

// BenchmarkConnBurstThenSingleton is the Stage 35 batching-safety guard:
// each op sends a 64-message burst followed by one singleton write and
// waits for the receiver to acknowledge full delivery. A Writev arm that
// regresses op completion versus the Write-loop arm means the singleton
// is getting trapped behind the vector.
func BenchmarkConnBurstThenSingleton(b *testing.B) {
	const (
		size  = 32
		burst = 64
	)
	for _, mode := range []string{"writeloop", "writev"} {
		b.Run("mode="+mode, func(b *testing.B) {
			client, server := benchmarkConnPairAddr(b, "iroh-workload-conn-message/0", workloadIPv4())
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			stream, err := client.OpenStreamSync(ctx)
			if err != nil {
				b.Fatal(err)
			}
			const opBytes = (burst + 1) * size
			done := make(chan error, 1)
			go func() {
				peer, err := server.AcceptStream(ctx)
				if err != nil {
					done <- err
					return
				}
				buf := make([]byte, opBytes)
				for {
					if _, err := io.ReadFull(peer, buf); err != nil {
						if err == io.EOF || err == io.ErrUnexpectedEOF {
							err = nil
						}
						done <- err
						return
					}
					if _, err := peer.Write([]byte{1}); err != nil {
						done <- err
						return
					}
				}
			}()
			msgs := make([][]byte, burst)
			for i := range msgs {
				msgs[i] = make([]byte, size)
			}
			scratch := make([][]byte, burst)
			single := make([]byte, size)
			var ack [1]byte
			b.SetBytes(int64(opBytes))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if mode == "writev" {
					copy(scratch, msgs)
					bufs := net.Buffers(scratch)
					if _, err := stream.Writev(&bufs); err != nil {
						b.Fatal(err)
					}
				} else {
					for _, m := range msgs {
						if _, err := stream.Write(m); err != nil {
							b.Fatal(err)
						}
					}
				}
				if _, err := stream.Write(single); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(stream, ack[:]); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := stream.Close(); err != nil {
				b.Fatal(err)
			}
			if err := <-done; err != nil {
				b.Fatal(err)
			}
		})
	}
}
