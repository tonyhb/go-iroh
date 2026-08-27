package iroh

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/tmc/go-iroh/relayserver"
)

func benchmarkRelayConnPair(b *testing.B, alpn string) (client, server *Conn) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	b.Cleanup(cancel)

	srv := newEchoRelayServer(b, relayserver.WithClientRate(0))
	relayURL := srv.url(b)
	mode := relay.ModeCustom(relay.MapFromURLs(relayURL))

	srvKey, _ := key.GenerateSecretKey()
	serverEP, err := Bind(ctx,
		WithSecretKey(srvKey),
		WithALPNs(alpn),
		WithRelayMode(mode),
		WithoutIPTransports(),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { serverEP.Shutdown(context.Background()) })

	clientEP, err := Bind(ctx, WithRelayMode(mode), WithoutIPTransports())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { clientEP.Shutdown(context.Background()) })

	if err := serverEP.Online(ctx); err != nil {
		b.Fatalf("server online: %v", err)
	}
	if err := clientEP.Online(ctx); err != nil {
		b.Fatalf("client online: %v", err)
	}

	type accepted struct {
		conn *Conn
		err  error
	}
	done := make(chan accepted, 1)
	go func() {
		c, err := serverEP.Accept(ctx)
		done <- accepted{conn: c, err: err}
	}()

	addr := netaddr.NewEndpointAddr(serverEP.ID()).WithRelayURL(relayURL)
	client, err = clientEP.Connect(ctx, addr, alpn)
	if err != nil {
		b.Fatalf("relay connect: %v", err)
	}
	res := <-done
	if res.err != nil {
		b.Fatalf("accept: %v", res.err)
	}
	b.Cleanup(func() {
		client.CloseWithError(0, "")
		res.conn.CloseWithError(0, "")
	})
	return client, res.conn
}

func BenchmarkRelayConnSetupLatency(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping relay endpoint benchmark in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	srv := newEchoRelayServer(b, relayserver.WithClientRate(0))
	relayURL := srv.url(b)
	mode := relay.ModeCustom(relay.MapFromURLs(relayURL))

	const alpn = "iroh-bench-relay-setup/0"
	srvKey, _ := key.GenerateSecretKey()
	serverEP, err := Bind(ctx,
		WithSecretKey(srvKey),
		WithALPNs(alpn),
		WithRelayMode(mode),
		WithoutIPTransports(),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer serverEP.Shutdown(ctx)
	if err := serverEP.Online(ctx); err != nil {
		b.Fatalf("server online: %v", err)
	}

	addr := netaddr.NewEndpointAddr(serverEP.ID()).WithRelayURL(relayURL)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clientEP, err := Bind(ctx, WithRelayMode(mode), WithoutIPTransports())
		if err != nil {
			b.Fatal(err)
		}
		if err := clientEP.Online(ctx); err != nil {
			clientEP.Shutdown(ctx)
			b.Fatalf("client online: %v", err)
		}
		accepted := make(chan *Conn, 1)
		acceptErr := make(chan error, 1)
		go func() {
			conn, err := serverEP.Accept(ctx)
			if err != nil {
				acceptErr <- err
				return
			}
			accepted <- conn
		}()
		conn, err := clientEP.Connect(ctx, addr, alpn)
		if err != nil {
			clientEP.Shutdown(ctx)
			b.Fatalf("relay connect: %v", err)
		}
		select {
		case serverConn := <-accepted:
			serverConn.CloseWithError(0, "")
		case err := <-acceptErr:
			conn.CloseWithError(0, "")
			clientEP.Shutdown(ctx)
			b.Fatalf("accept: %v", err)
		}
		conn.CloseWithError(0, "")
		clientEP.Shutdown(ctx)
	}
}

func BenchmarkRelayConnStreamThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping relay endpoint benchmark in short mode")
	}
	client, server := benchmarkRelayConnPair(b, "iroh-bench-relay-throughput/0")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := client.OpenStreamSync(ctx)
	if err != nil {
		b.Fatalf("open stream: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		peer, err := server.AcceptStream(ctx)
		if err != nil {
			done <- err
			return
		}
		defer peer.Close()
		_, err = io.Copy(io.Discard, peer)
		done <- err
	}()

	buf := make([]byte, 64*1024)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	cpuStart := readBenchmarkCPUTime(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Write(buf); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
	b.StopTimer()
	reportCPUPerOp(b, cpuStart)
	if err := s.Close(); err != nil {
		b.Fatalf("close stream: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			b.Fatalf("copy: %v", err)
		}
	case <-time.After(10 * time.Second):
		s.CancelRead(0)
		s.CancelWrite(0)
		b.Fatalf("copy did not finish after stream close")
	}
}
