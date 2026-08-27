package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/internal/relayclient"
	"github.com/tmc/go-iroh/internal/relayproto"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// TestServeRelaysAndHealth stands up the command's server on a real listener,
// checks the health endpoint, and round-trips a datagram between two relay
// clients through it — the self-host proof for deliverable 2.
func TestServeRelaysAndHealth(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	logger := log.New(io.Discard, "", 0)

	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, logger, time.Second) }()

	base := "http://" + ln.Addr().String()

	// Health endpoint is up.
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "ok\n" {
		t.Fatalf("/healthz = %d %q, want 200 \"ok\\n\"", resp.StatusCode, body)
	}

	// Net-report probe endpoint answers 200 (Rust clients require it before
	// selecting a home relay).
	resp, err = http.Get(base + "/ping")
	if err != nil {
		t.Fatalf("GET /ping: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ping = %d, want 200", resp.StatusCode)
	}

	// Two endpoints connect through the relay and a datagram round-trips.
	u, err := netaddr.ParseRelayURL(base)
	if err != nil {
		t.Fatal(err)
	}
	// Connecting and the round trip get separate budgets, so a slow dial
	// cannot spend the deadline the forwarding assertion is waiting on and
	// report itself as a forwarding failure.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer dialCancel()

	sk1, _ := key.GenerateSecretKey()
	c1, err := relayclient.Connect(dialCtx, u, relayclient.Options{SecretKey: sk1})
	if err != nil {
		t.Fatalf("connect c1: %v", err)
	}
	defer c1.Close()

	sk2, _ := key.GenerateSecretKey()
	c2, err := relayclient.Connect(dialCtx, u, relayclient.Options{SecretKey: sk2})
	if err != nil {
		t.Fatalf("connect c2: %v", err)
	}
	defer c2.Close()

	cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer ccancel()

	payload := []byte("self-hosted relay works")
	if err := c1.Send(cctx, relayproto.ClientToRelayMsg{
		Type:          relayproto.FrameClientToRelayDatagram,
		DstEndpointID: sk2.Public().EndpointID(),
		Datagrams:     relayproto.DatagramsFromBytes(payload),
	}); err != nil {
		t.Fatalf("send datagram: %v", err)
	}
	got, err := c2.Recv(cctx)
	if err != nil {
		t.Fatalf("recv forwarded datagram: %v", err)
	}
	if string(got.Datagrams.Contents) != string(payload) {
		t.Fatalf("datagram = %q, want %q", got.Datagrams.Contents, payload)
	}

	// Graceful shutdown returns without error.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v, want nil on graceful shutdown", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("serve did not return after shutdown")
	}
}

// TestRunRejectsExtraArgs is a small guard on argument handling.
func TestRunRejectsExtraArgs(t *testing.T) {
	var buf bytes.Buffer
	if err := run([]string{"unexpected"}, &buf); err == nil {
		t.Fatal("run with extra arg = nil, want error")
	}
}

func TestRunRejectsPprofAddressInUse(t *testing.T) {
	pprofListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pprofListener.Close()

	var logOut bytes.Buffer
	err = run([]string{
		"-addr=127.0.0.1:0",
		"-pprof-addr=" + pprofListener.Addr().String(),
	}, &logOut)
	if err == nil {
		t.Fatal("run succeeded with pprof address in use")
	}
	if !strings.Contains(err.Error(), "pprof listener") {
		t.Fatalf("run error = %q, want pprof listener context", err)
	}
}

func TestHTTPServerTimeouts(t *testing.T) {
	srv := newHTTPServer(http.NotFoundHandler())
	if srv.ReadHeaderTimeout != httpReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %v, want %v", srv.ReadHeaderTimeout, httpReadHeaderTimeout)
	}
	if srv.ReadTimeout != httpReadTimeout {
		t.Fatalf("ReadTimeout = %v, want %v", srv.ReadTimeout, httpReadTimeout)
	}
	if srv.WriteTimeout != httpWriteTimeout {
		t.Fatalf("WriteTimeout = %v, want %v", srv.WriteTimeout, httpWriteTimeout)
	}
	if srv.IdleTimeout != httpIdleTimeout {
		t.Fatalf("IdleTimeout = %v, want %v", srv.IdleTimeout, httpIdleTimeout)
	}
}
