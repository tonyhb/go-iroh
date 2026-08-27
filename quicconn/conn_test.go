package quicconn_test

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/quicconn"
)

func TestConnStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const alpn = "h3-iroh-test/1"
	server, err := iroh.Bind(ctx,
		iroh.WithALPNs(alpn),
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
	)
	if err != nil {
		t.Fatalf("bind server: %v", err)
	}
	defer server.Shutdown(ctx)

	accepted := make(chan *iroh.Conn, 1)
	errc := make(chan error, 1)
	go func() {
		c, err := server.Accept(ctx)
		if err != nil {
			errc <- err
			return
		}
		accepted <- c
	}()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatalf("bind client: %v", err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	clientConn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer clientConn.CloseWithError(0, "")

	var serverConn *iroh.Conn
	select {
	case serverConn = <-accepted:
	case err := <-errc:
		t.Fatalf("accept: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer serverConn.CloseWithError(0, "")

	clientQC := quicconn.NewConn(clientConn)
	serverQC := quicconn.NewConn(serverConn)
	if clientQC.IrohConn() != clientConn {
		t.Fatal("IrohConn did not return the underlying connection")
	}
	var nilConn *quicconn.Conn
	if nilConn.IrohConn() != nil {
		t.Fatal("nil Conn returned a non-nil underlying connection")
	}

	done := make(chan error, 1)
	go func() {
		s, err := serverQC.AcceptBidi(ctx)
		if err != nil {
			done <- err
			return
		}
		_, err = io.Copy(s, s)
		done <- err
	}()

	stream, err := clientQC.OpenBidi(ctx)
	if err != nil {
		t.Fatalf("open bidi: %v", err)
	}
	if stream.IrohStream() == nil {
		t.Fatal("IrohStream returned nil")
	}
	deadline := time.Now().Add(5 * time.Second)
	if err := stream.SetDeadline(deadline); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if err := stream.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if err := stream.SetWriteDeadline(deadline); err != nil {
		t.Fatalf("set write deadline: %v", err)
	}
	if stream.Context() == nil {
		t.Fatal("stream Context returned nil")
	}
	if _, err := stream.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len("hello"))
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("echo = %q, want hello", buf)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server stream: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	uniData := make(chan []byte, 1)
	uniErr := make(chan error, 1)
	go func() {
		s, err := serverQC.AcceptUni(ctx)
		if err != nil {
			uniErr <- err
			return
		}
		if s.IrohReceiveStream() == nil {
			uniErr <- errors.New("IrohReceiveStream returned nil")
			return
		}
		if err := s.SetReadDeadline(deadline); err != nil {
			uniErr <- err
			return
		}
		b, err := io.ReadAll(s)
		if err != nil {
			uniErr <- err
			return
		}
		uniData <- b
	}()

	send, err := clientQC.OpenUni(ctx)
	if err != nil {
		t.Fatalf("open uni: %v", err)
	}
	if send.IrohSendStream() == nil {
		t.Fatal("IrohSendStream returned nil")
	}
	if send.Context() == nil {
		t.Fatal("send stream Context returned nil")
	}
	if err := send.SetWriteDeadline(deadline); err != nil {
		t.Fatalf("set uni write deadline: %v", err)
	}
	if _, err := send.Write([]byte("one way")); err != nil {
		t.Fatalf("write uni: %v", err)
	}
	if err := send.Close(); err != nil {
		t.Fatalf("close uni: %v", err)
	}
	select {
	case b := <-uniData:
		if string(b) != "one way" {
			t.Fatalf("uni data = %q, want one way", b)
		}
	case err := <-uniErr:
		t.Fatalf("receive uni: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	if err := clientQC.SendDatagram([]byte("datagram")); err != nil {
		t.Fatalf("send datagram: %v", err)
	}
	datagram, err := serverQC.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatalf("receive datagram: %v", err)
	}
	if string(datagram) != "datagram" {
		t.Fatalf("datagram = %q, want datagram", datagram)
	}

	if err := clientQC.Close(0, ""); err != nil {
		t.Fatalf("close connection: %v", err)
	}
}
