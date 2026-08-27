package quic

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/tmc/go-iroh/internal/qng/internal/protocol"
)

func TestQNTGoToGoRoutePathCarriesData(t *testing.T) {
	maxPath := uint32(4)
	qntLimit := uint8(4)
	serverCfg := &Config{
		InitialMaxPathID:               &maxPath,
		MaxRemoteNATTraversalAddresses: &qntLimit,
		EnableDatagrams:                true,
		KeepAlivePeriod:                100 * time.Millisecond,
		MaxIdleTimeout:                 10 * time.Second,
	}
	clientCfg := &Config{
		InitialMaxPathID:               &maxPath,
		MaxRemoteNATTraversalAddresses: &qntLimit,
		EnableDatagrams:                true,
		KeepAlivePeriod:                100 * time.Millisecond,
		MaxIdleTimeout:                 10 * time.Second,
	}

	clientConn, serverConn, cleanup := twoEndpoints(t, serverCfg, clientCfg)
	defer cleanup()

	clientAddr := canonicalTestAddr(t, clientConn.LocalAddr().String())
	serverAddr := canonicalTestAddr(t, serverConn.LocalAddr().String())
	if err := clientConn.AddNATTraversalAddress(clientAddr); err != nil {
		t.Fatalf("client AddNATTraversalAddress: %v", err)
	}
	if err := serverConn.AddNATTraversalAddress(serverAddr); err != nil {
		t.Fatalf("server AddNATTraversalAddress: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	waitForNATAddress(t, ctx, clientConn, serverAddr)
	if addrs, err := serverConn.NATTraversalAddresses(); err != nil {
		t.Fatalf("server NATTraversalAddresses: %v", err)
	} else if slices.Contains(addrs, clientAddr) {
		t.Fatalf("server learned client NAT address from ADD_ADDRESS: %v", addrs)
	}

	addrs, err := clientConn.InitiateNATTraversalRound(ctx)
	if err != nil {
		t.Fatalf("client InitiateNATTraversalRound: %v", err)
	}
	if len(addrs) != 1 || addrs[0] != serverAddr {
		t.Fatalf("client QNT round addresses = %v, want [%v]", addrs, serverAddr)
	}

	path := waitForQNTRoutePath(t, ctx, clientConn, serverAddr)
	const msg = "qnt-route-datagram"
	if err := clientConn.SendDatagramOnPath(path.ID, []byte(msg)); err != nil {
		t.Fatalf("client SendDatagramOnPath(%d): %v", path.ID, err)
	}
	got, err := serverConn.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatalf("server ReceiveDatagram: %v", err)
	}
	if string(got) != msg {
		t.Fatalf("server received %q over QNT route, want %q", got, msg)
	}
	path = waitForQNTPathRTT(t, ctx, clientConn, path.ID)
	if path.SmoothedRTT <= 0 {
		t.Fatalf("path SmoothedRTT = %v, want measured RTT", path.SmoothedRTT)
	}
}

func canonicalTestAddr(t *testing.T, s string) netip.AddrPort {
	t.Helper()
	addr, err := netip.ParseAddrPort(s)
	if err != nil {
		t.Fatalf("parse addr %q: %v", s, err)
	}
	return netip.AddrPortFrom(addr.Addr().Unmap(), addr.Port())
}

func waitForNATAddress(t *testing.T, ctx context.Context, c *Conn, want netip.AddrPort) {
	t.Helper()
	for {
		addrs, err := c.NATTraversalAddresses()
		if err != nil {
			t.Fatalf("NATTraversalAddresses: %v", err)
		}
		if slices.Contains(addrs, want) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for remote NAT address %v; last addrs=%v", want, addrs)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitForQNTRoutePath(t *testing.T, ctx context.Context, c *Conn, want netip.AddrPort) PathInfo {
	t.Helper()
	for {
		paths := c.Paths()
		for _, p := range paths {
			if p.ID != protocol.PathIDZero && p.Validated && p.RemoteAddr == want {
				return p
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for validated QNT route path %v; last paths=%v qnt=%s closeCause=%v", want, paths, qntDebugState(c), context.Cause(c.Context()))
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// waitForQNTPathRTT waits for an application-data RTT sample on path id,
// sending a fresh datagram on that path each round.
//
// Resending is what makes this terminate. A sample is only taken when the
// largest newly acknowledged packet is not a path probe, so an ACK whose
// largest acked is a QNT probe yields none — and on a freshly validated path
// the probe can easily carry the higher packet number. Since nothing else is
// sent on the path afterwards, a single datagram leaves the path permanently
// without an RTT rather than merely slow to acquire one. Each round offers
// another non-probe candidate.
func waitForQNTPathRTT(t *testing.T, ctx context.Context, c *Conn, id protocol.PathID) PathInfo {
	t.Helper()
	for {
		paths := c.Paths()
		for _, p := range paths {
			if p.ID == id && p.HasRTT {
				return p
			}
		}
		if err := c.SendDatagramOnPath(id, []byte("qnt-route-rtt-probe")); err != nil {
			t.Fatalf("client SendDatagramOnPath(%d): %v", id, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for RTT on path %d; last paths=%v", id, paths)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func qntDebugState(c *Conn) string {
	st := c.qntLocalState()
	st.mu.Lock()
	defer st.mu.Unlock()
	return fmt.Sprintf("pendingProbes=%v sentProbes=%d validatedProbes=%v", st.pendingProbes, len(st.sentProbes), st.validatedProbes)
}
