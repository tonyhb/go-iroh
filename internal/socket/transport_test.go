package socket_test

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	quic "github.com/tmc/go-iroh/internal/qng"
	"github.com/tmc/go-iroh/internal/socket"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

type fakeCustomTransport struct {
	mu    sync.Mutex
	sends []fakeCustomSend
	recv  chan socket.CustomDatagram
}

type fakePacketTransport struct {
	fakeCustomTransport

	packetRecv chan socket.Packet

	mu          sync.Mutex
	packetSends []fakeCustomSend
}

func newFakePacketTransport() *fakePacketTransport {
	return &fakePacketTransport{
		fakeCustomTransport: *newFakeCustomTransport(),
		packetRecv:          make(chan socket.Packet, 4),
	}
}

func (t *fakePacketTransport) ServePackets(ctx context.Context, recv func(socket.Packet) bool) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-t.packetRecv:
			recv(p)
		}
	}
}

func (t *fakePacketTransport) SendPacket(remote netaddr.CustomAddr, local *netaddr.CustomAddr, p []byte) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	var localCopy *netaddr.CustomAddr
	if local != nil {
		v := *local
		localCopy = &v
	}
	t.packetSends = append(t.packetSends, fakeCustomSend{
		remote: remote,
		local:  localCopy,
		data:   append([]byte(nil), p...),
	})
	return true
}

func (t *fakePacketTransport) lastPacketSend() (fakeCustomSend, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.packetSends) == 0 {
		return fakeCustomSend{}, false
	}
	return t.packetSends[len(t.packetSends)-1], true
}

type fakeCustomSend struct {
	remote netaddr.CustomAddr
	local  *netaddr.CustomAddr
	data   []byte
}

func newFakeCustomTransport() *fakeCustomTransport {
	return &fakeCustomTransport{recv: make(chan socket.CustomDatagram, 4)}
}

func (t *fakeCustomTransport) Serve(ctx context.Context, recv func(socket.CustomDatagram) bool) {
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-t.recv:
			recv(d)
		}
	}
}

func (t *fakeCustomTransport) Send(remote netaddr.CustomAddr, local *netaddr.CustomAddr, p []byte) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	var localCopy *netaddr.CustomAddr
	if local != nil {
		v := *local
		localCopy = &v
	}
	t.sends = append(t.sends, fakeCustomSend{
		remote: remote,
		local:  localCopy,
		data:   append([]byte(nil), p...),
	})
	return true
}

func (t *fakeCustomTransport) lastSend() (fakeCustomSend, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.sends) == 0 {
		return fakeCustomSend{}, false
	}
	return t.sends[len(t.sends)-1], true
}

// newLoopbackMagic binds a loopback UDP socket and returns a serving MagicConn
// plus a stop func.
func newLoopbackMagic(t *testing.T) (*socket.MagicConn, *net.UDPConn, func()) {
	t.Helper()
	udp, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	sock := socket.NewSocket()
	m := socket.NewMagicConn(sock, udp)
	ctx, cancel := context.WithCancel(context.Background())
	go m.Serve(ctx)
	stop := func() {
		cancel()
		m.Close()
	}
	return m, udp, stop
}

// TestMagicConnIPRecvPassThrough checks an IP datagram read through the magic
// socket surfaces the real remote IP unchanged: the IP transport must not rewrite
// the source address into a mapped ULA (iroh/src/socket/transports/ip.rs:219).
func TestMagicConnIPRecvPassThrough(t *testing.T) {
	m, udp, stop := newLoopbackMagic(t)
	defer stop()

	// A second socket sends a datagram to the magic socket's bound port.
	sender, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	const payload = "direct-ip"
	if _, err := sender.WriteTo([]byte(payload), udp.LocalAddr()); err != nil {
		t.Fatal(err)
	}

	m.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	n, addr, err := m.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if string(buf[:n]) != payload {
		t.Errorf("payload = %q, want %q", buf[:n], payload)
	}

	got, ok := addr.(*net.UDPAddr)
	if !ok {
		t.Fatalf("addr type = %T, want *net.UDPAddr", addr)
	}
	wantPort := sender.LocalAddr().(*net.UDPAddr).Port
	if got.Port != wantPort {
		t.Errorf("recv addr port = %d, want %d", got.Port, wantPort)
	}
	// The source IP must be a real loopback address, never a mapped ULA.
	if socket.Classify(got.AddrPort().Addr()) != socket.KindIP {
		t.Errorf("recv addr %s classified as mapped, want real IP", got)
	}
}

// TestMagicConnIPSend checks WriteTo to a real IP routes out the IP transport:
// the datagram arrives on a plain UDP socket at the target address.
func TestMagicConnIPSend(t *testing.T) {
	m, _, stop := newLoopbackMagic(t)
	defer stop()

	dst, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()

	const payload = "out-the-ip-transport"
	n, err := m.WriteTo([]byte(payload), dst.LocalAddr())
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != len(payload) {
		t.Errorf("WriteTo n = %d, want %d", n, len(payload))
	}

	dst.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	rn, _, err := dst.ReadFrom(buf)
	if err != nil {
		t.Fatalf("dst.ReadFrom: %v", err)
	}
	if string(buf[:rn]) != payload {
		t.Errorf("received %q, want %q", buf[:rn], payload)
	}
}

// TestMagicConnBlackhole checks the blackhole invariant: a WriteTo to a mapped
// address with no live transport (relay/custom/endpoint-id), and a WriteTo after
// Close, both report success so quic-go's loss recovery — not a hard error —
// handles the lost datagram (iroh/src/socket/transports.rs:1176).
func TestMagicConnBlackhole(t *testing.T) {
	m, _, stop := newLoopbackMagic(t)
	defer stop()

	mapped := socket.NewRelayMappedAddr().AddrPort()
	dst := net.UDPAddrFromAddrPort(mapped)
	payload := []byte("relay-not-wired-yet")
	n, err := m.WriteTo(payload, dst)
	if err != nil {
		t.Errorf("WriteTo(mapped) err = %v, want nil (blackhole)", err)
	}
	if n != len(payload) {
		t.Errorf("WriteTo(mapped) n = %d, want %d", n, len(payload))
	}

	// After Close, sends are still reported as success.
	stop()
	n, err = m.WriteTo(payload, net.UDPAddrFromAddrPort(
		netip.AddrPortFrom(netip.IPv6Loopback(), 9)))
	if err != nil {
		t.Errorf("WriteTo after close err = %v, want nil (blackhole)", err)
	}
	if n != len(payload) {
		t.Errorf("WriteTo after close n = %d, want %d", n, len(payload))
	}
}

func TestMagicConnCustomSend(t *testing.T) {
	udp, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()

	sock := socket.NewSocket()
	custom := newFakeCustomTransport()
	m := socket.NewMagicConnWithTransports(sock, udp, nil, custom)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Serve(ctx)
	defer m.Close()

	remote := netaddr.NewCustomAddr(7, []byte("remote"))
	mapped := sock.CustomMappedAddrFor(remote)
	const payload = "custom-send"
	n, err := m.WriteTo([]byte(payload), net.UDPAddrFromAddrPort(mapped.AddrPort()))
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("WriteTo n = %d, want %d", n, len(payload))
	}

	deadline := time.After(2 * time.Second)
	for {
		send, ok := custom.lastSend()
		if ok {
			if send.remote.String() != remote.String() {
				t.Fatalf("send remote = %v, want %v", send.remote, remote)
			}
			if send.local != nil {
				t.Fatalf("send local = %v, want nil", send.local)
			}
			if string(send.data) != payload {
				t.Fatalf("send data = %q, want %q", send.data, payload)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for custom send")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestMagicConnCustomRecvRewrite(t *testing.T) {
	udp, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()

	sock := socket.NewSocket()
	custom := newFakeCustomTransport()
	m := socket.NewMagicConnWithTransports(sock, udp, nil, custom)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Serve(ctx)
	defer m.Close()

	remote := netaddr.NewCustomAddr(9, []byte("peer"))
	const payload = "custom-recv"
	custom.recv <- socket.CustomDatagram{Remote: remote, Data: []byte(payload)}

	m.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	n, addr, err := m.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if string(buf[:n]) != payload {
		t.Fatalf("payload = %q, want %q", buf[:n], payload)
	}
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		t.Fatalf("addr type = %T, want *net.UDPAddr", addr)
	}
	if socket.Classify(udpAddr.AddrPort().Addr()) != socket.KindCustom {
		t.Fatalf("addr = %v, want custom mapped", udpAddr)
	}
	if got, ok := sock.LookupCustom(socket.CustomMappedAddrFromAddr(udpAddr.AddrPort().Addr())); !ok || got.String() != remote.String() {
		t.Fatalf("LookupCustom = %v, %v; want %v", got, ok, remote)
	}
}

func TestMagicConnPacketTransportRecvReleasesBuffer(t *testing.T) {
	udp, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()

	sock := socket.NewSocket()
	packet := newFakePacketTransport()
	m := socket.NewMagicConnWithTransports(sock, udp, nil, packet)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Serve(ctx)
	defer m.Close()

	remote := netaddr.NewCustomAddr(12, []byte("packet-peer"))
	const payload = "packet-recv"
	freed := make(chan struct{})
	packet.packetRecv <- socket.Packet{
		Remote: remote,
		Data:   []byte(payload),
		Free:   func() { close(freed) },
	}

	m.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	n, _, err := m.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if string(buf[:n]) != payload {
		t.Fatalf("payload = %q, want %q", buf[:n], payload)
	}
	select {
	case <-freed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for packet buffer release")
	}
}

func TestMagicConnPacketTransportSendUsesPacketPath(t *testing.T) {
	udp, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()

	sock := socket.NewSocket()
	packet := newFakePacketTransport()
	m := socket.NewMagicConnWithTransports(sock, udp, nil, packet)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Serve(ctx)
	defer m.Close()

	remote := netaddr.NewCustomAddr(13, []byte("packet-send"))
	mapped := sock.CustomMappedAddrFor(remote)
	const payload = "packet-send"
	n, err := m.WriteTo([]byte(payload), net.UDPAddrFromAddrPort(mapped.AddrPort()))
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("WriteTo n = %d, want %d", n, len(payload))
	}

	deadline := time.After(2 * time.Second)
	for {
		send, ok := packet.lastPacketSend()
		if ok {
			if send.remote.String() != remote.String() {
				t.Fatalf("send remote = %v, want %v", send.remote, remote)
			}
			if string(send.data) != payload {
				t.Fatalf("send data = %q, want %q", send.data, payload)
			}
			if _, ok := packet.lastSend(); ok {
				t.Fatal("compatibility Send was called for packet transport")
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for packet send")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestMagicConnEndpointIDSend(t *testing.T) {
	udp, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()

	sock := socket.NewSocket()
	m := socket.NewMagicConn(sock, udp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Serve(ctx)
	defer m.Close()

	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	id := sk.Public().EndpointID()
	mapped := sock.EndpointIDMappedAddrFor(id)

	got := make(chan []byte, 1)
	m.SetEndpointSender(func(remote key.EndpointID, p []byte) bool {
		if !remote.Equal(id) {
			t.Errorf("remote = %s, want %s", remote, id)
		}
		got <- append([]byte(nil), p...)
		return true
	})

	const payload = "endpoint-id-send"
	n, err := m.WriteTo([]byte(payload), net.UDPAddrFromAddrPort(mapped.AddrPort()))
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("WriteTo n = %d, want %d", n, len(payload))
	}

	select {
	case b := <-got:
		if string(b) != payload {
			t.Fatalf("payload = %q, want %q", b, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for endpoint sender")
	}
}

// TestMagicConnReadDeadline checks ReadFrom honors a past read deadline by
// returning a timeout net.Error rather than blocking.
func TestMagicConnReadDeadline(t *testing.T) {
	m, _, stop := newLoopbackMagic(t)
	defer stop()

	m.SetReadDeadline(time.Now().Add(-time.Second))
	_, _, err := m.ReadFrom(make([]byte, 16))
	ne, ok := err.(net.Error)
	if !ok || !ne.Timeout() {
		t.Errorf("ReadFrom past deadline err = %v, want timeout net.Error", err)
	}
}

// TestMagicConnNotOOBCapable pins that MagicConn deliberately does not satisfy
// quic-go's OOBCapablePacketConn. It does expose SyscallConn for buffer sizing
// and the DF bit, which alone does not make it OOB-capable.
func TestMagicConnNotOOBCapable(t *testing.T) {
	m, _, stop := newLoopbackMagic(t)
	defer stop()

	if _, ok := any(m).(quic.OOBCapablePacketConn); ok {
		t.Error("MagicConn satisfies OOBCapablePacketConn; receive-side OOB metadata does not generalize across transports")
	}
	if _, ok := any(m).(net.PacketConn); !ok {
		t.Error("MagicConn must satisfy net.PacketConn")
	}
}

// TestAddrCanonical checks IPAddr canonicalizes an IPv4-mapped IPv6 address back
// to plain IPv4, matching iroh/src/socket/transports.rs:825.
func TestAddrCanonical(t *testing.T) {
	v4mapped := netip.MustParseAddr("::ffff:192.0.2.7")
	a := socket.IPAddr(netip.AddrPortFrom(v4mapped, 443))
	ap, ok := a.IP()
	if !ok {
		t.Fatal("IP() = _, false")
	}
	if !ap.Addr().Is4() {
		t.Errorf("addr = %s, want plain IPv4", ap.Addr())
	}
	if ap.Addr().String() != "192.0.2.7" {
		t.Errorf("addr = %s, want 192.0.2.7", ap.Addr())
	}
}

// TestSocketRelayRoundTrip checks the relay mapped-address table round-trips a
// (url, eid) pair to a stable mapped address and back.
func TestSocketRelayRoundTrip(t *testing.T) {
	s := socket.NewSocket()
	url := netaddr.RelayURL{}
	eid := key.EndpointID{}

	m1 := s.RelayMappedAddrFor(url, eid)
	m2 := s.RelayMappedAddrFor(url, eid)
	if m1 != m2 {
		t.Error("RelayMappedAddrFor is not stable for the same key")
	}
	if socket.Classify(m1.Addr()) != socket.KindRelay {
		t.Errorf("relay mapped addr %s did not classify as relay", m1.Addr())
	}
	key, ok := s.LookupRelay(m1)
	if !ok || !key.EID.Equal(eid) {
		t.Errorf("LookupRelay = %+v,%v, want eid %s,true", key, ok, eid)
	}
}
