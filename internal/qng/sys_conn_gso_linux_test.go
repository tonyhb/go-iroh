//go:build linux

package quic

import (
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/tmc/go-iroh/internal/qng/internal/ackhandler"
	"github.com/tmc/go-iroh/internal/qng/internal/protocol"
	"github.com/tmc/go-iroh/internal/qng/internal/wire"
	"github.com/tmc/go-iroh/internal/socket"
)

func TestPacketHasDatagram(t *testing.T) {
	tests := []struct {
		name  string
		frame wire.Frame
		want  bool
	}{
		{name: "ping", frame: &wire.PingFrame{}},
		{name: "datagram", frame: &wire.DatagramFrame{}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := shortHeaderPacket{Frames: []ackhandler.Frame{{Frame: tt.frame}}}
			if got := packetHasDatagram(p); got != tt.want {
				t.Fatalf("packetHasDatagram = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGSOSendConnSplitsSmallBatch(t *testing.T) {
	sender, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	receiver, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	conn := &gsoSendConn{basicConn: &basicConn{PacketConn: sender}}
	if n, err := conn.WritePacket([]byte("abcdefg"), receiver.LocalAddr(), nil, 3, protocol.ECNUnsupported); err != nil {
		t.Fatal(err)
	} else if n != 7 {
		t.Fatalf("WritePacket wrote %d bytes, want 7", n)
	}
	for _, want := range []string{"abc", "def", "g"} {
		buf := make([]byte, 8)
		n, _, err := receiver.ReadFromUDP(buf)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(buf[:n]); got != want {
			t.Fatalf("ReadFromUDP = %q, want %q", got, want)
		}
	}
}

func TestWrapConnGSOSendOnly(t *testing.T) {
	udp, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	m := socket.NewMagicConn(socket.NewSocket(), udp)
	t.Cleanup(func() { m.Close() })
	raw, err := m.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if !isGSOEnabled(raw) {
		t.Skip("UDP GSO is unavailable")
	}

	conn, err := wrapConn(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := conn.(*gsoSendConn); !ok {
		t.Fatalf("wrapConn returned %T, want *gsoSendConn", conn)
	}
	capabilities := conn.capabilities()
	if !capabilities.GSO {
		t.Fatal("GSO is disabled")
	}
	if capabilities.ECN {
		t.Fatal("ECN is enabled without an OOB receive path")
	}
}

func TestWrapConnGSOSendDisabled(t *testing.T) {
	t.Setenv("QUIC_GO_DISABLE_GSO", "true")
	udp, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}
	m := socket.NewMagicConn(socket.NewSocket(), udp)
	t.Cleanup(func() { m.Close() })

	conn, err := wrapConn(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := conn.(*basicConn); !ok {
		t.Fatalf("wrapConn returned %T, want *basicConn", conn)
	}
}
