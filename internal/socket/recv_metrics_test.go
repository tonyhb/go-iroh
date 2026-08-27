package socket

import (
	"context"
	"net"
	"testing"
	"time"
)

// A datagram received over IPv4 must count as IPv4. The IP transport reports
// its source address in recvBatch.ip rather than in info.Remote, so recording
// info.Remote directly attributed every direct-IP datagram to the IPv6
// counter (Addr's zero value has kind AddrIP and an invalid address).
func TestRecvCountersByAddressFamily(t *testing.T) {
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	m := NewMagicConn(NewSocket(), udp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Serve(ctx)
	defer m.Close()

	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if _, err := sender.WriteToUDP([]byte("v4"), m.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	if err := m.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ReadFrom(make([]byte, 2048)); err != nil {
		t.Fatal(err)
	}
	got := m.Metrics()
	if got.IPv4Recv != 1 || got.IPv6Recv != 0 {
		t.Errorf("after one IPv4 datagram: ipv4Recv=%d ipv6Recv=%d, want 1 and 0", got.IPv4Recv, got.IPv6Recv)
	}
}
