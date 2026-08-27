package quic

import "testing"

func TestPacketInfoOOBReservesControlMessageSpace(t *testing.T) {
	oob := packetInfoOOB(packetInfo{})
	if got := cap(oob) - len(oob); got < 64 {
		t.Fatalf("packetInfoOOB spare capacity = %d, want at least 64", got)
	}
}
