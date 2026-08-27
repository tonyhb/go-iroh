package relayproto

import "testing"

func TestGetBufLen(t *testing.T) {
	// The largest batch a peer may send encodes to one byte more than the
	// large pool holds, which is why GetBuf cannot assume a pooled buffer.
	maxContents := MaxPacketSize - 32 - 3
	msg := RelayToClientMsg{
		Type:      FrameRelayToClientDatagramBat,
		Datagrams: Datagrams{SegmentSize: 1200, Contents: make([]byte, maxContents)},
	}
	for _, n := range []int{0, 1, 2048, 2049, MaxPacketSize, msg.EncodedLen(), MaxPacketSize * 2} {
		p := GetBuf(n)
		if len(*p) != n {
			t.Errorf("len(GetBuf(%d)) = %d", n, len(*p))
		}
		*p = (*p)[:0]
		PutBuf(p)
	}
}
