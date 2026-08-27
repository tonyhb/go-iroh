//go:build !linux

package quic

func newGSOSendConn(gsoCapablePacketConn, bool) (rawConn, bool, error) {
	return nil, false, nil
}
