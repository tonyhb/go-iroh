package quic

import "time"

var (
	sendStreamBurstFreshness = 10 * time.Microsecond
	sendStreamTailDelay      = 5 * time.Microsecond
)

const (
	sendStreamBurstMinWrites      = 4
	sendStreamActivationThreshold = 1200
)
