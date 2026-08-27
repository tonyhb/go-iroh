//go:build !iroh_performance_stats

package quic

type performanceCounters struct{}
type performanceSnapshotRequest struct{}
type sendConnPerformanceCounters struct{}

func (*performanceCounters) recordPacket(shortHeaderPacket)  {}
func (*performanceCounters) recordSendLoop()                 {}
func (*performanceCounters) recordStreamActivation()         {}
func (performanceSnapshotRequest) fill(*performanceCounters) {}
func (*sendConnPerformanceCounters) recordWrite(int, uint16) {}
func recordCorkTimerActivation()                             {}
func (*sendConnPerformanceCounters) snapshot() PerformanceStats {
	return PerformanceStats{}
}

// PerformanceStats returns zero counters in an ordinary build.
func (*Conn) PerformanceStats() PerformanceStats { return PerformanceStats{} }
