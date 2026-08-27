package socket

// PerformanceStats contains direct-IP receive counters collected by binaries
// built with the iroh_performance_stats build tag.
type PerformanceStats struct {
	UDPReceiveSyscalls   uint64
	UDPDatagramsReceived uint64
	UDPGROReads          uint64
}
