//go:build !iroh_performance_stats

package socket

func recordUDPReceive(int, bool) {}

// SnapshotPerformanceStats returns zero counters in an ordinary build.
func SnapshotPerformanceStats() PerformanceStats { return PerformanceStats{} }
