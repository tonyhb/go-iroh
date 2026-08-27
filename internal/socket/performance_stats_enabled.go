//go:build iroh_performance_stats

package socket

import "sync/atomic"

var performanceStats struct {
	receiveSyscalls atomic.Uint64
	datagrams       atomic.Uint64
	groReads        atomic.Uint64
}

func recordUDPReceive(datagrams int, gro bool) {
	performanceStats.receiveSyscalls.Add(1)
	performanceStats.datagrams.Add(uint64(datagrams))
	if gro {
		performanceStats.groReads.Add(1)
	}
}

// SnapshotPerformanceStats returns the process-wide direct-IP receive counters.
func SnapshotPerformanceStats() PerformanceStats {
	return PerformanceStats{
		UDPReceiveSyscalls:   performanceStats.receiveSyscalls.Load(),
		UDPDatagramsReceived: performanceStats.datagrams.Load(),
		UDPGROReads:          performanceStats.groReads.Load(),
	}
}
