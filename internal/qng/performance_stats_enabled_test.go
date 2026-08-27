//go:build iroh_performance_stats

package quic

import "testing"

func TestSendConnPerformanceCounters(t *testing.T) {
	tests := []struct {
		name          string
		bytes         int
		gsoSize       uint16
		wantDatagrams uint64
		wantSyscalls  uint64
		wantGSOCalls  uint64
	}{
		{name: "single", bytes: 1200, wantDatagrams: 1, wantSyscalls: 1},
		{name: "split small batch", bytes: 3 * 1200, gsoSize: 1200, wantDatagrams: 3, wantSyscalls: 3},
		{name: "GSO batch", bytes: 4 * 1200, gsoSize: 1200, wantDatagrams: 4, wantSyscalls: 1, wantGSOCalls: 1},
		{name: "GSO short tail", bytes: 4*1200 + 100, gsoSize: 1200, wantDatagrams: 5, wantSyscalls: 1, wantGSOCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var counters sendConnPerformanceCounters
			counters.recordWrite(tt.bytes, tt.gsoSize)
			got := counters.snapshot()
			if got.UDPDatagramsSent != tt.wantDatagrams || got.UDPSendSyscalls != tt.wantSyscalls || got.UDPGSOSyscalls != tt.wantGSOCalls {
				t.Fatalf("snapshot = %+v; want datagrams %d, syscalls %d, GSO calls %d", got, tt.wantDatagrams, tt.wantSyscalls, tt.wantGSOCalls)
			}
			if got.UDPBytesSent != uint64(tt.bytes) {
				t.Fatalf("UDP bytes = %d, want %d", got.UDPBytesSent, tt.bytes)
			}
			wantSegments := uint64(0)
			if tt.wantGSOCalls != 0 {
				wantSegments = tt.wantDatagrams
			}
			if got.UDPGSOSegments != wantSegments {
				t.Fatalf("GSO segments = %d, want %d", got.UDPGSOSegments, wantSegments)
			}
		})
	}
}
