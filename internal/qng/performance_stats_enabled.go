//go:build iroh_performance_stats

package quic

import "sync/atomic"

type performanceCounters struct {
	stats PerformanceStats
}

type performanceSnapshotRequest struct {
	dst *PerformanceStats
}

type sendConnPerformanceCounters struct {
	datagrams   atomic.Uint64
	bytes       atomic.Uint64
	syscalls    atomic.Uint64
	gsoCalls    atomic.Uint64
	gsoSegments atomic.Uint64
}

var corkTimerActivations atomic.Uint64

func recordCorkTimerActivation() {
	corkTimerActivations.Add(1)
}

func (c *performanceCounters) recordPacket(p shortHeaderPacket) {
	if p.Ack != nil {
		c.stats.ACKFramesSent++
		if !p.HasStreamFrame && len(p.StreamFrames) == 0 && len(p.Frames) == 0 {
			c.stats.ACKOnlyPacketsSent++
		}
	}
	if p.HasStreamFrame {
		c.stats.StreamFramesSent++
		c.stats.StreamBytesSent += uint64(p.StreamFrame.Frame.DataLen())
	}
	for _, frame := range p.StreamFrames {
		c.stats.StreamFramesSent++
		c.stats.StreamBytesSent += uint64(frame.Frame.DataLen())
	}
}

func (c *performanceCounters) recordSendLoop() {
	c.stats.SendLoopRuns++
}

func (c *performanceCounters) recordStreamActivation() {
	c.stats.StreamActivations++
}

func (r performanceSnapshotRequest) fill(c *performanceCounters) {
	if r.dst != nil {
		*r.dst = c.stats
	}
}

func (c *sendConnPerformanceCounters) recordWrite(bytes int, gsoSize uint16) {
	segments := uint64(1)
	if gsoSize > 0 {
		segments = uint64((bytes + int(gsoSize) - 1) / int(gsoSize))
	}
	c.datagrams.Add(segments)
	c.bytes.Add(uint64(bytes))
	if gsoSize == 0 || segments < minGSOSegments {
		c.syscalls.Add(segments)
		return
	}
	c.syscalls.Add(1)
	c.gsoCalls.Add(1)
	c.gsoSegments.Add(segments)
}

func (c *sendConnPerformanceCounters) snapshot() PerformanceStats {
	return PerformanceStats{
		UDPDatagramsSent: c.datagrams.Load(),
		UDPBytesSent:     c.bytes.Load(),
		UDPSendSyscalls:  c.syscalls.Load(),
		UDPGSOSyscalls:   c.gsoCalls.Load(),
		UDPGSOSegments:   c.gsoSegments.Load(),
	}
}

// PerformanceStats returns a consistent snapshot of packetization counters.
func (c *Conn) PerformanceStats() PerformanceStats {
	dst := new(PerformanceStats)
	req := &pathSnapshotRequest{
		performance: performanceSnapshotRequest{dst: dst},
		done:        make(chan struct{}),
	}
	select {
	case c.pathSnapshotQueue <- req:
	case <-c.ctx.Done():
		return PerformanceStats{}
	}
	c.scheduleSending()
	select {
	case <-req.done:
		if conn, ok := c.conn.(*sconn); ok {
			udp := conn.performance.snapshot()
			dst.UDPDatagramsSent = udp.UDPDatagramsSent
			dst.UDPBytesSent = udp.UDPBytesSent
			dst.UDPSendSyscalls = udp.UDPSendSyscalls
			dst.UDPGSOSyscalls = udp.UDPGSOSyscalls
			dst.UDPGSOSegments = udp.UDPGSOSegments
		}
		dst.CorkTimerActivations = corkTimerActivations.Load()
		return *dst
	case <-c.ctx.Done():
		return PerformanceStats{}
	}
}
