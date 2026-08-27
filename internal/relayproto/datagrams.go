package relayproto

import "bytes"

// Datagrams is one or multiple datagrams transferred via the relay, modeled
// after the QUIC transmit structure.
type Datagrams struct {
	// Ecn is the explicit congestion notification codepoint, or 0 for Not-ECT.
	Ecn EcnCodepoint
	// SegmentSize is the per-datagram segment size when this transmit carries
	// multiple datagrams (a batch); 0 means a single datagram.
	SegmentSize uint16
	// Contents holds the datagram bytes.
	Contents []byte
	buf      *[]byte
}

// DatagramsFromBytes wraps b as a single (non-batch) datagram.
func DatagramsFromBytes(b []byte) Datagrams {
	return Datagrams{Contents: bytes.Clone(b)}
}

// isBatch reports whether the datagram is a batch (has a segment size).
func (d Datagrams) isBatch() bool { return d.SegmentSize != 0 }

// appendTo appends the wire encoding of d (ECN byte, optional segment size,
// then contents) to dst.
func (d Datagrams) appendTo(dst []byte) []byte {
	dst = append(dst, byte(d.Ecn))
	if d.SegmentSize != 0 {
		dst = append(dst, byte(d.SegmentSize>>8), byte(d.SegmentSize))
	}
	return append(dst, d.Contents...)
}

// encodedLen returns the number of bytes appendTo writes.
func (d Datagrams) encodedLen() int {
	n := 1 + len(d.Contents)
	if d.SegmentSize != 0 {
		n += 2
	}
	return n
}

// datagramsFromBytes decodes a Datagrams payload. isBatch selects whether a
// 2-byte segment size precedes the contents.
func datagramsFromBytes(b []byte, isBatch bool) (Datagrams, error) {
	return datagramsFromBytesCopy(b, isBatch, true)
}

func datagramsFromBytesNoCopy(b []byte, isBatch bool) (Datagrams, error) {
	return datagramsFromBytesCopy(b, isBatch, false)
}

func datagramsFromBytesCopy(b []byte, isBatch, copyContents bool) (Datagrams, error) {
	if isBatch {
		if len(b) < 3 {
			return Datagrams{}, ErrInvalidFrame
		}
	} else if len(b) < 1 {
		return Datagrams{}, ErrInvalidFrame
	}
	ecn, _ := ecnFromBits(b[0])
	b = b[1:]
	var segSize uint16
	if isBatch {
		segSize = uint16(b[0])<<8 | uint16(b[1])
		b = b[2:]
	}
	if copyContents {
		b = bytes.Clone(b)
	}
	return Datagrams{Ecn: ecn, SegmentSize: segSize, Contents: b}, nil
}

// Pooled returns a copy of d whose Contents live in a pooled buffer. Call
// Release when done with it.
func (d Datagrams) Pooled() Datagrams {
	p := GetBuf(len(d.Contents))
	copy(*p, d.Contents)
	d.Contents = *p
	d.buf = p
	return d
}

// Release returns a buffer obtained via Pooled. It is a no-op otherwise.
func (d Datagrams) Release() {
	if d.buf != nil {
		PutBuf(d.buf)
	}
}
