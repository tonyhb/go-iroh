package relayproto

import "sync"

var (
	smallPool = sync.Pool{New: func() any { b := make([]byte, 2048); return &b }}
	largePool = sync.Pool{New: func() any { b := make([]byte, MaxPacketSize); return &b }}
)

// GetBuf returns a buffer of length n. A frame may encode to more than
// MaxPacketSize, which no pool here holds, so n above that is allocated.
func GetBuf(n int) *[]byte {
	var p *[]byte
	switch {
	case n <= 2048:
		p = smallPool.Get().(*[]byte)
	case n <= MaxPacketSize:
		p = largePool.Get().(*[]byte)
	default:
		b := make([]byte, n)
		return &b
	}
	*p = (*p)[:n]
	return p
}

// PutBuf returns a buffer obtained from GetBuf. A buffer larger than either
// pool holds is dropped rather than kept.
func PutBuf(p *[]byte) {
	switch c := cap(*p); {
	case c <= 2048:
		smallPool.Put(p)
	case c <= MaxPacketSize:
		largePool.Put(p)
	}
}
