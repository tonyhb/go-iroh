package fuzztape

import "encoding/binary"

// A Tape consumes a fuzz input as a sequence of typed decisions.
// The zero Tape is empty and yields zero values forever.
type Tape struct {
	data []byte
	pos  int
}

// New returns a Tape reading data.
func New(data []byte) *Tape {
	return &Tape{data: data}
}

// Done reports whether the input is exhausted. Further reads succeed and
// return zero values.
func (t *Tape) Done() bool {
	return t.pos >= len(t.data)
}

// Pos returns the number of input bytes consumed so far. Reads past
// the end of the input do not advance it.
func (t *Tape) Pos() int {
	return t.pos
}

// Byte returns the next byte, or 0 when the input is exhausted.
func (t *Tape) Byte() byte {
	if t.pos >= len(t.data) {
		return 0
	}
	b := t.data[t.pos]
	t.pos++
	return b
}

// Bool returns the next boolean decision.
func (t *Tape) Bool() bool {
	return t.Byte()&1 == 1
}

// Uint64 returns the next 8 bytes as a little-endian integer,
// zero-filled past the end of the input.
func (t *Tape) Uint64() uint64 {
	var buf [8]byte
	for i := range buf {
		buf[i] = t.Byte()
	}
	return binary.LittleEndian.Uint64(buf[:])
}

// IntN returns an integer in [0, n). It panics if n <= 0.
//
// The distribution is deliberately not uniform: one leading selector
// byte occasionally forces a boundary value — 0, 1, n-1, or a power of
// two — because most bugs live at boundaries and uniform bytes rarely
// land there. A zero tape decodes to 0.
func (t *Tape) IntN(n int) int {
	if n <= 0 {
		panic("fuzztape: IntN called with n <= 0")
	}
	switch t.Byte() {
	case 0:
		return 0
	case 1:
		return min(1, n-1)
	case 2:
		return n - 1
	case 3:
		p := 1
		for p*2 < n {
			p *= 2
		}
		return min(p, n-1)
	}
	return int(t.Uint64() % uint64(n))
}

// Bytes returns a slice of length in [0, max], drawn from the tape and
// zero-filled past the end of the input.
func (t *Tape) Bytes(max int) []byte {
	if max < 0 {
		max = 0
	}
	out := make([]byte, t.IntN(max+1))
	for i := range out {
		out[i] = t.Byte()
	}
	return out
}

// Pick returns one of opts, chosen by the tape. It panics if opts is
// empty. A zero tape picks opts[0].
func Pick[T any](t *Tape, opts []T) T {
	return opts[t.IntN(len(opts))]
}
