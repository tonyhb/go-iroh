package relayproto

import (
	"context"
	"io"

	"github.com/coder/websocket"
)

// FrameReader reads WebSocket messages into a reused buffer. The returned
// slice is valid until the next Read.
type FrameReader struct {
	buf []byte
}

func (r *FrameReader) Read(ctx context.Context, c *websocket.Conn) ([]byte, error) {
	_, rd, err := c.Reader(ctx)
	if err != nil {
		return nil, err
	}
	buf := r.buf[:0]
	if cap(buf) == 0 {
		buf = make([]byte, 0, 2048)
	}
	for {
		n, err := rd.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err == io.EOF {
			r.buf = buf
			return buf, nil
		}
		if err != nil {
			return nil, err
		}
		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)]
		}
	}
}
