package quicconn

import (
	"context"

	"github.com/tmc/go-iroh/iroh"
)

// Conn adapts an [iroh.Conn] to the stream and datagram operations used by
// HTTP/3 transports.
type Conn struct {
	c *iroh.Conn
}

// NewConn returns an HTTP/3 transport adapter for c.
func NewConn(c *iroh.Conn) *Conn {
	return &Conn{c: c}
}

// IrohConn returns the underlying iroh connection.
func (c *Conn) IrohConn() *iroh.Conn {
	if c == nil {
		return nil
	}
	return c.c
}

// OpenBidi opens a bidirectional stream.
func (c *Conn) OpenBidi(ctx context.Context) (*BidiStream, error) {
	s, err := c.c.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &BidiStream{s: s}, nil
}

// AcceptBidi accepts a bidirectional stream.
func (c *Conn) AcceptBidi(ctx context.Context) (*BidiStream, error) {
	s, err := c.c.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &BidiStream{s: s}, nil
}

// OpenUni opens a unidirectional send stream.
func (c *Conn) OpenUni(ctx context.Context) (*SendStream, error) {
	s, err := c.c.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &SendStream{s: s}, nil
}

// AcceptUni accepts a unidirectional receive stream.
func (c *Conn) AcceptUni(ctx context.Context) (*ReceiveStream, error) {
	s, err := c.c.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	return &ReceiveStream{s: s}, nil
}

// SendDatagram sends an unreliable datagram.
func (c *Conn) SendDatagram(b []byte) error {
	return c.c.SendDatagram(b)
}

// ReceiveDatagram receives an unreliable datagram.
func (c *Conn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return c.c.ReadDatagram(ctx)
}

// Close closes the connection with an HTTP/3 error code and reason.
func (c *Conn) Close(code uint64, reason string) error {
	return c.c.CloseWithError(code, reason)
}
