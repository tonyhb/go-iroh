package quicconn

import (
	"context"
	"time"

	"github.com/tmc/go-iroh/iroh"
)

// BidiStream is an HTTP/3 bidirectional stream.
type BidiStream struct {
	s *iroh.Stream
}

// SendStream is an HTTP/3 unidirectional send stream.
type SendStream struct {
	s *iroh.SendStream
}

// ReceiveStream is an HTTP/3 unidirectional receive stream.
type ReceiveStream struct {
	s *iroh.ReceiveStream
}

// Read reads from s.
func (s *BidiStream) Read(p []byte) (int, error) { return s.s.Read(p) }

// Write writes to s.
func (s *BidiStream) Write(p []byte) (int, error) { return s.s.Write(p) }

// Close closes the send side of s.
func (s *BidiStream) Close() error { return s.s.Close() }

// SetDeadline sets read and write deadlines for s.
func (s *BidiStream) SetDeadline(t time.Time) error { return s.s.SetDeadline(t) }

// SetReadDeadline sets the read deadline for s.
func (s *BidiStream) SetReadDeadline(t time.Time) error { return s.s.SetReadDeadline(t) }

// SetWriteDeadline sets the write deadline for s.
func (s *BidiStream) SetWriteDeadline(t time.Time) error { return s.s.SetWriteDeadline(t) }

// CancelRead aborts receiving on s with code.
func (s *BidiStream) CancelRead(code uint64) { s.s.CancelRead(code) }

// CancelWrite aborts sending on s with code.
func (s *BidiStream) CancelWrite(code uint64) { s.s.CancelWrite(code) }

// Context is cancelled when s is closed.
func (s *BidiStream) Context() context.Context { return s.s.Context() }

// IrohStream returns the underlying iroh stream.
func (s *BidiStream) IrohStream() *iroh.Stream {
	if s == nil {
		return nil
	}
	return s.s
}

// Write writes to s.
func (s *SendStream) Write(p []byte) (int, error) { return s.s.Write(p) }

// Close closes s.
func (s *SendStream) Close() error { return s.s.Close() }

// SetWriteDeadline sets the write deadline for s.
func (s *SendStream) SetWriteDeadline(t time.Time) error { return s.s.SetWriteDeadline(t) }

// CancelWrite aborts sending on s with code.
func (s *SendStream) CancelWrite(code uint64) { s.s.CancelWrite(code) }

// Context is cancelled when s is closed.
func (s *SendStream) Context() context.Context { return s.s.Context() }

// IrohSendStream returns the underlying iroh send stream.
func (s *SendStream) IrohSendStream() *iroh.SendStream {
	if s == nil {
		return nil
	}
	return s.s
}

// Read reads from s.
func (s *ReceiveStream) Read(p []byte) (int, error) { return s.s.Read(p) }

// SetReadDeadline sets the read deadline for s.
func (s *ReceiveStream) SetReadDeadline(t time.Time) error { return s.s.SetReadDeadline(t) }

// CancelRead aborts receiving on s with code.
func (s *ReceiveStream) CancelRead(code uint64) { s.s.CancelRead(code) }

// IrohReceiveStream returns the underlying iroh receive stream.
func (s *ReceiveStream) IrohReceiveStream() *iroh.ReceiveStream {
	if s == nil {
		return nil
	}
	return s.s
}
