// Package quicconn adapts iroh connections to a QUIC-like surface.
//
// It provides a small connection and stream surface over [iroh.Conn] so
// higher-level packages — an HTTP/3 stack, for example — can build on
// go-iroh without importing internal transport packages or adding another
// QUIC stack. The package implements no application protocol of its own.
//
// The Go API is not stable before v1 and may change in any v0 release.
package quicconn
