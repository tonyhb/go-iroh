// Package endpointticket encodes and decodes Rust-compatible iroh endpoint
// tickets.
//
// An endpoint ticket is a compact string form of an endpoint address for
// out-of-band sharing. The string starts with "endpoint" followed by lowercase
// base32 without padding.
//
// The Go API is not stable before v1 and may change in any v0 release.
package endpointticket
