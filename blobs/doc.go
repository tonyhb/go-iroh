// Package blobs defines Rust-compatible iroh blob tickets, blob identifiers,
// and transfer helpers.
//
// A blob ticket combines a provider endpoint address with a BLAKE3 hash and the
// blob format to request. Its string form is the "blob" kind prefix followed by
// RFC 4648 base32 without padding, matching Rust's iroh-blobs ticket format.
// The transfer helpers implement the raw full-blob subset of the iroh-blobs
// provider protocol.
//
// The Go API is not stable before v1 and may change in any v0 release; the
// wire format tracks pinned upstream iroh-blobs releases and does not.
package blobs
