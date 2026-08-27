// Package docs implements Rust-compatible iroh-docs data and sync protocols.
//
// The package currently covers the stable value types used to share document
// capabilities out of band, an in-memory entry store, the iroh-docs sync
// protocol over an iroh Router, and live synchronization over iroh-gossip.
//
// The Go API is not stable before v1 and may change in any v0 release; the
// wire format tracks pinned upstream iroh-docs releases and does not.
package docs
