// Package dns provides DNS-based endpoint discovery for go-iroh: publishing and
// resolving endpoint addressing information via DNS using the pkarr signed
// packet format.
//
// It is a port of the Rust crate iroh-dns. The central types are [EndpointData]
// (a relay URL, direct addresses, and optional user data about an endpoint) and
// [EndpointInfo] (an [key.EndpointID] coupled with its [EndpointData]).
//
// Records are published under the name "_iroh.<z32-endpoint-id>.<origin>" as TXT
// records of the form "key=value" (RFC 1464), with keys "relay", "addr", and
// "user-data". [EndpointInfo] converts to and from both TXT record sets and
// pkarr signed packets.
//
// The Go API is not stable before v1 and may change in any v0 release.
package dns
