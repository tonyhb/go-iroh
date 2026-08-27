// Package netaddr provides endpoint and transport addresses for go-iroh.
//
// An [EndpointAddr] combines a key.EndpointID with the network-level
// [TransportAddr] values at which the endpoint may be reached. A transport
// address is a [RelayAddr], [IPAddr], or [CustomAddr]. A [RelayURL] identifies
// an iroh relay server.
//
// The Go API is not stable before v1 and may change in any v0 release.
package netaddr
