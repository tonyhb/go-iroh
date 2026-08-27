//go:build !js

// Package mdns provides local-network endpoint discovery for go-iroh.
//
// A Discovery advertises endpoint direct addresses on the local multicast DNS
// link and resolves peers advertised by other Discovery values. It implements
// iroh.AddressPublisher and iroh.AddressResolver, so it can be registered with
// iroh.AddressLookupServices.
//
// The Go API is not stable before v1 and may change in any v0 release.
package mdns
