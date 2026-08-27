// Package key provides Ed25519 keys, signatures, and endpoint identifiers for
// go-iroh.
//
// A [SecretKey] signs messages and derives a [PublicKey]. Use [PublicKey] for
// cryptographic operations and [EndpointID] for network-facing endpoint
// identities. Keys and endpoint IDs render as lowercase hex by default;
// [EndpointID.Z32] returns the z-base-32 form used in pkarr names.
//
// The Go API is not stable before v1 and may change in any v0 release.
package key
