// Package pkarr exposes go-iroh's pkarr signed DNS packet codec.
//
// It is a thin re-export of the internal implementation so sibling modules can
// build compatible pkarr records without duplicating the wire codec.
//
// The Go API is not stable before v1 and may change in any v0 release.
package pkarr
