// Package socket implements iroh's "magic socket": a single net.PacketConn,
// driven by quic-go, that multiplexes datagrams across several transports
// (direct UDP, relay, custom). Because quic-go addresses paths with net.Addr,
// each non-IP path is represented by a synthetic IPv6 Unique Local Address (RFC
// 4193) from a private range. These mapped addresses are an internal indirection
// only — they are never sent on the wire — but the byte scheme matches the Rust
// implementation (iroh/src/socket/mapped_addrs.rs) for cross-referencing.
package socket

import (
	"encoding/binary"
	"net/netip"
	"sync"
	"sync/atomic"
)

// The mapped-address scheme. See iroh/src/socket/mapped_addrs.rs.
const (
	// addrPrefixL is the first byte of every Unique Local Address (RFC 4193).
	addrPrefixL = 0xfd
	// mappedPort is the fixed dummy port for all mapped socket addresses; the
	// port plays no role in addressing.
	mappedPort = 12345
)

// addrGlobalID is n0's 40-bit ULA global id (bytes 1..6).
var addrGlobalID = [5]byte{0x15, 0x07, 0x0a, 0x51, 0x0b}

// Subnet ids (bytes 6..8) distinguish the mapped-address kinds.
var (
	subnetEndpointID = [2]byte{0x00, 0x00} // fd15:70a:510b::/64
	subnetRelay      = [2]byte{0x00, 0x01} // fd15:70a:510b:1::/64
	subnetCustom     = [2]byte{0x00, 0x03} // fd15:70a:510b:3::/64
)

// Per-kind counters; the low 8 bytes of each mapped address. They start at 1,
// matching the Rust AtomicU64::new(1) (the first Add(1) yields 1).
var (
	endpointIDCounter atomic.Uint64
	relayCounter      atomic.Uint64
	customCounter     atomic.Uint64
)

// mappedAddr builds a mapped IPv6 address for the given subnet and counter.
func mappedAddr(subnet [2]byte, counter uint64) netip.Addr {
	var a [16]byte
	a[0] = addrPrefixL
	copy(a[1:6], addrGlobalID[:])
	copy(a[6:8], subnet[:])
	binary.BigEndian.PutUint64(a[8:16], counter)
	return netip.AddrFrom16(a)
}

// hasMappedPrefix reports whether addr is in n0's mapped ULA range with the
// given subnet id.
func hasMappedPrefix(addr netip.Addr, subnet [2]byte) bool {
	if !addr.Is6() || addr.Is4In6() {
		return false
	}
	o := addr.As16()
	return o[0] == addrPrefixL &&
		o[1] == addrGlobalID[0] && o[2] == addrGlobalID[1] && o[3] == addrGlobalID[2] &&
		o[4] == addrGlobalID[3] && o[5] == addrGlobalID[4] &&
		o[6] == subnet[0] && o[7] == subnet[1]
}

// EndpointIDMappedAddr addresses a remote endpoint via any/all of its paths. It
// is used for the initial connection, before a path is selected: the socket
// duplicates datagrams sent here onto every candidate path.
type EndpointIDMappedAddr struct{ a netip.Addr }

// RelayMappedAddr addresses a remote endpoint via a specific relay path
// (an (EndpointID, RelayURL) pair).
type RelayMappedAddr struct{ a netip.Addr }

// CustomMappedAddr addresses a remote endpoint via a custom transport path.
type CustomMappedAddr struct{ a netip.Addr }

// NewEndpointIDMappedAddr allocates a fresh endpoint-id mapped address.
func NewEndpointIDMappedAddr() EndpointIDMappedAddr {
	return EndpointIDMappedAddr{mappedAddr(subnetEndpointID, endpointIDCounter.Add(1))}
}

// NewRelayMappedAddr allocates a fresh relay mapped address.
func NewRelayMappedAddr() RelayMappedAddr {
	return RelayMappedAddr{mappedAddr(subnetRelay, relayCounter.Add(1))}
}

// RelayMappedAddrFromAddr wraps an existing relay mapped IPv6 address. It is
// used to reverse-look-up the (relay, endpoint) pair an address maps to via
// [Socket.LookupRelay]; it does not allocate a new mapping.
func RelayMappedAddrFromAddr(a netip.Addr) RelayMappedAddr { return RelayMappedAddr{a: a} }

// NewCustomMappedAddr allocates a fresh custom mapped address.
func NewCustomMappedAddr() CustomMappedAddr {
	return CustomMappedAddr{mappedAddr(subnetCustom, customCounter.Add(1))}
}

// Addr returns the underlying IPv6 address.
func (m EndpointIDMappedAddr) Addr() netip.Addr { return m.a }

// EndpointIDMappedAddrFromAddr wraps an existing endpoint-id mapped IPv6
// address. It is used to reverse-look-up the endpoint id via
// [Socket.LookupEndpointID].
func EndpointIDMappedAddrFromAddr(a netip.Addr) EndpointIDMappedAddr {
	return EndpointIDMappedAddr{a: a}
}

// Addr returns the underlying IPv6 address.
func (m RelayMappedAddr) Addr() netip.Addr { return m.a }

// Addr returns the underlying IPv6 address.
func (m CustomMappedAddr) Addr() netip.Addr { return m.a }

// CustomMappedAddrFromAddr wraps an existing custom mapped IPv6 address. It is
// used to reverse-look-up the custom address via [Socket.LookupCustom].
func CustomMappedAddrFromAddr(a netip.Addr) CustomMappedAddr { return CustomMappedAddr{a: a} }

// AddrPort returns the mapped address with the fixed dummy port, suitable for
// handing to quic-go as a path's net.Addr.
func (m EndpointIDMappedAddr) AddrPort() netip.AddrPort { return mappedAddrPort(m.a) }

// AddrPort returns the mapped address with the fixed dummy port.
func (m RelayMappedAddr) AddrPort() netip.AddrPort { return mappedAddrPort(m.a) }

// AddrPort returns the mapped address with the fixed dummy port.
func (m CustomMappedAddr) AddrPort() netip.AddrPort { return mappedAddrPort(m.a) }

func mappedAddrPort(a netip.Addr) netip.AddrPort {
	return netip.AddrPortFrom(a, mappedPort)
}

// MappedKind classifies a netip.Addr as one of the mapped kinds or a real IP.
type MappedKind int

const (
	// KindIP is a real (non-mapped) IP address.
	KindIP MappedKind = iota
	// KindEndpointID is an EndpointIDMappedAddr.
	KindEndpointID
	// KindRelay is a RelayMappedAddr.
	KindRelay
	// KindCustom is a CustomMappedAddr.
	KindCustom
)

// Classify reports which mapped kind addr belongs to, or KindIP if it is a real
// address. The order matches the Rust MultipathMappedAddr::from conversion.
func Classify(addr netip.Addr) MappedKind {
	switch {
	case hasMappedPrefix(addr, subnetEndpointID):
		return KindEndpointID
	case hasMappedPrefix(addr, subnetRelay):
		return KindRelay
	case hasMappedPrefix(addr, subnetCustom):
		return KindCustom
	default:
		return KindIP
	}
}

// AddrMap is a bidirectional map between a key K and a mapped address of value
// type V, generating a new mapped address on first lookup of a key. It is the
// Go analog of the Rust AddrMap.
type AddrMap[K comparable, V comparable] struct {
	gen    func() V
	addrOf func(V) netip.Addr

	mu  sync.Mutex
	fwd map[K]V
	rev map[netip.Addr]K
}

// NewAddrMap returns an AddrMap whose missing keys are filled with gen(), keyed
// in reverse by addrOf(value).
func NewAddrMap[K comparable, V comparable](gen func() V, addrOf func(V) netip.Addr) *AddrMap[K, V] {
	return &AddrMap[K, V]{
		gen:    gen,
		addrOf: addrOf,
		fwd:    make(map[K]V),
		rev:    make(map[netip.Addr]K),
	}
}

// Get returns the mapped address for key, generating and recording one if it
// does not yet exist.
func (m *AddrMap[K, V]) Get(key K) V {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.fwd[key]; ok {
		return v
	}
	v := m.gen()
	m.fwd[key] = v
	m.rev[m.addrOf(v)] = key
	return v
}

// Lookup returns the key that maps to addr, if any.
func (m *AddrMap[K, V]) Lookup(addr netip.Addr) (K, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k, ok := m.rev[addr]
	return k, ok
}

// Remove deletes the mapping for key, if any. The next Get of the same key
// generates a fresh mapped address.
func (m *AddrMap[K, V]) Remove(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.fwd[key]
	if !ok {
		return
	}
	delete(m.fwd, key)
	delete(m.rev, m.addrOf(v))
}

// Len returns the number of mappings. Intended for tests and metrics.
func (m *AddrMap[K, V]) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.fwd)
}
