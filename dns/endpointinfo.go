package dns

import (
	"errors"
	"net/netip"
	"slices"
	"strings"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// IrohTXTName is the DNS record name under which iroh TXT records are published.
const IrohTXTName = "_iroh"

// UserDataMaxLength is the maximum byte length of [UserData]. A DNS TXT
// character-string holds at most 255 bytes; subtracting the "user-data=" prefix
// leaves 245 bytes.
const UserDataMaxLength = 245

// ErrUserDataTooLong is returned when a [UserData] value exceeds
// [UserDataMaxLength].
var ErrUserDataTooLong = errors.New("user data: max length exceeded")

// UserData is application-defined data published and resolved through endpoint
// discovery. It is a UTF-8 string of at most [UserDataMaxLength] bytes. iroh
// neither inspects nor uses it.
type UserData struct {
	s string
}

// NewUserData returns a UserData wrapping s, or [ErrUserDataTooLong] if s
// exceeds [UserDataMaxLength] bytes.
func NewUserData(s string) (UserData, error) {
	if len(s) > UserDataMaxLength {
		return UserData{}, ErrUserDataTooLong
	}
	return UserData{s: s}, nil
}

// String returns the user data string.
func (u UserData) String() string { return u.s }

// MarshalText implements encoding.TextMarshaler.
func (u UserData) MarshalText() ([]byte, error) {
	return []byte(u.s), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (u *UserData) UnmarshalText(text []byte) error {
	parsed, err := NewUserData(string(text))
	if err != nil {
		return err
	}
	*u = parsed
	return nil
}

// EndpointData is the addressing and metadata published about an endpoint: a set
// of transport addresses (in priority order) and optional [UserData]. It does
// not include the endpoint's id; see [EndpointInfo].
//
// The zero value is an empty EndpointData and is ready to use.
type EndpointData struct {
	// addrs is the ordered, de-duplicated set of transport addresses. Order is
	// preserved (it encodes priority) while duplicates are removed.
	addrs    []netaddr.TransportAddr
	userData *UserData
}

// NewEndpointData returns an EndpointData with the given addresses. Order is
// preserved; duplicates are removed.
func NewEndpointData(addrs ...netaddr.TransportAddr) EndpointData {
	d := EndpointData{}
	d.AddAddrs(addrs...)
	return d
}

// WithRelayURL returns a copy of d with u added to the end of the address list,
// unless already present.
func (d EndpointData) WithRelayURL(u netaddr.RelayURL) EndpointData {
	return d.WithAddrs(netaddr.RelayAddr{URL: u})
}

// AddRelayURL adds a relay URL to the end of the address list, unless already
// present.
func (d *EndpointData) AddRelayURL(u netaddr.RelayURL) {
	d.AddAddrs(netaddr.RelayAddr{URL: u})
}

// WithIPAddrs returns a copy of d with addrs added in order, skipping
// duplicates and existing ones.
func (d EndpointData) WithIPAddrs(addrs ...netip.AddrPort) EndpointData {
	d = d.clone()
	d.AddIPAddrs(addrs...)
	return d
}

// AddIPAddrs adds IP addresses in order, skipping duplicates and existing ones.
func (d *EndpointData) AddIPAddrs(addrs ...netip.AddrPort) {
	conv := make([]netaddr.TransportAddr, len(addrs))
	for i, a := range addrs {
		conv[i] = netaddr.IPAddr{Addr: a}
	}
	d.AddAddrs(conv...)
}

// WithAddrs returns a copy of d with addrs added in order, skipping duplicates
// and existing ones.
func (d EndpointData) WithAddrs(addrs ...netaddr.TransportAddr) EndpointData {
	d = d.clone()
	d.AddAddrs(addrs...)
	return d
}

// AddAddrs adds addresses in order, skipping duplicates and ones already
// present. Duplicate filtering preserves the existing order.
func (d *EndpointData) AddAddrs(addrs ...netaddr.TransportAddr) {
	for _, a := range addrs {
		if !d.contains(a) {
			d.addrs = append(d.addrs, a)
		}
	}
}

// WithUserData returns a copy of d with the user data set or cleared.
func (d EndpointData) WithUserData(u *UserData) EndpointData {
	d = d.clone()
	d.userData = u
	return d
}

// SetUserData sets or clears the user data.
func (d *EndpointData) SetUserData(u *UserData) { d.userData = u }

// WithoutIPAddrs returns a copy of d with all direct IP addresses removed.
func (d EndpointData) WithoutIPAddrs() EndpointData {
	d = d.clone()
	d.ClearIPAddrs()
	return d
}

// ClearIPAddrs removes all direct IP addresses.
func (d *EndpointData) ClearIPAddrs() {
	d.addrs = slices.DeleteFunc(d.addrs, func(a netaddr.TransportAddr) bool {
		_, ok := a.(netaddr.IPAddr)
		return ok
	})
}

// WithoutRelayURLs returns a copy of d with all relay addresses removed.
func (d EndpointData) WithoutRelayURLs() EndpointData {
	d = d.clone()
	d.ClearRelayURLs()
	return d
}

// ClearRelayURLs removes all relay addresses.
func (d *EndpointData) ClearRelayURLs() {
	d.addrs = slices.DeleteFunc(d.addrs, func(a netaddr.TransportAddr) bool {
		_, ok := a.(netaddr.RelayAddr)
		return ok
	})
}

// Addrs returns the ordered transport addresses.
func (d EndpointData) Addrs() []netaddr.TransportAddr { return slices.Clone(d.addrs) }

// RelayURLs returns the relay URLs in order.
func (d EndpointData) RelayURLs() []netaddr.RelayURL {
	var out []netaddr.RelayURL
	for _, a := range d.addrs {
		if r, ok := a.(netaddr.RelayAddr); ok {
			out = append(out, r.URL)
		}
	}
	return out
}

// IPAddrs returns the direct IP addresses in order.
func (d EndpointData) IPAddrs() []netip.AddrPort {
	var out []netip.AddrPort
	for _, a := range d.addrs {
		if ip, ok := a.(netaddr.IPAddr); ok {
			out = append(out, ip.Addr)
		}
	}
	return out
}

// UserData returns the user data, or nil if unset.
func (d EndpointData) UserData() *UserData { return d.userData }

// HasAddrs reports whether any addresses are present.
func (d EndpointData) HasAddrs() bool { return len(d.addrs) > 0 }

// String returns a diagnostic string for d.
func (d EndpointData) String() string {
	var b strings.Builder
	b.WriteString("EndpointData{addrs:[")
	for i, a := range d.addrs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(a.String())
	}
	b.WriteString("]")
	if d.userData != nil {
		b.WriteString(", user-data:")
		b.WriteString(d.userData.String())
	}
	b.WriteString("}")
	return b.String()
}

func (d EndpointData) contains(a netaddr.TransportAddr) bool {
	return slices.ContainsFunc(d.addrs, func(x netaddr.TransportAddr) bool {
		return x.Compare(a) == 0
	})
}

func (d EndpointData) clone() EndpointData {
	d.addrs = slices.Clone(d.addrs)
	return d
}

// EndpointDataFromAddr builds EndpointData from an [netaddr.EndpointAddr], taking
// its (already de-duplicated, sorted) addresses.
func EndpointDataFromAddr(addr netaddr.EndpointAddr) EndpointData {
	return EndpointData{addrs: addr.Addrs()}
}

// EndpointInfo couples an [key.EndpointID] with the [EndpointData] published
// about it.
type EndpointInfo struct {
	// ID is the endpoint this information is about.
	ID key.EndpointID
	// Data is the addressing and metadata.
	Data EndpointData
}

// String returns a diagnostic string for e.
func (e EndpointInfo) String() string {
	return "EndpointInfo{id:" + e.ID.String() + ", data:" + e.Data.String() + "}"
}

// EndpointInfoFromAddr converts an [netaddr.EndpointAddr] into an EndpointInfo.
func EndpointInfoFromAddr(addr netaddr.EndpointAddr) EndpointInfo {
	return EndpointInfo{ID: addr.ID, Data: EndpointDataFromAddr(addr)}
}

// Addr converts the info into an [netaddr.EndpointAddr].
func (e EndpointInfo) Addr() netaddr.EndpointAddr {
	return netaddr.NewEndpointAddr(e.ID, e.Data.addrs...)
}

// ToTXTStrings renders the endpoint info as "key=value" TXT record strings.
func (e EndpointInfo) ToTXTStrings() []string {
	return e.toAttrs().toTxtStrings()
}

// ToSignedPacket builds a signed packet for this endpoint info, signed with
// secretKey and using the given record TTL in seconds.
func (e EndpointInfo) ToSignedPacket(secretKey key.SecretKey, ttl uint32) (*SignedPacket, error) {
	packet, err := e.toAttrs().toPkarrSignedPacket(secretKey, ttl)
	if err != nil {
		return nil, err
	}
	return signedPacketFromInternal(packet), nil
}

// EndpointInfoFromTXTLookup parses an EndpointInfo from DNS TXT lookup results.
// domainName is the queried name ("_iroh.<z32>.<origin>") and values are the
// TXT record string values.
func EndpointInfoFromTXTLookup(domainName string, values []string) (EndpointInfo, error) {
	attrs, err := txtAttrsFromTXTLookup(domainName, values)
	if err != nil {
		return EndpointInfo{}, err
	}
	return endpointInfoFromAttrs(attrs), nil
}

// EndpointInfoFromSignedPacket parses an EndpointInfo from a signed packet.
func EndpointInfoFromSignedPacket(packet *SignedPacket) (EndpointInfo, error) {
	attrs, err := txtAttrsFromPkarrSignedPacket(packet)
	if err != nil {
		return EndpointInfo{}, err
	}
	return endpointInfoFromAttrs(attrs), nil
}
