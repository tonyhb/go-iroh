package dns

import (
	"net/netip"
	"slices"
	"testing"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func mustRelay(t *testing.T, s string) netaddr.RelayURL {
	t.Helper()
	u, err := netaddr.ParseRelayURL(s)
	if err != nil {
		t.Fatalf("ParseRelayURL(%q): %v", s, err)
	}
	return u
}

// TestTxtAttrRoundTrip mirrors iroh-dns txt_attr_roundtrip.
func TestTxtAttrRoundTrip(t *testing.T) {
	ud, err := NewUserData("foobar")
	if err != nil {
		t.Fatal(err)
	}
	data := NewEndpointData(
		netaddr.RelayAddr{URL: mustRelay(t, "https://example.com")},
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1234")},
	)
	data.SetUserData(&ud)
	id, err := key.ParseEndpointID("vpnk377obfvzlipnsfbqba7ywkkenc4xlpmovt5tsfujoa75zqia")
	if err != nil {
		t.Fatal(err)
	}
	want := EndpointInfo{ID: id, Data: data}
	attrs := want.toAttrs()
	got := endpointInfoFromAttrs(attrs)
	assertEndpointInfoEqual(t, got, want)
}

// TestTxtAttrRoundTripCustomAddr mirrors txt_attr_roundtrip_with_custom_addr.
func TestTxtAttrRoundTripCustomAddr(t *testing.T) {
	bt := netaddr.NewCustomAddr(1, []byte{0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6})
	tor := netaddr.NewCustomAddr(42, bytesRepeat(0xab, 32))
	data := NewEndpointData(
		netaddr.RelayAddr{URL: mustRelay(t, "https://example.com")},
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1234")},
		bt,
		tor,
	)
	id, err := key.ParseEndpointID("vpnk377obfvzlipnsfbqba7ywkkenc4xlpmovt5tsfujoa75zqia")
	if err != nil {
		t.Fatal(err)
	}
	want := EndpointInfo{ID: id, Data: data}
	got := endpointInfoFromAttrs(want.toAttrs())
	assertEndpointInfoEqual(t, got, want)
}

// TestSignedPacketRoundTrip mirrors signed_packet_roundtrip.
func TestSignedPacketRoundTrip(t *testing.T) {
	sk, err := key.ParseSecretKey("vpnk377obfvzlipnsfbqba7ywkkenc4xlpmovt5tsfujoa75zqia")
	if err != nil {
		t.Fatal(err)
	}
	ud, _ := NewUserData("foobar")
	data := NewEndpointData(
		netaddr.RelayAddr{URL: mustRelay(t, "https://example.com")},
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1234")},
	)
	data.SetUserData(&ud)
	want := EndpointInfo{ID: sk.Public().EndpointID(), Data: data}
	packet, err := want.ToSignedPacket(sk, 30)
	if err != nil {
		t.Fatalf("ToSignedPacket: %v", err)
	}
	got, err := EndpointInfoFromSignedPacket(packet)
	if err != nil {
		t.Fatalf("EndpointInfoFromSignedPacket: %v", err)
	}
	assertEndpointInfoEqual(t, got, want)
}

// TestSignedPacketRoundTripCustomAddr mirrors signed_packet_roundtrip_with_custom_addr.
func TestSignedPacketRoundTripCustomAddr(t *testing.T) {
	sk, err := key.ParseSecretKey("vpnk377obfvzlipnsfbqba7ywkkenc4xlpmovt5tsfujoa75zqia")
	if err != nil {
		t.Fatal(err)
	}
	bt := netaddr.NewCustomAddr(1, []byte{0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6})
	tor := netaddr.NewCustomAddr(42, bytesRepeat(0xab, 32))
	ud, _ := NewUserData("foobar")
	data := NewEndpointData(
		netaddr.RelayAddr{URL: mustRelay(t, "https://example.com")},
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1234")},
		bt, tor,
	)
	data.SetUserData(&ud)
	want := EndpointInfo{ID: sk.Public().EndpointID(), Data: data}
	packet, err := want.ToSignedPacket(sk, 30)
	if err != nil {
		t.Fatal(err)
	}
	got, err := EndpointInfoFromSignedPacket(packet)
	if err != nil {
		t.Fatal(err)
	}
	assertEndpointInfoEqual(t, got, want)
}

// TestFromTxtLookupMultiAddr mirrors test_from_hickory_lookup: more than one
// addr record must be parsed, and records with the wrong name are excluded.
func TestFromTxtLookupMultiAddr(t *testing.T) {
	id, err := key.ParseEndpointID("1992d53c02cdc04566e5c0edb1ce83305cd550297953a047a445ea3264b54b18")
	if err != nil {
		t.Fatal(err)
	}
	name := "_iroh." + id.Z32() + ".dns.iroh.link."
	values := []string{
		"addr=192.168.96.145:60165",
		"addr=213.208.157.87:60165",
		"relay=https://euw1-1.relay.iroh.network./",
	}
	got, err := EndpointInfoFromTXTLookup(name, values)
	if err != nil {
		t.Fatalf("EndpointInfoFromTXTLookup: %v", err)
	}
	want := EndpointInfo{ID: id, Data: NewEndpointData(
		netaddr.RelayAddr{URL: mustRelay(t, "https://euw1-1.relay.iroh.network./")},
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("192.168.96.145:60165")},
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("213.208.157.87:60165")},
	)}
	assertEndpointInfoEqual(t, got, want)
}

func TestTxtStringsOrder(t *testing.T) {
	// Reference BTreeMap order is relay, addr, user-data (enum order), not lexical.
	ud, _ := NewUserData("x")
	data := NewEndpointData(
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1")},
		netaddr.RelayAddr{URL: mustRelay(t, "https://r.example.com")},
	)
	data.SetUserData(&ud)
	info := EndpointInfo{ID: testID(t), Data: data}
	got := info.ToTXTStrings()
	// relay first, then addr, then user-data.
	if len(got) != 3 || got[0][:6] != "relay=" || got[1][:5] != "addr=" || got[2][:10] != "user-data=" {
		t.Errorf("ToTXTStrings order wrong: %v", got)
	}
}

func TestTxtStringsGolden(t *testing.T) {
	ud, _ := NewUserData("foobar")
	data := NewEndpointData(
		netaddr.RelayAddr{URL: mustRelay(t, "https://example.com/")},
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1234")},
		netaddr.NewCustomAddr(1, []byte{0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6}),
	)
	data.SetUserData(&ud)
	info := EndpointInfo{ID: testID(t), Data: data}
	got := info.ToTXTStrings()
	want := []string{
		"relay=https://example.com/",
		"addr=127.0.0.1:1234",
		"addr=1_a1b2c3d4e5f6",
		"user-data=foobar",
	}
	if !slices.Equal(got, want) {
		t.Errorf("ToTXTStrings = %v, want %v", got, want)
	}
}

func TestEndpointDataAddrsReturnsCopy(t *testing.T) {
	ip1 := netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1")}
	ip2 := netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:2")}
	data := NewEndpointData(ip1)
	addrs := data.Addrs()
	addrs[0] = ip2
	got := data.Addrs()
	if len(got) != 1 || got[0].Compare(ip1) != 0 {
		t.Fatalf("Addrs exposed internal slice: %v", got)
	}
}

func TestEndpointDataWithMethods(t *testing.T) {
	relay := mustRelay(t, "https://relay.example/")
	ip1 := netip.MustParseAddrPort("127.0.0.1:1")
	ip2 := netip.MustParseAddrPort("127.0.0.1:2")
	custom := netaddr.NewCustomAddr(7, []byte{0xde, 0xad})
	ud, _ := NewUserData("hello")

	base := NewEndpointData(netaddr.IPAddr{Addr: ip1})
	next := base.
		WithRelayURL(relay).
		WithIPAddrs(ip1, ip2).
		WithAddrs(custom).
		WithUserData(&ud)

	if got := strs(base.Addrs()); !slices.Equal(got, []string{"ip:127.0.0.1:1"}) {
		t.Fatalf("base changed: %v", got)
	}
	want := []string{
		"ip:127.0.0.1:1",
		"relay:https://relay.example/",
		"ip:127.0.0.1:2",
		"7_dead",
	}
	if got := strs(next.Addrs()); !slices.Equal(got, want) {
		t.Fatalf("With methods addrs = %v, want %v", got, want)
	}
	if next.UserData() == nil || next.UserData().String() != "hello" {
		t.Fatalf("UserData = %v, want hello", next.UserData())
	}

	withoutIP := next.WithoutIPAddrs()
	if got, want := strs(withoutIP.Addrs()), []string{"relay:https://relay.example/", "7_dead"}; !slices.Equal(got, want) {
		t.Fatalf("WithoutIPAddrs = %v, want %v", got, want)
	}
	withoutRelay := next.WithoutRelayURLs()
	if got, want := strs(withoutRelay.Addrs()), []string{"ip:127.0.0.1:1", "ip:127.0.0.1:2", "7_dead"}; !slices.Equal(got, want) {
		t.Fatalf("WithoutRelayURLs = %v, want %v", got, want)
	}
	cleared := next.WithUserData(nil)
	if cleared.UserData() != nil {
		t.Fatalf("WithUserData(nil) = %v, want nil", cleared.UserData())
	}
	if next.UserData() == nil {
		t.Fatal("WithUserData(nil) changed original")
	}
}

func TestEndpointDataString(t *testing.T) {
	if got, want := (EndpointData{}).String(), "EndpointData{addrs:[]}"; got != want {
		t.Fatalf("empty String = %q, want %q", got, want)
	}
	ud, _ := NewUserData("hello")
	data := NewEndpointData(
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1")},
		netaddr.RelayAddr{URL: mustRelay(t, "https://relay.example/")},
	)
	data.SetUserData(&ud)
	want := "EndpointData{addrs:[ip:127.0.0.1:1, relay:https://relay.example/], user-data:hello}"
	if got := data.String(); got != want {
		t.Fatalf("String = %q, want %q", got, want)
	}
}

func TestEndpointInfoString(t *testing.T) {
	info := EndpointInfo{
		ID: testID(t),
		Data: NewEndpointData(
			netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1")},
		),
	}
	want := "EndpointInfo{id:" + info.ID.String() + ", data:EndpointData{addrs:[ip:127.0.0.1:1]}}"
	if got := info.String(); got != want {
		t.Fatalf("String = %q, want %q", got, want)
	}
}

func TestTxtAttrsSplitLikeRust(t *testing.T) {
	id := testID(t)
	name := "_iroh." + id.Z32() + ".dns.iroh.link."
	got, err := EndpointInfoFromTXTLookup(name, []string{"user-data=a=b"})
	if err != nil {
		t.Fatalf("EndpointInfoFromTXTLookup: %v", err)
	}
	if got.Data.UserData() == nil || got.Data.UserData().String() != "a" {
		t.Fatalf("UserData = %v, want a", got.Data.UserData())
	}
}

func TestUserDataTooLong(t *testing.T) {
	long := make([]byte, UserDataMaxLength+1)
	if _, err := NewUserData(string(long)); err == nil {
		t.Error("expected error for over-length user data")
	}
	ok := make([]byte, UserDataMaxLength)
	if _, err := NewUserData(string(ok)); err != nil {
		t.Errorf("unexpected error for max-length user data: %v", err)
	}
}

func TestUserDataText(t *testing.T) {
	u, err := NewUserData("hello")
	if err != nil {
		t.Fatal(err)
	}
	text, err := u.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	if string(text) != "hello" {
		t.Fatalf("MarshalText = %q, want hello", text)
	}
	var got UserData
	if err := got.UnmarshalText(text); err != nil {
		t.Fatal(err)
	}
	if got.String() != u.String() {
		t.Fatalf("UnmarshalText = %q, want %q", got.String(), u.String())
	}

	long := make([]byte, UserDataMaxLength+1)
	if err := got.UnmarshalText(long); err == nil {
		t.Fatal("UnmarshalText accepted over-length user data")
	}
}

func testID(t *testing.T) key.EndpointID {
	t.Helper()
	id, err := key.ParseEndpointID("1992d53c02cdc04566e5c0edb1ce83305cd550297953a047a445ea3264b54b18")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func assertEndpointInfoEqual(t *testing.T, got, want EndpointInfo) {
	t.Helper()
	if !got.ID.Equal(want.ID) {
		t.Errorf("id mismatch: %s != %s", got.ID, want.ID)
	}
	gotAddrs, wantAddrs := got.Data.Addrs(), want.Data.Addrs()
	if len(gotAddrs) != len(wantAddrs) {
		t.Fatalf("addr count: %d != %d (%v vs %v)", len(gotAddrs), len(wantAddrs), strs(gotAddrs), strs(wantAddrs))
	}
	// Compare as sets (order can differ between stored and reparsed).
	gs, ws := strs(gotAddrs), strs(wantAddrs)
	slices.Sort(gs)
	slices.Sort(ws)
	if !slices.Equal(gs, ws) {
		t.Errorf("addrs mismatch: %v != %v", gs, ws)
	}
	gu, wu := got.Data.UserData(), want.Data.UserData()
	switch {
	case gu == nil && wu == nil:
	case gu == nil || wu == nil:
		t.Errorf("user data presence mismatch: %v != %v", gu, wu)
	case gu.String() != wu.String():
		t.Errorf("user data: %q != %q", gu.String(), wu.String())
	}
}

func strs(addrs []netaddr.TransportAddr) []string {
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a.String()
	}
	return out
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
