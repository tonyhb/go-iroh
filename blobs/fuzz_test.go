package blobs

import (
	"net/netip"
	"testing"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

func FuzzDecodeTicketBytes(f *testing.F) {
	ticket := NewTicket(netaddr.NewEndpointAddr(testEndpointID(), netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1234")}), testHash(), Raw)
	f.Add(ticket.EncodeBytes())
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeBytes(data)
	})
}

func FuzzDecodeTicketString(f *testing.F) {
	ticket := NewTicket(netaddr.NewEndpointAddr(testEndpointID(), netaddr.IPAddr{Addr: netip.MustParseAddrPort("127.0.0.1:1234")}), testHash(), Raw)
	f.Add(ticket.EncodeString())
	f.Add(TicketKind)
	f.Add("blob!")
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = DecodeString(s)
	})
}

func FuzzDecodeRequestBytes(f *testing.F) {
	hash := testHash()
	for _, req := range []Request{
		{Type: RequestGet, Get: ptr(GetBlob(hash))},
		{Type: RequestObserve, Observe: ptr(ObserveBlob(hash))},
		{Type: RequestGetMany, GetMany: ptr(NewGetManyRequest([]Hash{hash}, ChunkRangesSeqAll()))},
	} {
		data, err := EncodeRequestBytes(req)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	f.Add([]byte{})
	f.Add([]byte{byte(RequestPush)})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeRequestBytes(data)
	})
}

func FuzzReadObserveItem(f *testing.F) {
	var valid []byte
	if err := writeObserveItem(bytesBuffer{&valid}, CompleteBitfield(1234)); err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add(impossibleObserveItem())
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = readObserveItem(newByteReader(data))
	})
}

func ptr[T any](v T) *T { return &v }

func testHash() Hash {
	var h Hash
	for i := range h {
		h[i] = byte(0xda + i)
	}
	return h
}

func testEndpointID() key.EndpointID {
	var seed [32]byte
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	return key.NewSecretKey(seed).Public().EndpointID()
}
