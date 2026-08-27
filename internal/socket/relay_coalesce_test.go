package socket

import (
	"bytes"
	"testing"

	"github.com/tmc/go-iroh/internal/relayproto"
	"github.com/tmc/go-iroh/key"
)

func TestCoalesceDatagrams(t *testing.T) {
	a := key.EndpointID{}
	sk, _ := key.GenerateSecretKey()
	b := sk.Public().EndpointID()
	dg := func(s string) relayproto.Datagrams { return relayproto.Datagrams{Contents: []byte(s)} }
	items := []RelaySendItem{
		{RemoteEndpoint: a, Datagrams: dg("1111")},
		{RemoteEndpoint: a, Datagrams: dg("2222")},
		{RemoteEndpoint: a, Datagrams: dg("33")},                                                              // short tail ends the run
		{RemoteEndpoint: a, Datagrams: dg("4444")},                                                            // new run
		{RemoteEndpoint: b, Datagrams: dg("5555")},                                                            // different destination
		{RemoteEndpoint: b, Datagrams: relayproto.Datagrams{Ecn: relayproto.EcnCe, Contents: []byte("6666")}}, // different ECN
		{RemoteEndpoint: b, Datagrams: relayproto.Datagrams{SegmentSize: 2, Contents: []byte("7788")}},        // already a batch
	}
	var got []relayproto.ClientToRelayMsg
	collect := func(m relayproto.ClientToRelayMsg) error {
		m.Datagrams.Contents = bytes.Clone(m.Datagrams.Contents)
		got = append(got, m)
		return nil
	}
	if err := coalesceDatagrams(items, 64, nil, collect); err != nil {
		t.Fatal(err)
	}
	want := []struct {
		dst  key.EndpointID
		seg  uint16
		ecn  relayproto.EcnCodepoint
		body string
	}{
		{a, 4, 0, "1111222233"},
		{a, 0, 0, "4444"},
		{b, 0, 0, "5555"},
		{b, 0, relayproto.EcnCe, "6666"},
		{b, 2, 0, "7788"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d msgs, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		if g.DstEndpointID != w.dst || g.Datagrams.SegmentSize != w.seg || g.Datagrams.Ecn != w.ecn || !bytes.Equal(g.Datagrams.Contents, []byte(w.body)) {
			t.Errorf("msg %d = {seg=%d ecn=%v %q}, want %+v", i, g.Datagrams.SegmentSize, g.Datagrams.Ecn, g.Datagrams.Contents, w)
		}
	}

	got = nil
	if err := coalesceDatagrams(items[:2], 7, nil, collect); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("maxSize: got %d msgs, want 2", len(got))
	}
}

func splitSegments(d relayproto.Datagrams) [][]byte {
	if d.SegmentSize == 0 || len(d.Contents) == 0 {
		return [][]byte{d.Contents}
	}
	stride := int(d.SegmentSize)
	var out [][]byte
	for off := 0; off < len(d.Contents); off += stride {
		end := off + stride
		if end > len(d.Contents) {
			end = len(d.Contents)
		}
		out = append(out, d.Contents[off:end])
	}
	return out
}
