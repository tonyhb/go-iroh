package relayproto

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/tmc/go-iroh/key"
)

func FuzzParseRelayFrames(f *testing.F) {
	secret := fuzzSecretKey()
	id := secret.Public().EndpointID()
	challenge := ServerChallenge{Challenge: [16]byte{1, 2, 3}}
	for _, data := range [][]byte{
		(RelayToClientMsg{Type: FramePing, Ping: ping42()}).AppendTo(nil),
		(RelayToClientMsg{Type: FrameStatus, Status: StatusHealthy}).AppendTo(nil),
		(RelayToClientMsg{Type: FrameRestarting, ReconnectIn: time.Millisecond, TryFor: 2 * time.Millisecond}).AppendTo(nil),
		(RelayToClientMsg{Type: FrameRelayToClientDatagram, RemoteEndpointID: id, Datagrams: Datagrams{Contents: []byte("hello")}}).AppendTo(nil),
		(ClientToRelayMsg{Type: FramePong, Ping: ping42()}).AppendTo(nil),
		(ClientToRelayMsg{Type: FrameClientToRelayDatagram, DstEndpointID: id, Datagrams: Datagrams{Contents: []byte("hello")}}).AppendTo(nil),
		challenge.AppendTo(nil),
		NewClientAuth(secret, challenge).AppendTo(nil),
		(ServerConfirmsAuth{}).AppendTo(nil),
		(ServerDeniesAuth{Reason: "no"}).AppendTo(nil),
		{},
	} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParseRelayToClientMsg(data, ProtocolV1)
		_, _ = ParseRelayToClientMsg(data, ProtocolV2)
		_, _ = ParseClientToRelayMsg(data)
		_, _ = ParseHandshakeFrame(data)
	})
}

func FuzzKeyMaterialClientAuthHeader(f *testing.F) {
	secret := fuzzSecretKey()
	auth := KeyMaterialClientAuth{
		PublicKey:         secret.Public(),
		Signature:         key.NewSignature([key.SignatureSize]byte{1}),
		KeyMaterialSuffix: [16]byte{2},
	}
	value, err := auth.HeaderValue()
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte(value))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		checkKeyMaterialClientAuthHeader(t, string(data))
		checkKeyMaterialClientAuthHeader(t, base64.RawURLEncoding.EncodeToString(data))
	})
}

func checkKeyMaterialClientAuthHeader(t *testing.T, value string) {
	t.Helper()
	auth, err := KeyMaterialClientAuthFromHeader(value)
	if err != nil {
		return
	}
	encoded, err := auth.HeaderValue()
	if err != nil {
		t.Fatalf("re-encode auth header: %v", err)
	}
	reparsed, err := KeyMaterialClientAuthFromHeader(encoded)
	if err != nil {
		t.Fatalf("reparse auth header: %v", err)
	}
	if !reparsed.PublicKey.Equal(auth.PublicKey) ||
		reparsed.Signature != auth.Signature ||
		reparsed.KeyMaterialSuffix != auth.KeyMaterialSuffix {
		t.Fatal("auth header changed after reparse")
	}
}

func fuzzSecretKey() key.SecretKey {
	var seed [32]byte
	for i := range seed {
		seed[i] = 42
	}
	return key.NewSecretKey(seed)
}
