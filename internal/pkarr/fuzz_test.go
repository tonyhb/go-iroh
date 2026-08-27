package pkarr

import (
	"bytes"
	"testing"

	"github.com/tmc/go-iroh/key"
)

func FuzzFromBytes(f *testing.F) {
	valid := fuzzSignedPacket(f)
	f.Add(valid)
	f.Add([]byte{})
	f.Add(valid[:headerSize])
	f.Fuzz(func(t *testing.T, data []byte) {
		if packet, err := FromBytesUnchecked(data); err == nil {
			reparsed, err := FromBytesUnchecked(packet.Bytes())
			if err != nil {
				t.Fatalf("reparse unchecked packet: %v", err)
			}
			if !bytes.Equal(reparsed.Bytes(), packet.Bytes()) {
				t.Fatal("unchecked packet changed after reparse")
			}
		}

		packet, err := FromBytes(data)
		if err != nil {
			return
		}
		if err := packet.PublicKey().Verify(
			signable(packet.Timestamp().Micros(), packet.EncodedPacket()),
			packet.Signature(),
		); err != nil {
			t.Fatalf("accepted packet has invalid signature: %v", err)
		}
		if _, err := FromBytes(packet.Bytes()); err != nil {
			t.Fatalf("reparse verified packet: %v", err)
		}
	})
}

func fuzzSignedPacket(t testing.TB) []byte {
	t.Helper()
	secret := key.NewSecretKey([32]byte{1})
	encoded, err := buildTxtPacket(
		"_iroh."+secret.Public().EndpointID().Z32(),
		[]string{"relay=https://example.com/", "addr=127.0.0.1:1234"},
		30,
	)
	if err != nil {
		t.Fatal(err)
	}
	timestamp := TimestampFromMicros(1)
	signature := secret.Sign(signable(timestamp.Micros(), encoded))
	publicKey := secret.Public().Bytes()
	signatureBytes := signature.Bytes()
	b := make([]byte, 0, headerSize+len(encoded))
	b = append(b, publicKey[:]...)
	b = append(b, signatureBytes[:]...)
	b = append(b, timestamp.beBytes()...)
	return append(b, encoded...)
}
