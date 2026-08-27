package dns

import (
	"bytes"

	"github.com/tmc/go-iroh/internal/pkarr"
	"github.com/tmc/go-iroh/key"
)

// MaxSignedPacketSize is the maximum wire size of a signed packet, including
// its public key.
const MaxSignedPacketSize = pkarr.MaxBytes

// SignedPacket is a signed DNS packet in the pkarr format used for iroh
// endpoint discovery.
//
// A SignedPacket is immutable. Accessors return copies where mutation would
// otherwise affect the packet.
type SignedPacket struct {
	packet *pkarr.SignedPacket
}

// SignedPacketFromBytes parses and verifies a signed packet from its wire
// representation.
func SignedPacketFromBytes(b []byte) (*SignedPacket, error) {
	packet, err := pkarr.FromBytes(b)
	if err != nil {
		return nil, err
	}
	return &SignedPacket{packet: packet}, nil
}

// SignedPacketFromBytesUnchecked parses a signed packet without verifying its
// signature. It still validates the minimum length and DNS packet encoding.
func SignedPacketFromBytesUnchecked(b []byte) (*SignedPacket, error) {
	packet, err := pkarr.FromBytesUnchecked(b)
	if err != nil {
		return nil, err
	}
	return &SignedPacket{packet: packet}, nil
}

func signedPacketFromInternal(packet *pkarr.SignedPacket) *SignedPacket {
	return &SignedPacket{packet: packet}
}

// Bytes returns the full serialized wire bytes.
func (p *SignedPacket) Bytes() []byte {
	if p == nil || p.packet == nil {
		return nil
	}
	return bytes.Clone(p.packet.Bytes())
}

// RelayPayload returns the relay payload: everything after the public key.
func (p *SignedPacket) RelayPayload() []byte {
	if p == nil || p.packet == nil {
		return nil
	}
	return p.packet.RelayPayload()
}

// PublicKey returns the signer's public key.
func (p *SignedPacket) PublicKey() key.PublicKey {
	if p == nil || p.packet == nil {
		return key.PublicKey{}
	}
	return p.packet.PublicKey()
}

// TimestampMicros returns the packet timestamp in microseconds since the UNIX
// epoch.
func (p *SignedPacket) TimestampMicros() uint64 {
	if p == nil || p.packet == nil {
		return 0
	}
	return uint64(p.packet.Timestamp())
}

// MoreRecentThan reports whether p should replace other. Equal timestamps are
// ordered by their encoded DNS packets, matching pkarr's conflict rule.
func (p *SignedPacket) MoreRecentThan(other *SignedPacket) bool {
	if p == nil || p.packet == nil {
		return false
	}
	if other == nil || other.packet == nil {
		return true
	}
	return p.packet.MoreRecentThan(other.packet)
}

// TXTRecords returns the TXT string values under name, normalized relative to
// the signer's z-base-32 public key.
func (p *SignedPacket) TXTRecords(name string) []string {
	if p == nil || p.packet == nil {
		return nil
	}
	return p.packet.TxtRecords(name)
}
