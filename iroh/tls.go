package iroh

import (
	"crypto/ed25519"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"

	tls "github.com/tmc/go-iroh/internal/itls/tls"
	"github.com/tmc/go-iroh/key"
)

// alpn values and TLS parameters shared by iroh peers. iroh authenticates peers
// with TLS 1.3 raw public keys (RFC 7250): each endpoint presents its ed25519
// public key as a bare SubjectPublicKeyInfo, and verifies the peer's key rather
// than any X.509 chain. See [github.com/tmc/go-iroh/internal/itls/tls] for the
// raw-public-key TLS implementation.

// base32DNSSEC is data_encoding::BASE32_DNSSEC: RFC 4648 base32hex, lowercase,
// unpadded. iroh encodes an EndpointID into the TLS server name with it (see
// [ServerName]); it differs from the z-base-32 used for human-facing key
// strings.
var base32DNSSEC = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv").WithPadding(base32.NoPadding)

// tlsNameSuffix is appended to the base32 endpoint id to form the TLS server
// name. The .invalid TLD (RFC 2606) never resolves; the iroh label is a
// protocol marker.
const tlsNameSuffix = ".iroh.invalid"

// KeyExchangePolicy selects the TLS key-exchange groups offered by an
// endpoint. The zero value uses the package default.
type KeyExchangePolicy uint8

const (
	// KeyExchangeDefault uses the package default key-exchange groups.
	KeyExchangeDefault KeyExchangePolicy = iota
	// KeyExchangeClassical disables post-quantum key exchange.
	KeyExchangeClassical
	// KeyExchangePreferPQ prefers X25519MLKEM768 and retains classical fallback.
	KeyExchangePreferPQ
	// KeyExchangePQOnly requires X25519MLKEM768.
	KeyExchangePQOnly
)

func (p KeyExchangePolicy) valid() bool { return p <= KeyExchangePQOnly }

func (p KeyExchangePolicy) curves() []tls.CurveID {
	switch p {
	case KeyExchangeClassical:
		return []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384, tls.CurveP521}
	case KeyExchangePreferPQ:
		return []tls.CurveID{tls.X25519MLKEM768, tls.X25519, tls.CurveP256, tls.CurveP384}
	case KeyExchangePQOnly:
		return []tls.CurveID{tls.X25519MLKEM768}
	default:
		return nil
	}
}

// ServerName returns the TLS server name (SNI) iroh uses to address id:
// BASE32_DNSSEC(id) + ".iroh.invalid". A dialing endpoint puts this in its
// ClientHello; the accepting endpoint proves it holds id by presenting id as
// its raw public key. Deriving the name from the id (rather than a constant)
// also keeps per-peer 0-RTT session tickets in separate cache buckets.
func ServerName(id key.EndpointID) string {
	b := id.Bytes()
	return base32DNSSEC.EncodeToString(b[:]) + tlsNameSuffix
}

// endpointIDFromServerName is the inverse of [ServerName]. It reports whether
// name is a well-formed iroh server name and, if so, the encoded endpoint id.
func endpointIDFromServerName(name string) (key.EndpointID, bool) {
	rest, ok := strings.CutSuffix(name, tlsNameSuffix)
	if !ok || strings.Contains(rest, ".") {
		return key.EndpointID{}, false
	}
	raw, err := base32DNSSEC.DecodeString(rest)
	if err != nil || len(raw) != key.PublicKeySize {
		return key.EndpointID{}, false
	}
	id, err := key.EndpointIDFromSlice(raw)
	if err != nil {
		return key.EndpointID{}, false
	}
	return id, true
}

// rawKeyCertificate builds the RFC 7250 certificate for sk: the leaf is sk's
// ed25519 public key as a SubjectPublicKeyInfo, signed under sk during the
// handshake.
func rawKeyCertificate(sk key.SecretKey) (tls.Certificate, error) {
	priv := sk.Ed25519()
	pub := priv.Public().(ed25519.PublicKey)
	return tls.MarshalRawPublicKeyCertificate(pub, priv)
}

// peerEndpointID extracts the peer's endpoint id from a completed raw-public-key
// TLS handshake. It is the public key carried in the single peer certificate.
func peerEndpointID(cs tls.ConnectionState) (key.EndpointID, error) {
	if len(cs.PeerCertificates) != 1 {
		return key.EndpointID{}, fmt.Errorf("iroh: expected exactly one peer certificate, got %d", len(cs.PeerCertificates))
	}
	pub, ok := cs.PeerCertificates[0].PublicKey.(ed25519.PublicKey)
	if !ok {
		return key.EndpointID{}, errors.New("iroh: peer key is not ed25519")
	}
	id, err := key.EndpointIDFromSlice(pub)
	if err != nil {
		return key.EndpointID{}, fmt.Errorf("iroh: peer key: %w", err)
	}
	return id, nil
}

// clientTLSConfig builds the TLS configuration a dialing endpoint uses to
// connect to want. It presents sk as a raw public key and verifies that the
// peer's key equals want — the same check iroh's ServerCertificateVerifier
// performs by decoding the dialed server name (RFC 7250 server auth).
//
// Session tickets are enabled and stored in cache so a repeat dial to want can
// resume with 0-RTT early data. iroh derives a unique [ServerName] per peer, so
// the cache keys tickets by identity automatically. cache may be nil to opt out
// of resumption (the connection then always performs a full handshake). This
// mirrors the Rust client config, which enables early data and stores tickets
// in a ClientSessionMemoryCache (iroh/src/tls.rs:86-87).
func clientTLSConfig(sk key.SecretKey, want key.EndpointID, alpns []string, cache tls.ClientSessionCache) (*tls.Config, error) {
	return clientTLSConfigWithCurves(sk, want, alpns, cache, nil)
}

func clientTLSConfigWithCurves(sk key.SecretKey, want key.EndpointID, alpns []string, cache tls.ClientSessionCache, curves []tls.CurveID) (*tls.Config, error) {
	cert, err := rawKeyCertificate(sk)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates:           []tls.Certificate{cert},
		RawPublicKeys:          true,
		MinVersion:             tls.VersionTLS13,
		MaxVersion:             tls.VersionTLS13,
		SessionTicketsDisabled: cache == nil,
		ClientSessionCache:     cache,
		CurvePreferences:       curves,
		NextProtos:             alpns,
		ServerName:             ServerName(want),
		InsecureSkipVerify:     true, // chain verification is replaced by VerifyConnection
		VerifyConnection: func(cs tls.ConnectionState) error {
			got, err := peerEndpointID(cs)
			if err != nil {
				return err
			}
			if !got.Equal(want) {
				return fmt.Errorf("iroh: server identity mismatch: dialed %s, got %s", want, got)
			}
			return nil
		},
	}, nil
}

// serverTLSConfig builds the TLS configuration an accepting endpoint uses. It
// presents sk as a raw public key and requires the client to do the same; the
// client's identity is learned from its certificate after the handshake (iroh's
// server does not check the client key against anything, mirroring
// ClientCertificateVerifier).
//
// Session tickets are enabled so the server issues a NewSessionTicket the client
// can later resume with for 0-RTT. The QUIC layer advertises max_early_data_size
// = u32::MAX on those tickets when 0-RTT acceptance is enabled (RFC 9001 §4.6.1,
// iroh/src/tls.rs:118); the iroh server opts in via [quic.Config.Allow0RTT].
func serverTLSConfig(sk key.SecretKey, alpns []string) (*tls.Config, error) {
	return serverTLSConfigWithCurves(sk, alpns, nil)
}

func serverTLSConfigWithCurves(sk key.SecretKey, alpns []string, curves []tls.CurveID) (*tls.Config, error) {
	cert, err := rawKeyCertificate(sk)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates:           []tls.Certificate{cert},
		RawPublicKeys:          true,
		MinVersion:             tls.VersionTLS13,
		MaxVersion:             tls.VersionTLS13,
		SessionTicketsDisabled: false,
		CurvePreferences:       curves,
		NextProtos:             alpns,
		ClientAuth:             tls.RequireAnyClientCert,
		InsecureSkipVerify:     true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			// Authenticate that a client key is present and parseable; the
			// signature over the handshake transcript proves possession. The
			// concrete identity is surfaced to the application post-handshake.
			_, err := peerEndpointID(cs)
			return err
		},
	}, nil
}
