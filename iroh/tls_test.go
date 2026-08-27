package iroh

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"slices"
	"testing"

	tls "github.com/tmc/go-iroh/internal/itls/tls"
	"github.com/tmc/go-iroh/key"
)

// TestServerNameSnapshot pins the SNI encoding to iroh's golden vector
// (iroh/src/tls/name.rs test_snapshot): the all-zero secret key's endpoint id
// must encode to this exact name, or a go-iroh dialer would address Rust peers
// with the wrong SNI and fail RFC 7250 server verification.
func TestServerNameSnapshot(t *testing.T) {
	sk := key.NewSecretKey([32]byte{})
	got := ServerName(sk.Public().EndpointID())
	const want = "7dl2ff6emqi2qol3l382krodedij45bn3nh479hqo14a32qpr8kg.iroh.invalid"
	if got != want {
		t.Errorf("ServerName(zero) = %q, want %q", got, want)
	}
}

// TestServerNameRoundTrip checks ServerName and endpointIDFromServerName are
// inverses for arbitrary keys.
func TestServerNameRoundTrip(t *testing.T) {
	for i := 0; i < 16; i++ {
		var seed [32]byte
		seed[0] = byte(i)
		seed[31] = byte(i * 7)
		id := key.NewSecretKey(seed).Public().EndpointID()
		name := ServerName(id)
		got, ok := endpointIDFromServerName(name)
		if !ok {
			t.Fatalf("endpointIDFromServerName(%q) failed", name)
		}
		if !got.Equal(id) {
			t.Errorf("round-trip mismatch for seed %d: got %s want %s", i, got, id)
		}
	}
}

func TestEndpointIDFromServerNameRejects(t *testing.T) {
	bad := []string{
		"",
		"example.com",
		"7dl2ff6emqi2qol3l382krodedij45bn3nh479hqo14a32qpr8kg.example.com",
		"sub.7dl2ff6emqi2qol3l382krodedij45bn3nh479hqo14a32qpr8kg.iroh.invalid",
		"zzz.iroh.invalid", // not valid base32hex of a 32-byte key
		".iroh.invalid",
	}
	for _, name := range bad {
		if _, ok := endpointIDFromServerName(name); ok {
			t.Errorf("endpointIDFromServerName(%q) = ok, want rejected", name)
		}
	}
}

func TestTLSConfigsMatchIrohRawKeyContract(t *testing.T) {
	serverKey := key.NewSecretKey([32]byte{1})
	clientKey := key.NewSecretKey([32]byte{2})
	cache := NewSessionCache()
	alpns := []string{"iroh-test/0"}

	serverTLS, err := serverTLSConfig(serverKey, alpns)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := clientTLSConfig(clientKey, serverKey.Public().EndpointID(), alpns, cache)
	if err != nil {
		t.Fatal(err)
	}

	checkRawKeyTLSConfig(t, "server", serverTLS, serverKey)
	checkRawKeyTLSConfig(t, "client", clientTLS, clientKey)

	if serverTLS.ClientAuth != tls.RequireAnyClientCert {
		t.Errorf("server ClientAuth = %v, want RequireAnyClientCert", serverTLS.ClientAuth)
	}
	if serverTLS.SessionTicketsDisabled {
		t.Error("server disabled session tickets")
	}
	if serverTLS.VerifyConnection == nil {
		t.Fatal("server VerifyConnection is nil")
	}

	if clientTLS.ServerName != ServerName(serverKey.Public().EndpointID()) {
		t.Errorf("client ServerName = %q, want %q", clientTLS.ServerName, ServerName(serverKey.Public().EndpointID()))
	}
	if !clientTLS.InsecureSkipVerify {
		t.Error("client did not replace X.509 verification")
	}
	if clientTLS.VerifyConnection == nil {
		t.Fatal("client VerifyConnection is nil")
	}
	if clientTLS.SessionTicketsDisabled {
		t.Error("client disabled session tickets with cache")
	}
	if clientTLS.ClientSessionCache != cache {
		t.Error("client did not use the provided session cache")
	}
	if maxTLSTickets != 8*32 {
		t.Errorf("maxTLSTickets = %d, want %d", maxTLSTickets, 8*32)
	}

	noCache, err := clientTLSConfig(clientKey, serverKey.Public().EndpointID(), alpns, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !noCache.SessionTicketsDisabled {
		t.Error("client enabled session tickets without a cache")
	}
	if noCache.ClientSessionCache != nil {
		t.Error("client set a session cache when nil was passed")
	}
}

func TestTLSConfigCurvePreferences(t *testing.T) {
	serverKey := key.NewSecretKey([key.SeedSize]byte{1})
	clientKey := key.NewSecretKey([key.SeedSize]byte{2})
	tests := []struct {
		name   string
		curves []tls.CurveID
	}{
		{name: "default"},
		{name: "prefer pq", curves: []tls.CurveID{tls.X25519MLKEM768, tls.X25519, tls.CurveP256, tls.CurveP384}},
		{name: "pq only", curves: []tls.CurveID{tls.X25519MLKEM768}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := clientTLSConfigWithCurves(clientKey, serverKey.Public().EndpointID(), []string{"iroh-test/0"}, nil, tt.curves)
			if err != nil {
				t.Fatal(err)
			}
			server, err := serverTLSConfigWithCurves(serverKey, []string{"iroh-test/0"}, tt.curves)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(client.CurvePreferences, tt.curves) {
				t.Errorf("client CurvePreferences = %v, want %v", client.CurvePreferences, tt.curves)
			}
			if !slices.Equal(server.CurvePreferences, tt.curves) {
				t.Errorf("server CurvePreferences = %v, want %v", server.CurvePreferences, tt.curves)
			}
		})
	}
}

func TestKeyExchangePolicyCurves(t *testing.T) {
	tests := []struct {
		name   string
		policy KeyExchangePolicy
		want   []tls.CurveID
	}{
		{name: "default", policy: KeyExchangeDefault},
		{name: "classical", policy: KeyExchangeClassical, want: []tls.CurveID{tls.X25519, tls.CurveP256, tls.CurveP384, tls.CurveP521}},
		{name: "prefer pq", policy: KeyExchangePreferPQ, want: []tls.CurveID{tls.X25519MLKEM768, tls.X25519, tls.CurveP256, tls.CurveP384}},
		{name: "pq only", policy: KeyExchangePQOnly, want: []tls.CurveID{tls.X25519MLKEM768}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.policy.curves(); !slices.Equal(got, tt.want) {
				t.Fatalf("curves = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTLSVerifyConnectionIdentityPolicy(t *testing.T) {
	serverKey := key.NewSecretKey([32]byte{1})
	clientKey := key.NewSecretKey([32]byte{2})
	otherClientKey := key.NewSecretKey([32]byte{3})
	wrongServerKey := key.NewSecretKey([32]byte{4})

	serverTLS, err := serverTLSConfig(serverKey, []string{"iroh-test/0"})
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := clientTLSConfig(clientKey, serverKey.Public().EndpointID(), []string{"iroh-test/0"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := clientTLS.VerifyConnection(connectionStateForKey(t, serverKey)); err != nil {
		t.Fatalf("client rejected dialed server id: %v", err)
	}
	if err := clientTLS.VerifyConnection(connectionStateForKey(t, wrongServerKey)); err == nil {
		t.Fatal("client accepted a server id different from the dialed id")
	}

	if err := serverTLS.VerifyConnection(connectionStateForKey(t, clientKey)); err != nil {
		t.Fatalf("server rejected client id: %v", err)
	}
	if err := serverTLS.VerifyConnection(connectionStateForKey(t, otherClientKey)); err != nil {
		t.Fatalf("server rejected a different client id: %v", err)
	}
}

// TestRawKeyCertificateMatchesKey checks the certificate's public key is the
// endpoint id of the secret key.
func TestRawKeyCertificateMatchesKey(t *testing.T) {
	sk, err := key.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	cert, err := rawKeyCertificate(sk)
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.Certificate) != 1 {
		t.Fatalf("expected 1 cert entry, got %d", len(cert.Certificate))
	}
	pub := rawKeyCertificatePublicKey(t, cert)
	want := sk.Public().Bytes()
	if !bytes.Equal(pub, want[:]) {
		t.Error("certificate SPKI does not carry the endpoint id public key")
	}
}

func checkRawKeyTLSConfig(t *testing.T, name string, cfg *tls.Config, sk key.SecretKey) {
	t.Helper()
	if !cfg.RawPublicKeys {
		t.Errorf("%s RawPublicKeys = false", name)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("%s MinVersion = %x, want TLS 1.3", name, cfg.MinVersion)
	}
	if cfg.MaxVersion != tls.VersionTLS13 {
		t.Errorf("%s MaxVersion = %x, want TLS 1.3", name, cfg.MaxVersion)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("%s certificates = %d, want 1", name, len(cfg.Certificates))
	}
	pub := rawKeyCertificatePublicKey(t, cfg.Certificates[0])
	want := sk.Public().Bytes()
	if !bytes.Equal(pub, want[:]) {
		t.Errorf("%s certificate public key does not match endpoint id", name)
	}
	priv, ok := cfg.Certificates[0].PrivateKey.(ed25519.PrivateKey)
	if !ok {
		t.Fatalf("%s private key type = %T, want ed25519.PrivateKey", name, cfg.Certificates[0].PrivateKey)
	}
	if !bytes.Equal(priv.Public().(ed25519.PublicKey), pub) {
		t.Errorf("%s private key does not match certificate public key", name)
	}
}

func rawKeyCertificatePublicKey(t *testing.T, cert tls.Certificate) ed25519.PublicKey {
	t.Helper()
	if len(cert.Certificate) != 1 {
		t.Fatalf("expected 1 cert entry, got %d", len(cert.Certificate))
	}
	pub, err := x509.ParsePKIXPublicKey(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse raw-key SPKI: %v", err)
	}
	key, ok := pub.(ed25519.PublicKey)
	if !ok {
		t.Fatalf("raw-key public key type = %T, want ed25519.PublicKey", pub)
	}
	return key
}

func connectionStateForKey(t *testing.T, sk key.SecretKey) tls.ConnectionState {
	t.Helper()
	cert, err := rawKeyCertificate(sk)
	if err != nil {
		t.Fatal(err)
	}
	pub := rawKeyCertificatePublicKey(t, cert)
	return tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{{
			Raw:                     cert.Certificate[0],
			RawSubjectPublicKeyInfo: cert.Certificate[0],
			PublicKey:               pub,
			PublicKeyAlgorithm:      x509.Ed25519,
		}},
	}
}
