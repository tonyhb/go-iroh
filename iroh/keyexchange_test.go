package iroh

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/tmc/go-iroh/netaddr"
)

func TestKeyExchangePolicyNegotiation(t *testing.T) {
	tests := []struct {
		name   string
		policy KeyExchangePolicy
		group  string
	}{
		{name: "classical", policy: KeyExchangeClassical, group: "X25519"},
		{name: "prefer pq", policy: KeyExchangePreferPQ, group: "X25519MLKEM768"},
		{name: "pq only", policy: KeyExchangePQOnly, group: "X25519MLKEM768"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server, err := Bind(ctx, WithALPNs("iroh-kx-test/0"), WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")), WithKeyExchangePolicy(tt.policy))
			if err != nil {
				t.Fatal(err)
			}
			defer server.Shutdown(context.Background())
			client, err := Bind(ctx, WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")), WithKeyExchangePolicy(tt.policy))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Shutdown(context.Background())

			accepted := make(chan *Conn, 1)
			acceptErr := make(chan error, 1)
			go func() {
				conn, err := server.Accept(ctx)
				if err != nil {
					acceptErr <- err
					return
				}
				accepted <- conn
			}()
			addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
			conn, err := client.Connect(ctx, addr, "iroh-kx-test/0")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if got := conn.KeyExchangeGroup(); got != tt.group {
				t.Fatalf("client group = %q, want %q", got, tt.group)
			}
			select {
			case peer := <-accepted:
				defer peer.Close()
				if got := peer.KeyExchangeGroup(); got != tt.group {
					t.Fatalf("server group = %q, want %q", got, tt.group)
				}
			case err := <-acceptErr:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestKeyExchangePQOnlyRefusesClassical(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	server, err := Bind(ctx, WithALPNs("iroh-kx-test/0"), WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")), WithKeyExchangePolicy(KeyExchangePQOnly))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	client, err := Bind(ctx, WithBindAddr(netip.MustParseAddrPort("127.0.0.1:0")), WithKeyExchangePolicy(KeyExchangeClassical))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Shutdown(context.Background())
	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	if _, err := client.Connect(ctx, addr, "iroh-kx-test/0"); err == nil {
		t.Fatal("classical client connected to PQ-only server")
	}
}

func TestWithKeyExchangePolicyRejectsInvalidValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := Bind(ctx, WithKeyExchangePolicy(KeyExchangePolicy(255))); err == nil {
		t.Fatal("Bind accepted invalid key exchange policy")
	}
}
