package main

import (
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestRunRejectsPprofAddressInUse(t *testing.T) {
	pprofListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pprofListener.Close()

	err = run([]string{
		"-addr=127.0.0.1:0",
		"-pprof-addr=" + pprofListener.Addr().String(),
	})
	if err == nil {
		t.Fatal("run succeeded with pprof address in use")
	}
	if !strings.Contains(err.Error(), "pprof listener") {
		t.Fatalf("run error = %q, want pprof listener context", err)
	}
}

func TestHTTPServerTimeouts(t *testing.T) {
	srv := newHTTPServer(":0", http.NotFoundHandler())
	if srv.ReadHeaderTimeout != httpReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %v, want %v", srv.ReadHeaderTimeout, httpReadHeaderTimeout)
	}
	if srv.ReadTimeout != httpReadTimeout {
		t.Fatalf("ReadTimeout = %v, want %v", srv.ReadTimeout, httpReadTimeout)
	}
	if srv.WriteTimeout != httpWriteTimeout {
		t.Fatalf("WriteTimeout = %v, want %v", srv.WriteTimeout, httpWriteTimeout)
	}
	if srv.IdleTimeout != httpIdleTimeout {
		t.Fatalf("IdleTimeout = %v, want %v", srv.IdleTimeout, httpIdleTimeout)
	}
}
