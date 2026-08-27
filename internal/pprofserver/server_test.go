package pprofserver

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestStartServesProfiles(t *testing.T) {
	var logOut bytes.Buffer
	s, err := Start("127.0.0.1:0", log.New(&logOut, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	baseURL := "http://" + s.Addr().String()
	client := &http.Client{Timeout: 5 * time.Second}
	tests := []struct {
		path string
		want string
	}{
		{path: "/debug/pprof/", want: "goroutine"},
		{path: "/debug/pprof/goroutine?debug=1", want: "goroutine profile:"},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			url := baseURL + test.path
			resp, err := client.Get(url)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s: status %s", url, resp.Status)
			}
			if !bytes.Contains(body, []byte(test.want)) {
				t.Fatalf("GET %s: response does not contain %q", url, test.want)
			}
		})
	}
	resp, err := client.Get(baseURL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /: status %s, want 404 Not Found", resp.Status)
	}

	indexURL := baseURL + "/debug/pprof/"
	if !strings.Contains(logOut.String(), indexURL) {
		t.Fatalf("startup log %q does not contain %q", logOut.String(), indexURL)
	}
}

func TestStartFailsWhenAddressInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	_, err = Start(ln.Addr().String(), log.New(io.Discard, "", 0))
	if err == nil {
		t.Fatal("Start succeeded with address in use")
	}
	if !strings.Contains(err.Error(), "pprof listener") {
		t.Fatalf("Start error = %q, want pprof listener context", err)
	}
}
