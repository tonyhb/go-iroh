// Package pprofserver serves Go runtime profiles on a dedicated HTTP listener.
package pprofserver

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

const readHeaderTimeout = 5 * time.Second

// A Server serves runtime profiles until it is closed.
type Server struct {
	server   *http.Server
	listener net.Listener
}

// Start starts a pprof HTTP server on addr.
func Start(addr string, logger *log.Logger) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("pprof listener: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	s := &Server{server: httpServer, listener: ln}
	logger.Printf("pprof listening on http://%s/debug/pprof/", ln.Addr())
	go func() {
		if err := httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("pprof server: %v", err)
		}
	}()
	return s, nil
}

// Addr returns the server's listen address.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// Close stops the server.
func (s *Server) Close() error {
	return s.server.Close()
}
