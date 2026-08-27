// Command iroh-dns-server runs the pkarr HTTP and DNS surfaces used by iroh
// discovery.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/tmc/go-iroh/dnsserver"
	"github.com/tmc/go-iroh/internal/pprofserver"
)

const (
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 10 * time.Second
	httpWriteTimeout      = 10 * time.Second
	httpIdleTimeout       = time.Minute
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "iroh-dns-server:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("iroh-dns-server", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", ":3350", "listen address")
	dnsAddr := fs.String("dns-addr", "", "UDP DNS listen address")
	pprofAddr := fs.String("pprof-addr", "", "pprof HTTP listen address (disabled if empty)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if *pprofAddr != "" {
		profiler, err := pprofserver.Start(*pprofAddr, log.New(os.Stderr, "", log.LstdFlags))
		if err != nil {
			return err
		}
		defer profiler.Close()
	}
	server := dnsserver.New()
	if *dnsAddr == "" {
		return newHTTPServer(*addr, server).ListenAndServe()
	}
	pc, err := net.ListenPacket("udp", *dnsAddr)
	if err != nil {
		return err
	}
	defer pc.Close()

	errc := make(chan error, 2)
	go func() { errc <- server.ServePacketConn(context.Background(), pc) }()
	go func() { errc <- newHTTPServer(*addr, server).ListenAndServe() }()
	return <-errc
}

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
}
