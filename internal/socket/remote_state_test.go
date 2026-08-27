package socket

import (
	"net/netip"
	"testing"
)

// TestPathInfosDeduplicatesConnAddr covers a connection whose qng-validated
// path names the connection's own address. cs.addr and cs.paths then hold the
// same address, and PathInfos must still report it once: two entries for one
// address both compare equal to the selected address, so both report Selected,
// and only the last of them receives merged multipath statistics.
func TestPathInfosDeduplicatesConnAddr(t *testing.T) {
	addr := IPAddr(netip.MustParseAddrPort("[::1]:41139"))
	conn := newFakeConn(addr, 0)

	a := &RemoteStateActor{
		conns: map[Connection]*connState{},
		paths: NewRemotePathState(),
	}
	sel := addr
	a.selected = &sel
	a.conns[conn] = &connState{
		conn: conn,
		addr: addr,
		// syncMultipathPathsLocked appends validated qng paths here with
		// appendUniqueAddr, which dedupes within paths but cannot see cs.addr.
		paths: []Addr{addr},
	}

	infos := a.PathInfos(conn)
	if len(infos) != 1 {
		t.Fatalf("path count = %d, want 1; infos=%+v", len(infos), infos)
	}
	var selected int
	for _, p := range infos {
		if p.Selected {
			selected++
		}
	}
	if selected != 1 {
		t.Fatalf("selected path count = %d, want 1; infos=%+v", selected, infos)
	}
}
