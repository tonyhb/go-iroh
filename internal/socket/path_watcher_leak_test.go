package socket

import (
	"context"
	"net/netip"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/key"
)

// drainUntilClosed reads events from ch until it closes, failing t if that
// takes longer than the deadline. A closed channel proves the subscription's
// delivery goroutine exited.
func drainUntilClosed(t *testing.T, ch <-chan PathEvent) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timeout:
			t.Fatal("event channel not closed: delivery goroutine still running")
		}
	}
}

// parkDelivery drives a fresh subscriber into the state where its delivery
// goroutine is blocked handing an event to a reader that stopped reading.
//
// Reaching that state takes two waves. A single burst does not: Send never
// blocks, so a synchronous burst outruns delivery, overflows the ring, and
// collapses into one Lagged event that fits in the channel buffer, leaving the
// goroutine idle in cond.Wait with nothing pending. Only after delivery has
// settled with the buffer full does one further event force it to block.
func parkDelivery(t *testing.T, w *PathWatcher) (<-chan PathEvent, func()) {
	t.Helper()
	base := blockedDeliveries()
	ch, cancel := w.Subscribe()
	addr := IPAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 9))

	for range cap(ch) {
		w.Send(PathEvent{Kind: PathEventOpened, Addr: addr})
	}
	waitFor(t, "channel buffer to fill", func() bool { return len(ch) == cap(ch) })
	waitFor(t, "delivery to settle idle", func() bool { return blockedDeliveries() == base })

	w.Send(PathEvent{Kind: PathEventOpened, Addr: addr})
	waitFor(t, "delivery to block mid-send", func() bool { return blockedDeliveries() == base+1 })
	return ch, cancel
}

// blockedDeliveries counts delivery goroutines parked in the blocking send
// rather than idle in cond.Wait. Callers compare against a baseline taken
// before subscribing, so a goroutine leaked by an earlier test cannot be
// mistaken for this one.
func blockedDeliveries() int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(g, "(*pathSub).deliver") && strings.Contains(g, "[select]") {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestPathWatcherCancelUnblocksAbandonedSubscriber pins the Subscribe cancel
// contract: cancelling a subscription whose reader stopped must end delivery
// and close the channel.
func TestPathWatcherCancelUnblocksAbandonedSubscriber(t *testing.T) {
	w := NewPathWatcher()
	defer w.Close()
	ch, cancel := w.Subscribe()

	// Overfill: the channel buffer, then the ring, then lag accounting.
	addr := IPAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 9))
	for range 3 * (PathBroadcastCapacity + 1) {
		w.Send(PathEvent{Kind: PathEventOpened, Addr: addr})
	}

	cancel()
	drainUntilClosed(t, ch)
}

// TestPathWatcherCloseUnblocksParkedSubscriber holds [PathWatcher.Close] to its
// documented contract against the case that used to defeat it: a subscriber
// that stopped reading with its delivery goroutine blocked mid-send. Close once
// left that goroutine parked forever, because it declined to close done.
func TestPathWatcherCloseUnblocksParkedSubscriber(t *testing.T) {
	base := blockedDeliveries()
	w := NewPathWatcher()
	parkDelivery(t, w) // leaves the subscription abandoned on purpose

	w.Close()
	waitFor(t, "Close to stop the delivery goroutine", func() bool { return blockedDeliveries() == base })
}

// TestPathWatcherCancelUnblocksParkedSubscriber is the same case against the
// cancel path.
func TestPathWatcherCancelUnblocksParkedSubscriber(t *testing.T) {
	w := NewPathWatcher()
	defer w.Close()
	ch, cancel := parkDelivery(t, w)

	cancel()
	drainUntilClosed(t, ch)
}

// TestActorEndsConnSubscriptionOnConnClose verifies that the actor cancels the
// subscription it creates for a connection when that connection closes, even
// though AddConnection callers have no cancel handle of their own.
func TestActorEndsConnSubscriptionOnConnClose(t *testing.T) {
	a := newRemoteStateActor(t.Context(), key.EndpointID{}, nil, nil, time.Minute, nil, nil)

	addr := IPAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 9))
	conn := newFakeConn(addr, time.Millisecond)
	ch, ok := a.AddConnection(conn)
	if !ok {
		t.Fatal("AddConnection: actor stopped")
	}

	conn.Close()
	drainUntilClosed(t, ch)
}

// TestActorStopsWithUnreadSubscription is the drain-deadlock regression test:
// stopping an actor that still has a connection whose event channel nobody
// reads must not hang. Before the per-connection cancel, the watcher's
// closing drain waited forever on the abandoned reader and the actor
// goroutine leaked.
func TestActorStopsWithUnreadSubscription(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	a := newRemoteStateActor(ctx, key.EndpointID{}, nil, nil, time.Minute, nil, nil)

	addr := IPAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 9))
	conn := newFakeConn(addr, time.Millisecond)
	defer conn.Close()
	ch, ok := a.AddConnection(conn)
	if !ok {
		t.Fatal("AddConnection: actor stopped")
	}

	// Block the delivery goroutine mid-send: nobody reads ch.
	for range 3 * (PathBroadcastCapacity + 1) {
		a.watcher.Send(PathEvent{Kind: PathEventOpened, Addr: addr})
	}

	cancel()
	select {
	case <-a.donec():
	case <-time.After(5 * time.Second):
		t.Fatal("actor did not stop: watcher drain deadlocked on unread subscription")
	}
	drainUntilClosed(t, ch)
}
