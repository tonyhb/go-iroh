package socket

import (
	"sync"
)

// PathBroadcastCapacity is the per-subscriber buffer capacity for path events. A
// subscriber that falls more than this many events behind is told how many it
// missed via a [PathEventLagged] event rather than silently dropping. It matches
// the Rust BROADCAST_CAPACITY
// (iroh/src/socket/remote_map/remote_state/path_watcher.rs:50).
const PathBroadcastCapacity = 8

// PathEventKind tags the variant of a [PathEvent].
type PathEventKind int

const (
	// PathEventOpened reports a newly-opened network path.
	PathEventOpened PathEventKind = iota
	// PathEventClosed reports a closed network path.
	PathEventClosed
	// PathEventSelected reports that a path was selected for transmission.
	PathEventSelected
	// PathEventLagged reports that events were dropped before a subscriber read
	// them; Missed carries the count.
	PathEventLagged
)

func (k PathEventKind) String() string {
	switch k {
	case PathEventOpened:
		return "opened"
	case PathEventClosed:
		return "closed"
	case PathEventSelected:
		return "selected"
	case PathEventLagged:
		return "lagged"
	default:
		return "invalid"
	}
}

// PathEvent is a lifecycle notification for a network path of a connection. It
// is the Go analog of the Rust PathEvent enum (path_watcher.rs:55).
//
// For Opened, Closed, and Selected, Addr identifies the path. For Lagged, Missed
// is the number of events the subscriber missed and Addr is the zero value.
type PathEvent struct {
	// Kind is which kind of event this is.
	Kind PathEventKind
	// Addr is the path's transport address (zero for Lagged).
	Addr Addr
	// Missed is the number of dropped events (only for Lagged).
	Missed uint64
}

// PathWatcher is a drop-oldest broadcast of [PathEvent]s to any number of
// subscribers. Each subscriber has its own ring buffer of [PathBroadcastCapacity]
// events; when a subscriber falls behind, the oldest buffered event is dropped
// and the next event the subscriber receives is a [PathEventLagged] with the
// running missed count, mirroring tokio::broadcast's lagged-receiver behavior
// (path_watcher.rs).
//
// PathWatcher is safe for concurrent use. The writer calls [PathWatcher.Send];
// readers call [PathWatcher.Subscribe] and consume the returned channel. Each
// subscriber is served by a dedicated delivery goroutine that stops when the
// subscriber is cancelled or the watcher is closed.
type PathWatcher struct {
	mu     sync.Mutex
	subs   map[*pathSub]struct{}
	closed bool
}

// pathSub is a single subscriber. The ring buffer and lag bookkeeping are
// guarded by mu; cond wakes the delivery goroutine when an event is enqueued or
// the subscriber is closed.
type pathSub struct {
	mu     sync.Mutex
	cond   *sync.Cond
	ring   []PathEvent // pending events, oldest first; len <= PathBroadcastCapacity
	missed uint64      // events dropped since the last delivered Lagged
	lagged bool        // a Lagged event is pending delivery
	closed bool

	ch   chan PathEvent
	done chan struct{}
	once sync.Once
}

// NewPathWatcher returns an empty broadcast with no subscribers.
func NewPathWatcher() *PathWatcher {
	return &PathWatcher{subs: make(map[*pathSub]struct{})}
}

// Subscribe registers a new subscriber and returns the channel its events are
// delivered on plus a function to unsubscribe and stop delivery. Events sent
// before Subscribe are not replayed. The channel is closed when the subscriber
// is cancelled or the watcher is closed.
//
// The cancel function should be called when the subscriber is done, like
// [time.Ticker.Stop]: each subscription runs a delivery goroutine, which
// cancel stops without waiting for the subscriber to drain. [PathWatcher.Close]
// stops the goroutine of every remaining subscriber, so an abandoned
// subscription outlives at most the watcher itself. Events already buffered on
// the channel stay readable after it is closed; events still pending delivery
// when a subscriber stops reading are dropped.
func (w *PathWatcher) Subscribe() (<-chan PathEvent, func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := &pathSub{
		ch:   make(chan PathEvent, PathBroadcastCapacity+1),
		done: make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	if w.closed {
		close(s.ch)
		s.closed = true
		return s.ch, func() {}
	}
	w.subs[s] = struct{}{}
	go s.deliver()
	return s.ch, func() { w.unsubscribe(s) }
}

func (w *PathWatcher) unsubscribe(s *pathSub) {
	w.mu.Lock()
	if _, ok := w.subs[s]; !ok {
		w.mu.Unlock()
		return
	}
	delete(w.subs, s)
	w.mu.Unlock()
	s.close()
}

// Send broadcasts ev to every subscriber. A subscriber whose ring buffer is full
// has its oldest pending event dropped and its missed counter incremented; the
// next event that subscriber receives is a [PathEventLagged] carrying the missed
// count. Send never blocks.
func (w *PathWatcher) Send(ev PathEvent) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	subs := make([]*pathSub, 0, len(w.subs))
	for s := range w.subs {
		subs = append(subs, s)
	}
	w.mu.Unlock()
	for _, s := range subs {
		s.enqueue(ev)
	}
}

// Close stops every subscriber's delivery goroutine, closing its channel, and
// rejects future sends and subscriptions. It is idempotent. It is the analog of
// dropping the Rust broadcast sender, which ends every outstanding receiver.
func (w *PathWatcher) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	subs := make([]*pathSub, 0, len(w.subs))
	for s := range w.subs {
		subs = append(subs, s)
	}
	w.subs = make(map[*pathSub]struct{})
	w.mu.Unlock()
	for _, s := range subs {
		s.close()
	}
}

// enqueue appends ev to the subscriber's ring buffer, dropping the oldest
// pending event and accruing a missed count if the buffer is full, then wakes
// the delivery goroutine. Never blocks.
func (s *pathSub) enqueue(ev PathEvent) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if len(s.ring) >= PathBroadcastCapacity {
		s.ring = s.ring[1:] // drop oldest
		s.missed++
		s.lagged = true
	}
	s.ring = append(s.ring, ev)
	s.cond.Signal()
	s.mu.Unlock()
}

// close marks the subscriber closed, wakes its delivery goroutine, and
// interrupts an in-flight send so the goroutine cannot outlive the
// subscription. Idempotent.
func (s *pathSub) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.cond.Signal()
	s.mu.Unlock()
	s.once.Do(func() { close(s.done) })
}

// deliver is the per-subscriber delivery goroutine. It pops events from the ring
// buffer and sends them on the channel, prepending a single Lagged event
// whenever one is pending. It exits and closes the channel when the subscriber
// is closed and its ring is drained. If the subscriber stops reading, close
// interrupts an in-flight send instead of waiting forever for the drain.
func (s *pathSub) deliver() {
	for {
		s.mu.Lock()
		for !s.closed && !s.lagged && len(s.ring) == 0 {
			s.cond.Wait()
		}
		if s.closed && !s.lagged && len(s.ring) == 0 {
			s.mu.Unlock()
			close(s.ch)
			return
		}
		var out PathEvent
		if s.lagged {
			out = PathEvent{Kind: PathEventLagged, Missed: s.missed}
			s.lagged = false
			s.missed = 0
		} else {
			out = s.ring[0]
			s.ring = s.ring[1:]
		}
		s.mu.Unlock()
		select {
		case s.ch <- out:
			continue
		default:
		}
		select {
		case s.ch <- out:
		case <-s.done:
			close(s.ch)
			return
		}
	}
}
