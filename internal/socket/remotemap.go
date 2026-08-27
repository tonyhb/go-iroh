package socket

import (
	"context"
	"net/netip"
	"sync"
	"time"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// RemoteMap is the registry of per-remote [RemoteStateActor]s, keyed by
// [key.EndpointID]. It spawns an actor on first reference to a remote and
// removes it when the actor idles out. It is the Go analog of the Rust RemoteMap
// (iroh/src/socket/remote_map.rs).
//
// Actor insertion and idle-teardown deregistration are serialized by a single
// mutex, and an actor only removes itself if it is still the actor registered
// under its id. So an AddConnection arriving exactly as the 60s idle timeout
// fires yields exactly one actor: the teardown either runs first (the next
// reference spawns a fresh actor) or the AddConnection runs first (which resets
// the actor's idle timer, so it does not tear down). See [RemoteMap.ResolveRemote]
// / [RemoteMap.AddConnection] and the onExit closure in [RemoteMap.actor].
//
// RemoteMap is safe for concurrent use. Create one with [NewRemoteMap].
type RemoteMap struct {
	ctx      context.Context
	selector PathSelector
	resolve  ResolveFunc
	idle     time.Duration // actor idle timeout; ActorMaxIdleTimeout unless overridden for tests
	metrics  *Metrics

	mu          sync.Mutex
	actors      map[key.EndpointID]*RemoteStateActor
	onEvict     func(id key.EndpointID, addrs []Addr)
	noHolepunch bool
}

// NewRemoteMap returns a RemoteMap whose actors live until ctx is cancelled or
// they idle out. selector is the path selector shared by all actors (nil uses
// [BiasedRttPathSelector]); resolve is the address-lookup hook (nil disables
// lookup-driven resolution).
func NewRemoteMap(ctx context.Context, selector PathSelector, resolve ResolveFunc) *RemoteMap {
	return newRemoteMap(ctx, selector, resolve, ActorMaxIdleTimeout, nil)
}

// NewRemoteMapWithMetrics is like [NewRemoteMap], but records actor path
// lifecycle counters in metrics.
func NewRemoteMapWithMetrics(ctx context.Context, selector PathSelector, resolve ResolveFunc, metrics *Metrics) *RemoteMap {
	return newRemoteMap(ctx, selector, resolve, ActorMaxIdleTimeout, metrics)
}

// newRemoteMap is the constructor with a configurable actor idle timeout, so
// tests can drive the idle-teardown race without waiting the full minute.
func newRemoteMap(ctx context.Context, selector PathSelector, resolve ResolveFunc, idle time.Duration, metrics *Metrics) *RemoteMap {
	if selector == nil {
		selector = BiasedRttPathSelector{}
	}
	return &RemoteMap{
		ctx:      ctx,
		selector: selector,
		resolve:  resolve,
		idle:     idle,
		metrics:  metrics,
		actors:   make(map[key.EndpointID]*RemoteStateActor),
	}
}

// actor returns the running actor for id, spawning one if none is registered.
// The caller must hold m.mu. The spawned actor's onExit removes it from the map
// under m.mu, but only if it is still the actor registered under id, so a
// concurrently-spawned successor is never reaped.
func (m *RemoteMap) actor(id key.EndpointID) *RemoteStateActor {
	if a, ok := m.actors[id]; ok {
		return a
	}
	var a *RemoteStateActor
	onExit := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		// The captured a is assigned by the spawner under m.mu, and onExit
		// runs deferred in the actor goroutine, so under m.mu both the
		// variable and the actor-owned path state are race-free to read.
		addrs := a.paths.Addrs()
		successor := m.actors[id]
		if successor == a {
			delete(m.actors, id)
			successor = nil
		}
		// Evict the remote's mapped addresses unless a successor actor has
		// already been spawned for the same id (it shares them). Calling the
		// hook under m.mu keeps eviction ordered before any later spawn, so a
		// fresh actor always regenerates fresh mappings.
		if successor == nil && m.onEvict != nil {
			m.onEvict(id, addrs)
		}
	}
	a = newRemoteStateActor(m.ctx, id, m.selector, m.resolve, m.idle, m.metrics, onExit)
	a.noHolepunch.Store(m.noHolepunch)
	m.actors[id] = a
	return a
}

// DisableHolepunch stops actors from initiating NAT traversal or direct-path
// validation on their upgrade tick. Endpoints without IP transports set it:
// there is no direct path to punch toward, and a traversal round initiated on
// a relay-only connection stalls its in-flight relay streams. Set it before
// the first remote is referenced.
func (m *RemoteMap) DisableHolepunch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.noHolepunch = true
}

// SetOnEvict sets f to be called when a remote's actor is reaped with no
// successor, passing the remote's known path addresses. The endpoint uses it to
// release the remote's mapped addresses (see [Socket.EvictRemote]), so the
// mapped-address tables do not grow without bound under peer churn. f is called
// with the map's internal mutex held and must not call back into the RemoteMap.
// Set it before the first remote is referenced.
func (m *RemoteMap) SetOnEvict(f func(id key.EndpointID, addrs []Addr)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onEvict = f
}

// Actor returns the running actor for id, spawning one if none exists.
func (m *RemoteMap) Actor(id key.EndpointID) *RemoteStateActor {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.actor(id)
}

// AddNATTraversalAddresses reconciles local QNT candidates on currently-active
// remote actors. It does not spawn actors and ignores per-actor errors, because
// candidate updates must not make an established endpoint fail.
func (m *RemoteMap) AddNATTraversalAddresses(addrs []netip.AddrPort) {
	m.mu.Lock()
	actors := make([]*RemoteStateActor, 0, len(m.actors))
	for _, a := range m.actors {
		actors = append(actors, a)
	}
	m.mu.Unlock()
	for _, a := range actors {
		_ = a.AddNATTraversalAddresses(addrs)
	}
}

// Len returns the number of registered actors. Intended for tests and metrics.
func (m *RemoteMap) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.actors)
}

// RemoteInfo returns a snapshot for id if a running actor exists. It does not
// spawn a new actor.
func (m *RemoteMap) RemoteInfo(id key.EndpointID) (RemoteInfo, bool) {
	m.mu.Lock()
	a, ok := m.actors[id]
	m.mu.Unlock()
	if !ok {
		return RemoteInfo{}, false
	}
	select {
	case <-a.donec():
		m.dropIfStopped(id, a)
		return RemoteInfo{}, false
	default:
	}
	return a.RemoteInfo(), true
}

// AddConnection registers conn with the actor for remote, spawning the actor if
// needed, and returns the connection's path-event channel. Registering a
// connection resets the actor's idle timer, so this can race the idle teardown
// safely (O12): if the actor is mid-teardown the send observes its done channel
// and a fresh actor is spawned on the retry.
func (m *RemoteMap) AddConnection(remote key.EndpointID, conn Connection) <-chan PathEvent {
	ch, _ := m.AddConnectionActor(remote, conn)
	return ch
}

// AddConnectionActor is like [RemoteMap.AddConnection], but also returns the
// actor that accepted conn.
func (m *RemoteMap) AddConnectionActor(remote key.EndpointID, conn Connection) (<-chan PathEvent, *RemoteStateActor) {
	for {
		m.mu.Lock()
		a := m.actor(remote)
		m.mu.Unlock()

		// If the actor stopped between selection and use, spawn a fresh one.
		select {
		case <-a.donec():
			m.dropIfStopped(remote, a)
			continue
		default:
		}
		ch, ok := a.AddConnection(conn)
		// If the actor stopped before it could register the connection, drop it
		// and retry with a fresh actor.
		if !ok {
			m.dropIfStopped(remote, a)
			continue
		}
		return ch, a
	}
}

// ResolveRemote asks the actor for addr.ID to resolve and register more
// candidate paths, spawning the actor if needed. It returns the lookup error if
// any. It races idle teardown the same way as [RemoteMap.AddConnection].
func (m *RemoteMap) ResolveRemote(addr netaddr.EndpointAddr) error {
	for {
		m.mu.Lock()
		a := m.actor(addr.ID)
		m.mu.Unlock()

		select {
		case <-a.donec():
			m.dropIfStopped(addr.ID, a)
			continue
		default:
		}
		err := a.ResolveRemote(addr)
		// A canceled result with a stopped actor means we raced teardown; retry.
		if err == context.Canceled {
			select {
			case <-a.donec():
				m.dropIfStopped(addr.ID, a)
				continue
			default:
			}
		}
		return err
	}
}

// dropIfStopped removes a from the map if it is the stopped actor still
// registered under id, so the next reference spawns a fresh actor. The actor's
// own onExit already does this; dropIfStopped covers the window before onExit
// has run.
func (m *RemoteMap) dropIfStopped(id key.EndpointID, a *RemoteStateActor) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.actors[id] == a {
		delete(m.actors, id)
	}
}
