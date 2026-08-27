// Package watch provides an observable value: a [Value] that can be updated and
// one or more [Observer] handles that observe its changes.
//
// It is the Go analog of iroh's n0_watcher (Watchable + Watcher). An [Observer]
// exposes the current value, a one-shot wait for the next change, and an
// iterator stream of values. The root package uses Observer for APIs such as
// Endpoint.WatchAddr.
//
// The Go API is not stable before v1 and may change in any v0 release.
package watch

import (
	"context"
	"iter"
	"sync"
)

// Value is an observable container holding a T. It is safe for concurrent use.
// Updates are published to all [Observer] handles created from it.
//
// The zero Value holds the zero T and is ready to use.
type Value[T any] struct {
	mu      sync.Mutex
	val     T
	set     bool
	version uint64
	notify  chan struct{} // closed and replaced on each Set
	equal   func(a, b T) bool
}

// NewValue returns a Value initialized to v.
func NewValue[T any](v T) *Value[T] {
	return &Value[T]{val: v, set: true}
}

// NewValueFunc returns a Value initialized to v. The equal function, if non-nil,
// suppresses notifications for no-op updates.
func NewValueFunc[T any](v T, equal func(a, b T) bool) *Value[T] {
	return &Value[T]{val: v, set: true, equal: equal}
}

// Set updates the value and notifies all watchers if it changed.
func (s *Value[T]) Set(v T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.set && s.equal != nil && s.equal(s.val, v) {
		return
	}
	s.val = v
	s.set = true
	s.version++
	if s.notify != nil {
		close(s.notify)
		s.notify = nil
	}
}

// Current returns the current value.
func (s *Value[T]) Current() T {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.val
}

// Watch returns an [Observer] observing this value.
func (s *Value[T]) Watch() Observer[T] {
	return &watcher[T]{src: s}
}

// changed returns the current value, version, and a channel closed on the next
// change after the given version.
func (s *Value[T]) waitChan(after uint64) (T, uint64, <-chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.version != after {
		return s.val, s.version, closedChan
	}
	if s.notify == nil {
		s.notify = make(chan struct{})
	}
	return s.val, s.version, s.notify
}

var closedChan = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// Observer observes a [Value]. Multiple observers may observe the same value
// independently. It is the Go analog of iroh's n0_watcher::Watcher.
type Observer[T any] interface {
	// Current returns the current value.
	Current() T
	// Updated blocks until the value changes after the watcher's last observed
	// version, returning the new value, or ctx.Err() if the context is done.
	// The first call returns the current value immediately.
	Updated(ctx context.Context) (T, error)
	// Stream returns an iterator that yields each new value until ctx is done.
	// The current value is delivered first.
	Stream(ctx context.Context) iter.Seq[T]
}

type watcher[T any] struct {
	src  *Value[T]
	seen uint64
	once bool
}

func (w *watcher[T]) Current() T { return w.src.Current() }

func (w *watcher[T]) Updated(ctx context.Context) (T, error) {
	if !w.once {
		w.once = true
		v, ver, _ := w.src.waitChan(^uint64(0)) // force "current" on first call
		w.seen = ver
		return v, nil
	}
	for {
		v, ver, ch := w.src.waitChan(w.seen)
		if ver != w.seen {
			w.seen = ver
			return v, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		}
	}
}

func (w *watcher[T]) Stream(ctx context.Context) iter.Seq[T] {
	// Independent cursor so Stream doesn't disturb Updated on the same watcher.
	sub := &watcher[T]{src: w.src}
	return func(yield func(T) bool) {
		for {
			v, err := sub.Updated(ctx)
			if err != nil {
				return
			}
			if !yield(v) {
				return
			}
		}
	}
}
