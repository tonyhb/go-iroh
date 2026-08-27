// Package stats counts how often a fuzztape machine's ops actually run.
//
// A stateful property test can silently stop testing anything — every
// op of one kind rejected, or never selected. Wrapping a machine's ops
// with [Wrap] and logging the [Report] turns that silence into either
// genuine confidence or a visible gap in the machine's reach.
package stats

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/tmc/go-iroh/internal/fuzztape"
)

// A Report accumulates per-op counts across every input the wrapped
// ops run under, in this process. Methods are safe for concurrent use.
type Report struct {
	mu       sync.Mutex
	names    []string
	applied  []int
	rejected []int
}

// Wrap returns a copy of ops whose Apply functions count applications
// and rejections in the returned Report. Everything else about the ops
// — names, weights, When gates, and how they consume the tape — is
// unchanged, so wrapping does not alter the corpus encoding.
func Wrap[S any](ops []fuzztape.Op[S]) ([]fuzztape.Op[S], *Report) {
	r := &Report{
		names:    make([]string, len(ops)),
		applied:  make([]int, len(ops)),
		rejected: make([]int, len(ops)),
	}
	wrapped := make([]fuzztape.Op[S], len(ops))
	for i, op := range ops {
		r.names[i] = op.Name
		apply := op.Apply
		op.Apply = func(s S, t *fuzztape.Tape) error {
			err := apply(s, t)
			r.mu.Lock()
			if err != nil {
				r.rejected[i]++
			} else {
				r.applied[i]++
			}
			r.mu.Unlock()
			return err
		}
		wrapped[i] = op
	}
	return wrapped, r
}

// Missing returns the names of ops that have never been applied,
// whether never selected or always rejected.
func (r *Report) Missing() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var missing []string
	for i, n := range r.applied {
		if n == 0 {
			missing = append(missing, r.names[i])
		}
	}
	return missing
}

// Log writes per-op applied and rejected counts to t and flags ops
// that never applied. It is typically called once after Machine.Run,
// or from a testing.T Cleanup registered in Init.
func (r *Report) Log(t *testing.T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, name := range r.names {
		note := ""
		if r.applied[i] == 0 {
			note = "\tNEVER APPLIED"
		}
		t.Logf("stats: %-20s applied %6d\trejected %6d%s", name, r.applied[i], r.rejected[i], note)
	}
}

// String returns the counts as a single line, for logs outside testing.
func (r *Report) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b strings.Builder
	for i, name := range r.names {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s:%d/%d", name, r.applied[i], r.rejected[i])
	}
	return b.String()
}
