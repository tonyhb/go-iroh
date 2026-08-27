package fuzztape

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

var seedFlag = flag.Int64("fuzztape.seed", 0, "seed for fuzztape Machine.Run case generation (0 derives one)")

// An Op is one operation of a stateful property test.
type Op[S any] struct {
	// Name labels the op in failure logs.
	Name string
	// Weight is the op's relative selection frequency; 0 means 1.
	Weight int
	// When reports whether the op is currently applicable;
	// nil means always. Ops with a false When are not selected.
	When func(s S) bool
	// Apply performs the op, drawing parameters from the tape.
	// A non-nil error rejects the op — the sequence continues — and
	// must not be used for failures; fail with t.Fatalf in Check or
	// via the *testing.T captured by Init.
	Apply func(s S, t *Tape) error
}

// A Machine describes a stateful property test: a system under test, a
// set of operations, and an invariant. Each input decodes to a bounded
// operation sequence; the invariant is checked after every applied op.
type Machine[S any] struct {
	// Init returns a fresh system under test.
	Init func(t *testing.T) S
	// Ops is the operation set. It must be non-empty, and its order
	// is part of the corpus encoding: reordering ops changes how
	// previously saved inputs decode.
	Ops []Op[S]
	// Check asserts the invariant, failing with t.Fatalf.
	Check func(t *testing.T, s S)
	// MaxOps bounds the ops decoded per input; 0 means 64. It is only an
	// upper bound: a sequence also ends when the input runs out, which
	// for short inputs happens well before MaxOps.
	MaxOps int
	// Name, if set, is the fuzz target name (e.g. "FuzzStreamMachine")
	// under which Run saves shrunk failing inputs to testdata/fuzz/,
	// so a failure found by Run becomes a seed input replayed by both
	// Run and the fuzz target.
	Name string
	// Bubble runs each input's op sequence inside a testing/synctest
	// bubble: time is virtual, and the bubble's exit check reports any
	// goroutine the sequence left durably blocked, making every case a
	// goroutine-leak check. Ops must not depend on real time or on
	// goroutines started outside the bubble.
	//
	// Init runs inside the bubble, so a goroutine it starts must be
	// stopped inside the bubble too — by an op, or by a cleanup Init
	// registers on the *testing.T it is passed, which the bubble runs
	// before its exit check. A goroutine stopped on the outer test's
	// cleanup is still blocked at exit and fails every case.
	//
	// A leak is reported with the stacks of the goroutines left blocked,
	// and shrinks like any other failure. The goroutines themselves stay
	// blocked for the rest of the run, in a bubble nothing else can
	// reach.
	Bubble bool
}

// Fuzz registers the machine as the fuzz function of f. Corpus files,
// -fuzztime budgets, minimization, and seeds via f.Add behave as for
// any other fuzz target.
func (m Machine[S]) Fuzz(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		m.runTape(t, data, true, nil)
	})
}

// Run checks the machine against iters pseudo-random inputs inside an
// ordinary test; iters <= 0 means 100. The seed is printed and can be
// pinned with -fuzztape.seed. On failure Run shrinks the failing input,
// logs the minimal op sequence, and (if Name is set) saves the input to
// testdata/fuzz/ for permanent replay.
func (m Machine[S]) Run(t *testing.T, iters int) {
	if iters <= 0 {
		iters = 100
	}
	seed := *seedFlag
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	t.Logf("fuzztape: seed %d (rerun with -fuzztape.seed=%d)", seed, seed)
	rng := rand.New(rand.NewSource(seed))
	for i := range iters {
		data := make([]byte, 16+rng.Intn(496))
		rng.Read(data)
		if t.Run(fmt.Sprintf("case%04d", i), func(t *testing.T) { m.runTape(t, data, true, nil) }) {
			continue
		}
		data = m.shrink(t, data)
		t.Logf("fuzztape: shrunk failing input to %d bytes: %x", len(data), data)
		if m.Name != "" {
			if path, err := writeCorpusFile(m.Name, data); err != nil {
				t.Logf("fuzztape: save corpus file: %v", err)
			} else {
				t.Logf("fuzztape: saved failing input to %s", path)
			}
		}
		return
	}
}

// runTape runs one input, inside a synctest bubble if Bubble is set,
// and logs the op sequence if the input fails. The bubble goes here
// rather than around Run or Fuzz because both run each case as a
// subtest, and t.Run panics inside a bubble.
//
// A bubble reports a goroutine the sequence left blocked by panicking
// out of synctest.Test, after the sequence itself has returned. Turning
// that back into an ordinary failure is what keeps the rest of the
// package working on it: without the recover the test binary dies on
// the spot, with no shrinking, no op sequence, and no corpus file.
// Recovering is safe because the blocked goroutines stay in their
// abandoned bubble, where they can no longer affect later inputs. The
// stacks are worth printing only for the input Run failed on, not for
// the shrink attempts that follow, which by then are reporting the
// leaks of every attempt before them.
func (m Machine[S]) runTape(t *testing.T, data []byte, stacks bool, splits *[]int) {
	var applied []string
	defer func() {
		if m.Bubble {
			if r := recover(); r != nil {
				if stacks {
					t.Errorf("fuzztape: %v\n\n%s", r, bubbleStacks())
				} else {
					t.Errorf("fuzztape: %v", r)
				}
			}
		}
		if t.Failed() {
			t.Logf("fuzztape: op sequence (%d ops):\n\t%s", len(applied), strings.Join(applied, "\n\t"))
		}
	}()
	if !m.Bubble {
		m.runOps(t, data, &applied, splits)
		return
	}
	synctest.Test(t, func(t *testing.T) { m.runOps(t, data, &applied, splits) })
}

// Splits reports the tape offsets at which the ops decoded from data
// begin, by replaying data against a fresh system in a subtest of t.
// The final element is the offset just past the last byte the sequence
// consumed, so data[splits[i]:splits[i+1]] holds the bytes of op i.
// Cutting or splicing inputs at these offsets edits whole operations.
//
// The replay applies the sequence for real: Init runs, ops run, and if
// the input fails the machine the failure is reported on the subtest.
// Under Bubble each replay that fails its exit check leaves another
// set of blocked goroutines behind in an abandoned bubble — bounded
// and harmless, as for shrink attempts, but visible in goroutine
// dumps.
func (m Machine[S]) Splits(t *testing.T, data []byte) []int {
	var splits []int
	t.Run("splits", func(t *testing.T) { m.runTape(t, data, false, &splits) })
	return splits
}

// bubbleStacks returns the stacks of the goroutines running in a
// synctest bubble, which for a bubble that just failed its exit check
// are the goroutines it left blocked.
func bubbleStacks() string {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var keep []string
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(g, "synctest bubble") {
			keep = append(keep, g)
		}
	}
	return strings.Join(keep, "\n\n")
}

// runOps decodes data into one operation sequence and applies it,
// checking the invariant after every applied op. It appends the name of
// each op it applies to *applied, which outlives it so that the caller
// can report the sequence even when the failure surfaces after runOps
// has returned. If splits is non-nil it records each op's starting tape
// offset, plus one final offset past the last byte consumed.
func (m Machine[S]) runOps(t *testing.T, data []byte, applied *[]string, splits *[]int) {
	if len(m.Ops) == 0 {
		t.Fatal("fuzztape: Machine has no Ops")
	}
	tape := New(data)
	if splits != nil {
		defer func() { *splits = append(*splits, tape.Pos()) }()
	}
	s := m.Init(t)
	maxOps := m.MaxOps
	if maxOps <= 0 {
		maxOps = 64
	}
	for range maxOps {
		if tape.Done() {
			return
		}
		enabled := make([]*Op[S], 0, len(m.Ops))
		total := 0
		for i := range m.Ops {
			op := &m.Ops[i]
			if op.When == nil || op.When(s) {
				enabled = append(enabled, op)
				total += max(op.Weight, 1)
			}
		}
		if len(enabled) == 0 {
			return
		}
		if splits != nil {
			*splits = append(*splits, tape.Pos())
		}
		w := tape.IntN(total)
		var op *Op[S]
		for _, o := range enabled {
			w -= max(o.Weight, 1)
			if w < 0 {
				op = o
				break
			}
		}
		if err := op.Apply(s, tape); err != nil {
			*applied = append(*applied, op.Name+" (rejected: "+err.Error()+")")
			continue
		}
		*applied = append(*applied, op.Name)
		if m.Check != nil {
			m.Check(t, s)
		}
	}
}

// shrink reduces a failing input while it keeps failing: by truncation,
// then by deleting the bytes of whole middle ops, then by bisecting
// individual bytes toward zero. Because reads past the end of the input
// and zero bytes both decode to the simplest choice, byte-level edits
// shrink the decoded op sequence and its values. Attempts run as
// subtests of t (which is already failing).
func (m Machine[S]) shrink(t *testing.T, data []byte) []byte {
	attempts := 0
	fails := func(d []byte) bool {
		attempts++
		return !t.Run(fmt.Sprintf("shrink%03d", attempts), func(t *testing.T) { m.runTape(t, d, false, nil) })
	}

	// Truncation: halve, then trim single bytes.
	for len(data) > 0 && attempts < 200 {
		cut := data[:len(data)/2]
		if !fails(cut) {
			break
		}
		data = cut
	}
	for len(data) > 0 && attempts < 200 && fails(data[:len(data)-1]) {
		data = data[:len(data)-1]
	}

	// Chunk deletion: drop whole middle ops, last to first, so a
	// failure needing only its first and final ops shrinks past what
	// truncation alone can reach. Deleting a chunk changes how the rest
	// decodes, so the op boundaries are recomputed after each success.
	// The boundary replay itself runs as a shrink attempt. This phase
	// gets its own budget rather than the tail of truncation's: it is
	// the one that reaches failures truncation cannot.
	for stop := attempts + 80; attempts < stop; {
		var splits []int
		attempts++
		t.Run(fmt.Sprintf("shrink%03d", attempts), func(t *testing.T) { m.runTape(t, data, false, &splits) })
		deleted := false
		for i := len(splits) - 2; i >= 0 && attempts < stop; i-- {
			cut := slices.Concat(data[:splits[i]], data[splits[i+1]:])
			if len(cut) < len(data) && fails(cut) {
				data = cut
				deleted = true
				break
			}
		}
		if !deleted {
			break
		}
	}

	// Value bisection: walk each byte toward zero while the input
	// keeps failing, trying zero first and then repeated halving down
	// to 1 (zero itself was just proven to pass, so halving stops
	// above it).
	stop := attempts + 100
	for i := 0; i < len(data) && attempts < stop; i++ {
		try := func(v byte) bool {
			edited := slices.Clone(data)
			edited[i] = v
			if fails(edited) {
				data = edited
				return true
			}
			return false
		}
		if data[i] == 0 || try(0) {
			continue
		}
		for data[i] > 1 && attempts < stop && try(data[i]/2) {
		}
	}
	return data
}

// writeCorpusFile saves data as a seed corpus file for the named fuzz
// target, in the format go test replays from testdata/fuzz.
func writeCorpusFile(target string, data []byte) (string, error) {
	dir := filepath.Join("testdata", "fuzz", target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	content := fmt.Sprintf("go test fuzz v1\n[]byte(%q)\n", data)
	path := filepath.Join(dir, fmt.Sprintf("fuzztape-%x", sha256.Sum256(data))[:24])
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", err
	}
	return path, nil
}
