package fuzztape

// A Gen decodes a value of type T from a tape. A generator is a plain
// function: composite generators are written as func literals calling
// other generators, and every Gen bottoms out in Tape reads, so the
// tape's decoding and shrinking properties hold for generated values
// automatically.
type Gen[T any] func(*Tape) T

// Const returns a generator that always yields v.
func Const[T any](v T) Gen[T] {
	return func(*Tape) T { return v }
}

// IntRange returns a generator of integers in [lo, hi].
// It panics if hi < lo.
func IntRange(lo, hi int) Gen[int] {
	if hi < lo {
		panic("fuzztape: IntRange called with hi < lo")
	}
	return func(t *Tape) int { return lo + t.IntN(hi-lo+1) }
}

// OneOf returns a generator that defers to one of gens.
// It panics if gens is empty.
func OneOf[T any](gens ...Gen[T]) Gen[T] {
	if len(gens) == 0 {
		panic("fuzztape: OneOf called with no generators")
	}
	return func(t *Tape) T { return gens[t.IntN(len(gens))](t) }
}

// Map returns a generator that yields f applied to g's values.
func Map[A, B any](g Gen[A], f func(A) B) Gen[B] {
	return func(t *Tape) B { return f(g(t)) }
}

// SliceOf returns a generator of slices of up to max values of g.
func SliceOf[T any](g Gen[T], max int) Gen[[]T] {
	return func(t *Tape) []T {
		out := make([]T, t.IntN(max+1))
		for i := range out {
			out[i] = g(t)
		}
		return out
	}
}
