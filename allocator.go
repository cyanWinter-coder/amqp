package amqp

import (
	"bytes"
	"fmt"
	"math/big"
)

const (
	free      = 0
	allocated = 1
)

// allocator maintains a bitset of allocated numbers.
//
// Every successful allocation is also assigned a monotonically increasing
// generation.  The generation is stable for the lifetime of the allocation and
// is never reused, so an id that is released and allocated again is
// distinguishable from the previous owner of the same id.
type allocator struct {
	pool *big.Int
	last int
	low  int
	high int

	// generation is the most recently issued generation.  It only advances on
	// allocation, never on release, so releasing an id twice (or releasing an
	// id that is no longer allocated) cannot advance or rewind it.
	generation uint64

	// generations records the generation of the current allocation of an id.
	// An entry is removed when the allocation is released.
	generations map[int]uint64
}

// NewAllocator reserves and frees integers out of a range between low and
// high.
//
// O(N) worst case space used, where N is maximum allocated, divided by
// sizeof(big.Word)
func newAllocator(low, high int) *allocator {
	return &allocator{
		pool:        big.NewInt(0),
		last:        low,
		low:         low,
		high:        high,
		generations: make(map[int]uint64),
	}
}

// String returns a string describing the contents of the allocator like
// "allocator[low..high] reserved..until"
//
// O(N) where N is high-low
func (a allocator) String() string {
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "allocator[%d..%d]", a.low, a.high)

	for low := a.low; low <= a.high; low++ {
		high := low
		for a.reserved(high) && high <= a.high {
			high++
		}

		if high > low+1 {
			fmt.Fprintf(b, " %d..%d", low, high-1)
		} else if high > low {
			fmt.Fprintf(b, " %d", high-1)
		}

		low = high
	}
	return b.String()
}

// Next reserves and returns the next available number out of the range between
// low and high.  If no number is available, false is returned.
//
// O(N) worst case runtime where N is allocated, but usually O(1) due to a
// rolling index into the oldest allocation.
func (a *allocator) next() (int, bool) {
	n, _, ok := a.nextGeneration()
	return n, ok
}

// nextGeneration reserves and returns the next available number together with
// the monotonic generation assigned to that allocation.  Generations are
// unique within an allocator, including across repeated allocations of the
// same id, and only advance on successful allocation.
func (a *allocator) nextGeneration() (int, uint64, bool) {
	n, ok := a.nextID()
	if !ok {
		return 0, 0, false
	}

	// Refuse to wrap: a repeated generation would make an old owner of an id
	// look current again.  This cannot be reached in practice, but treating
	// it as exhaustion keeps generations strictly increasing.
	if a.generation == ^uint64(0) {
		a.pool.SetBit(a.pool, n-a.low, free)
		return 0, 0, false
	}

	a.generation++
	if a.generations == nil {
		a.generations = make(map[int]uint64)
	}
	a.generations[n] = a.generation

	return n, a.generation, true
}

// nextID reserves and returns the next available number out of the range
// between low and high.  If no number is available, false is returned.
func (a *allocator) nextID() (int, bool) {
	wrapped := a.last

	// Find trailing bit
	for ; a.last <= a.high; a.last++ {
		if a.reserve(a.last) {
			return a.last, true
		}
	}

	// Find preceding free'd pool
	a.last = a.low

	for ; a.last < wrapped; a.last++ {
		if a.reserve(a.last) {
			return a.last, true
		}
	}

	return 0, false
}

// reserve claims the bit if it is not already claimed, returning true if
// successfully claimed.
func (a *allocator) reserve(n int) bool {
	if n < a.low || n > a.high || a.reserved(n) {
		return false
	}
	a.pool.SetBit(a.pool, n-a.low, allocated)
	return true
}

// reserved returns true if the integer has been allocated
func (a *allocator) reserved(n int) bool {
	// Reject out of range ids before indexing the bitset: big.Int.Bit panics
	// on a negative index and would silently report a bit beyond high.
	if n < a.low || n > a.high {
		return false
	}
	return a.pool.Bit(n-a.low) == allocated
}

// release frees the use of the number for another allocation.
//
// Releasing an id that is not currently allocated is a no-op: it neither
// advances the generation nor touches the bit set (which would otherwise index
// negative for out-of-range ids).  This makes release idempotent so a repeated
// release cannot free a newer allocation of the same id.
func (a *allocator) release(n int) {
	if n < a.low || n > a.high || !a.reserved(n) {
		return
	}
	delete(a.generations, n)
	a.pool.SetBit(a.pool, n-a.low, free)
}
