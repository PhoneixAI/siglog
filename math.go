package siglog

// caseBit reports whether w is behind r in the unsigned ring-index domain.
// Kept for internal experiments and compatibility with the original layout.
func caseBit(w, r uint32) uint32 {
	return (w - r) >> 31
}

// available returns the number of writable bytes in a ring buffer that
// reserves one byte to distinguish the full state from the empty state.
// w is the write index and r is the read index.
func available(n int, w, r uint32) int {
	if n <= 1 {
		return 0
	}
	if w >= r {
		return n - int(w-r) - 1
	}
	return int(r-w) - 1
}

// startIndex returns the current write position. The read index is not needed
// when a one-byte sentinel is used to distinguish full and empty states.
func startIndex(_ int, w, _ uint32) uint32 {
	return w
}
