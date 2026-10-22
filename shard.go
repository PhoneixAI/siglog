package siglog

import (
	"runtime"
	"sync/atomic"
)

// Shard is a fixed-size, lock-protected ring buffer. One byte is deliberately
// left unused, so a physical capacity of N can store at most N-1 bytes.
type Shard struct {
	data []byte
	next *Shard

	head uint32 // next byte to read
	tail uint32 // next byte to write
	lock uint32
}

// NewShard creates a ring buffer. Capacity is normalized to at least two bytes
// because one slot is reserved to distinguish full from empty.
func NewShard(capacity int) *Shard {
	if capacity < 2 {
		capacity = 2
	}
	return &Shard{data: make([]byte, capacity)}
}

func (s *Shard) acquire() {
	for !atomic.CompareAndSwapUint32(&s.lock, 0, 1) {
		runtime.Gosched()
	}
}

func (s *Shard) release() {
	atomic.StoreUint32(&s.lock, 0)
}

// push appends all bytes atomically or leaves the shard unchanged. It handles
// wrap-around by splitting the copy into the tail and the start of the array.
func (s *Shard) push(data []byte) bool {
	s.acquire()
	defer s.release()

	if len(data) == 0 {
		return true
	}
	n := len(s.data)
	if n <= 1 || len(data) > available(n, s.tail, s.head) {
		return false
	}

	index := int(s.tail)
	first := copy(s.data[index:], data)
	if first < len(data) {
		copy(s.data, data[first:])
	}
	s.tail = uint32((index + len(data)) % n)
	return true
}

// read returns an owned copy of all currently buffered bytes. It must not
// return a view into s.data: after this method unlocks, a producer may reuse
// those ring slots while the consumer is still writing the bytes to a transport.
func (s *Shard) read() []byte {
	s.acquire()
	defer s.release()

	head, tail := s.head, s.tail
	if head == tail {
		return nil
	}

	n := len(s.data)
	size := 0
	if tail > head {
		size = int(tail - head)
	} else {
		size = n - int(head) + int(tail)
	}

	data := make([]byte, size)
	first := copy(data, s.data[int(head):])
	if first < size {
		copy(data[first:], s.data[:int(tail)])
	}
	s.head = tail
	return data
}

// drainTo copies buffered bytes directly into dst while holding the shard lock.
// Unlike read, it avoids allocating an intermediate slice for Buffer.readBytes.
func (s *Shard) drainTo(dst []byte) []byte {
	s.acquire()
	defer s.release()

	head, tail := s.head, s.tail
	if head == tail {
		return dst
	}
	if tail > head {
		dst = append(dst, s.data[int(head):int(tail)]...)
	} else {
		dst = append(dst, s.data[int(head):]...)
		dst = append(dst, s.data[:int(tail)]...)
	}
	s.head = tail
	return dst
}

// len returns the number of bytes currently buffered under the shard lock.
func (s *Shard) len() int {
	s.acquire()
	defer s.release()
	head, tail := s.head, s.tail
	if tail >= head {
		return int(tail - head)
	}
	return len(s.data) - int(head) + int(tail)
}
