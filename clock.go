package siglog

import (
	"sync"
	"sync/atomic"
	"time"
)

// Clock caches a formatted wall-clock timestamp. It refreshes every millisecond
// and should be closed when no longer used. The cached bytes are immutable and
// never exposed directly to callers.
type Clock struct {
	value atomic.Value // stores []byte; stored slices are immutable
	stop  chan struct{}
	done  chan struct{}
	once  sync.Once
}

// NewClock starts a timestamp cache using layout, following time.Time.Format.
// Close stops its background goroutine. An empty layout is passed to Format as-is.
func NewClock(layout string) *Clock {
	c := &Clock{stop: make(chan struct{}), done: make(chan struct{})}
	c.value.Store([]byte(time.Now().Format(layout)))
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		defer close(c.done)
		for {
			select {
			case <-c.stop:
				return
			case <-ticker.C:
				// Never mutate a stored slice. atomic.Value publishes a fresh value.
				c.value.Store([]byte(time.Now().Format(layout)))
			}
		}
	}()
	return c
}

// newClock preserves the original package-internal constructor name.
func newClock(layout string) *Clock { return NewClock(layout) }

// TimeBytes returns a copy of the latest cached timestamp. The returned bytes
// may be modified safely by the caller without affecting other readers.
func (c *Clock) TimeBytes() []byte {
	if c == nil {
		return nil
	}
	value, _ := c.value.Load().([]byte)
	return append([]byte(nil), value...)
}

// AppendTime appends the latest cached timestamp to dst. It avoids allocating
// a separate slice when dst already has enough capacity.
func (c *Clock) AppendTime(dst []byte) []byte {
	if c == nil {
		return dst
	}
	value, _ := c.value.Load().([]byte)
	return append(dst, value...)
}

// Close stops the refresh goroutine. It is safe to call more than once.
func (c *Clock) Close() {
	if c == nil {
		return
	}
	c.once.Do(func() { close(c.stop) })
	<-c.done
}
