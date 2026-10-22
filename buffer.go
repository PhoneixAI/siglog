package siglog

import (
	"math/rand/v2"
	"sync/atomic"
)

// Buffer distributes records across a fixed ring of shards. The ring of shards
// is intentionally retained; each Shard owns its byte ring and its lock.
type Buffer struct {
	shards     []*Shard
	shardCount int
	shardCap   int
	trackLen   atomic.Bool
	used       atomic.Int64
}

// NewBuffer creates a sharded buffer. Each shard reserves one byte, so its
// writable capacity is shardCapacity-1. Invalid small inputs are normalized.
func NewBuffer(shardCount int, shardCapacity int) *Buffer {
	if shardCount < 1 {
		shardCount = 1
	}
	if shardCapacity < 2 {
		shardCapacity = 2
	}

	data := make([]*Shard, shardCount)
	for i := range data {
		data[i] = NewShard(shardCapacity)
	}
	for i := range data {
		data[i].next = data[(i+1)%shardCount]
	}
	return &Buffer{
		shards:     data,
		shardCount: shardCount,
		shardCap:   shardCapacity,
	}
}

// push appends a record. A record larger than one shard's usable capacity is
// rejected. The historical overflow policy (drain when all attempted shards
// are full) is intentionally unchanged for now.
func (b *Buffer) push(data []byte) bool {
	if len(data) > b.shardCap-1 {
		return false
	}
	if len(data) == 0 {
		return true
	}

	for {
		index := rand.IntN(b.shardCount)
		shard := b.shards[index]
		if shard.push(data) {
			b.addUsed(len(data))
			return true
		}
		if shard.next.push(data) {
			b.addUsed(len(data))
			return true
		}
		if shard.next.next.push(data) {
			b.addUsed(len(data))
			return true
		}

		// Preserve the existing overflow behavior for this iteration of the
		// project. This policy can discard the drained batch and is documented
		// as a known issue to resolve separately.
		b.readBytes()
	}
}

// SetTrackLen enables the aggregate byte counter used by size-triggered
// flushing. Configure it before concurrent writes. When disabled, Len scans
// shards on demand and writes avoid a shared atomic update on every record.
func (b *Buffer) SetTrackLen(enabled bool) {
	if b == nil {
		return
	}
	if !enabled {
		b.trackLen.Store(false)
		b.used.Store(0)
		return
	}
	var total int64
	for _, shard := range b.shards {
		total += int64(shard.len())
	}
	b.used.Store(total)
	b.trackLen.Store(true)
}

func (b *Buffer) addUsed(n int) {
	if n != 0 && b.trackLen.Load() {
		b.used.Add(int64(n))
	}
}

// Len returns the approximate number of buffered bytes. With size tracking
// enabled it is an O(1) atomic read; otherwise it scans the shards.
func (b *Buffer) Len() int {
	if b == nil {
		return 0
	}
	if b.trackLen.Load() {
		used := b.used.Load()
		if used < 0 { // concurrent drain/push accounting can briefly cross zero
			return 0
		}
		return int(used)
	}
	var total int
	for _, shard := range b.shards {
		total += shard.len()
	}
	return total
}

func (b *Buffer) readBytes() []byte {
	// Allocate for the current batch, not the entire configured ring capacity.
	// A high-capacity buffer may be several megabytes while most flushes are small.
	track := b.trackLen.Load()
	data := make([]byte, 0, b.Len())
	for _, shard := range b.shards {
		before := len(data)
		data = shard.drainTo(data)
		if track {
			if readSize := len(data) - before; readSize != 0 {
				b.used.Add(-int64(readSize))
			}
		}
	}
	return data
}
