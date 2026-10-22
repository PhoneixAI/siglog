package siglog

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBufferPushRead(t *testing.T) {
	buffer := NewBuffer(1, 17)

	data := []byte("hello")

	buffer.push(data)

	got := buffer.readBytes()

	if !bytes.Equal(got, data) {
		t.Fatalf("expected %q, got %q", data, got)
	}
}
func TestBufferMultiplePushRead(t *testing.T) {
	buffer := NewBuffer(1, 17)

	buffer.push([]byte("hello"))
	buffer.push([]byte(" world"))

	got := buffer.readBytes()
	expected := []byte("hello world")

	if !bytes.Equal(got, expected) {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}
func TestBufferReadResets(t *testing.T) {
	buffer := NewBuffer(1, 17)

	buffer.push([]byte("hello"))

	first := buffer.readBytes()
	if !bytes.Equal(first, []byte("hello")) {
		t.Fatalf("first read: expected %q, got %q", "hello", first)
	}

	second := buffer.readBytes()
	if len(second) != 0 {
		t.Fatalf("expected empty buffer, got %q", second)
	}
}
func TestBufferShardRing(t *testing.T) {
	buffer := NewBuffer(3, 17)

	if buffer.shards[0].next != buffer.shards[1] {
		t.Fatal("shard 0 should point to shard 1")
	}

	if buffer.shards[1].next != buffer.shards[2] {
		t.Fatal("shard 1 should point to shard 2")
	}

	if buffer.shards[2].next != buffer.shards[0] {
		t.Fatal("shard 2 should point to shard 0")
	}
}

func BenchmarkBufferPush(b *testing.B) {
	buffer := NewBuffer(4, 1025)
	data := []byte("hello world")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer.push(data)

		// Чтобы buffer не забился полностью.
		if i%100 == 99 {
			buffer.readBytes()
		}
	}
}

func BenchmarkBufferRead(b *testing.B) {
	buffer := NewBuffer(4, 1025)
	data := []byte("hello world")

	for i := 0; i < 100; i++ {
		buffer.push(data)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer.readBytes()

		for j := 0; j < 100; j++ {
			buffer.push(data)
		}
	}
}

func BenchmarkBufferPushRead(b *testing.B) {
	buffer := NewBuffer(4, 1025)
	data := []byte("hello world")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buffer.push(data)
		buffer.readBytes()
	}
}
func BenchmarkBufferBatchRead(b *testing.B) {
	for _, batch := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("%d", batch), func(b *testing.B) {
			buffer := NewBuffer(4, 4097)
			data := []byte("hello world")

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for j := 0; j < batch; j++ {
					buffer.push(data)
				}

				buffer.readBytes()
			}
		})
	}
}
func BenchmarkBufferBatchReadSizes(b *testing.B) {
	sizes := []int{
		16,
		64,
		256,
		1024,
		4096,
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			buffer := NewBuffer(4, 131073)
			data := make([]byte, size)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for j := 0; j < 100; j++ {
					buffer.push(data)
				}

				buffer.readBytes()
			}
		})
	}
}
