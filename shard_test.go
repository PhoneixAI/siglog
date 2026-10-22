package siglog

import (
	"bytes"
	"fmt"
	"testing"
)

func TestShard_New(t *testing.T) {
	shard := NewShard(16)

	if len(shard.data) != 16 {
		t.Fatalf("expected capacity 16, got %d", len(shard.data))
	}

	if shard.head != 0 {
		t.Fatalf("expected head = 0, got %d", shard.head)
	}

	if shard.tail != 0 {
		t.Fatalf("expected tail = 0, got %d", shard.tail)
	}
}
func TestShard_Push(t *testing.T) {
	shard := NewShard(16)

	input := []byte("hello")

	shard.push(input)

	got := shard.read()

	if !bytes.Equal(got, input) {
		t.Fatalf("expected %q, got %q", input, got)
	}
}
func TestShard_PushMultiple(t *testing.T) {
	shard := NewShard(32)

	shard.push([]byte("hello"))
	shard.push([]byte(" world"))

	got := shard.read()

	expected := []byte("hello world")

	if !bytes.Equal(got, expected) {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}
func TestShard_ReadResets(t *testing.T) {
	shard := NewShard(16)

	shard.push([]byte("hello"))

	got1 := shard.read()

	if !bytes.Equal(got1, []byte("hello")) {
		t.Fatal("unexpected data")
	}

	if shard.head != shard.tail {
		t.Fatalf(
			"expected shard to be empty after read: head=%d tail=%d",
			shard.head,
			shard.tail,
		)
	}
}
func TestShard_PushRead(t *testing.T) {
	shard := NewShard(16)

	shard.push([]byte("hello"))

	got := shard.read()

	expected := []byte("hello")
	if !bytes.Equal(got, expected) {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}
func TestShard_MultiplePushRead(t *testing.T) {
	shard := NewShard(16)

	shard.push([]byte("hello"))
	shard.push([]byte("world"))

	got := shard.read()

	expected := []byte("helloworld")
	if !bytes.Equal(got, expected) {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}
func TestShard_ReadResetsHead(t *testing.T) {
	shard := NewShard(16)

	shard.push([]byte("0123456789"))
	shard.read()

	shard.push([]byte("abc"))

	got := shard.read()

	expected := []byte("abc")
	if !bytes.Equal(got, expected) {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}
func TestShard_FillToEnd(t *testing.T) {
	shard := NewShard(17)

	shard.push([]byte("0123456789abcdef"))

	got := shard.read()

	expected := []byte("0123456789abcdef")
	if !bytes.Equal(got, expected) {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestShard_WrapRead(t *testing.T) {
	shard := NewShard(17)

	shard.push([]byte("0123456789"))
	shard.read()

	// Освободили начало, но tail находится около конца.
	shard.push([]byte("abcdef"))

	got := shard.read()

	expected := []byte("abcdef")
	if !bytes.Equal(got, expected) {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func BenchmarkShardPush(b *testing.B) {
	shard := NewShard(1025)

	data := []byte("hello world")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		shard.push(data)

		// Не даём shard упереться в конец.
		if i%100 == 99 {
			shard.read()
		}
	}
}

func BenchmarkShardReadContiguous(b *testing.B) {
	data := []byte("hello world")

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		shard := NewShard(1025)
		shard.push(data)

		b.StartTimer()
		_ = shard.read()
		b.StopTimer()
	}
}

func BenchmarkShardPushRead(b *testing.B) {
	shard := NewShard(1025)
	data := []byte("hello world")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		shard.push(data)
		_ = shard.read()
	}
}
func BenchmarkShardReadSizes(b *testing.B) {
	sizes := []int{
		8,
		32,
		128,
		512,
		1024,
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			shard := NewShard(2049)
			data := make([]byte, size)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				shard.push(data)
				_ = shard.read()
			}
		})
	}
}
