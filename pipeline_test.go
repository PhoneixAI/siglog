package siglog

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

type testTransport struct {
	mu   sync.Mutex
	data []byte
}

func (t *testTransport) Write(data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.data = append(t.data, data...)
	return nil
}

func (t *testTransport) Close() error {
	return nil
}

func (t *testTransport) Data() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()

	return append([]byte(nil), t.data...)
}

func TestPipelineFlush(t *testing.T) {
	transport := &testTransport{}
	mt := NewTransport(transport)

	pipeline := NewPipeline(
		1,
		17,
		time.Second,
		mt,
	)

	data := []byte("hello")

	err := pipeline.Flush(data)
	if err != nil {
		t.Fatal(err)
	}

	got := transport.Data()

	if !bytes.Equal(got, data) {
		t.Fatalf("expected %q, got %q", data, got)
	}
}

func TestPipelinePush(t *testing.T) {
	transport := &testTransport{}
	mt := NewTransport(transport)

	pipeline := NewPipeline(
		1,
		17,
		time.Second,
		mt,
	)

	data := []byte("hello")

	pipeline.Push(data)

	got := pipeline.buffer.readBytes()

	if !bytes.Equal(got, data) {
		t.Fatalf("expected %q, got %q", data, got)
	}
}

func TestPipelineStart(t *testing.T) {
	transport := &testTransport{}
	mt := NewTransport(transport)

	pipeline := NewPipeline(
		1,
		17,
		10*time.Millisecond,
		mt,
	)

	pipeline.Push([]byte("hello"))

	go pipeline.Start()
	time.Sleep(30 * time.Millisecond)

	got := transport.Data()

	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("expected %q, got %q", "hello", got)
	}
	if err := pipeline.Close(); err != nil {
		t.Fatal(err)
	}
}

type benchTransport struct{}

func (t *benchTransport) Write(data []byte) error {
	return nil
}

func (t *benchTransport) Close() error {
	return nil
}

func BenchmarkPipelinePush(b *testing.B) {
	transport := NewTransport(&benchTransport{})
	pipeline := NewPipeline(64, 4097, time.Hour, transport)
	data := []byte("hello world")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pipeline.Push(data); err != nil {
			b.Fatal(err)
		}
		// Keep this benchmark focused on Push, not on the historical overflow
		// policy, by draining periodically outside the timed interval.
		if i%1000 == 999 {
			b.StopTimer()
			if err := pipeline.FlushBuffer(); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
	}
	b.StopTimer()
	if err := pipeline.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPipelineFlush(b *testing.B) {
	transport := NewTransport(&benchTransport{})

	pipeline := NewPipeline(
		4,
		4097,
		time.Second,
		transport,
	)

	data := []byte("hello world")

	for i := 0; i < 100; i++ {
		pipeline.Push(data)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pipeline.Flush(pipeline.buffer.readBytes())

		for j := 0; j < 100; j++ {
			pipeline.Push(data)
		}
	}
}

func BenchmarkPipelineBatch(b *testing.B) {
	transport := NewTransport(&benchTransport{})

	pipeline := NewPipeline(
		4,
		4097,
		time.Second,
		transport,
	)

	data := []byte("hello world")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			pipeline.Push(data)
		}

		pipeline.Flush(pipeline.buffer.readBytes())
	}
}
