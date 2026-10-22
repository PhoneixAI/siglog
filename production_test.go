package siglog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newNoopLogger(t *testing.T) *Logger {
	t.Helper()
	logger, err := New(WithTransport(&benchTransport{}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := logger.Close(); err != nil {
			t.Errorf("logger Close: %v", err)
		}
	})
	return logger
}

func TestNewBufferProfilesAndManualConfiguration(t *testing.T) {
	t.Run("balanced default profile", func(t *testing.T) {
		logger := newNoopLogger(t)
		if logger.pipeline.buffer.shardCount != 8 || logger.pipeline.buffer.shardCap != 64*KB {
			t.Fatalf("unexpected defaults: shards=%d capacity=%d", logger.pipeline.buffer.shardCount, logger.pipeline.buffer.shardCap)
		}
		if logger.pipeline.timeout != 250*time.Millisecond || logger.pipeline.flushSize.Load() != int64(64*KB) {
			t.Fatalf("unexpected default flush settings: interval=%s size=%d", logger.pipeline.timeout, logger.pipeline.flushSize.Load())
		}
	})

	t.Run("low memory profile", func(t *testing.T) {
		logger, err := New(WithBufferProfile(ProfileLowMemory), WithTransport(&benchTransport{}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := logger.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
		if logger.pipeline.buffer.shardCount != 4 || logger.pipeline.buffer.shardCap != 16*KB {
			t.Fatalf("unexpected low-memory profile: shards=%d capacity=%d", logger.pipeline.buffer.shardCount, logger.pipeline.buffer.shardCap)
		}
		if logger.pipeline.timeout != 100*time.Millisecond || logger.pipeline.flushSize.Load() != int64(8*KB) {
			t.Fatalf("unexpected low-memory flush settings: interval=%s size=%d", logger.pipeline.timeout, logger.pipeline.flushSize.Load())
		}
	})

	t.Run("high throughput profile", func(t *testing.T) {
		logger, err := New(WithBufferProfile(ProfileHighThroughput), WithTransport(&benchTransport{}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := logger.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
		if logger.pipeline.buffer.shardCount != 32 || logger.pipeline.buffer.shardCap != 256*KB {
			t.Fatalf("unexpected high-throughput profile: shards=%d capacity=%d", logger.pipeline.buffer.shardCount, logger.pipeline.buffer.shardCap)
		}
		if got := logger.pipeline.flushSize.Load(); got != int64(256*KB) {
			t.Fatalf("unexpected profile flush size: %d", got)
		}
	})

	t.Run("manual settings", func(t *testing.T) {
		logger, err := New(
			WithBuffer(BufferConfig{ShardCount: 3, ShardCapacity: 4096}),
			WithFlushSize(1024),
			WithFlushInterval(0),
			WithTransport(&benchTransport{}),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := logger.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
		if logger.pipeline.buffer.shardCount != 3 || logger.pipeline.buffer.shardCap != 4096 {
			t.Fatalf("unexpected manual buffer: shards=%d capacity=%d", logger.pipeline.buffer.shardCount, logger.pipeline.buffer.shardCap)
		}
		if logger.pipeline.timeout != 0 || logger.pipeline.flushSize.Load() != 1024 {
			t.Fatalf("unexpected manual flush settings: interval=%s size=%d", logger.pipeline.timeout, logger.pipeline.flushSize.Load())
		}
	})
}

func TestNewWithFieldsOptionEncodesContextFields(t *testing.T) {
	sink := &testTransport{}
	logger, err := New(WithFields(String("service", "checkout")), WithTransport(sink))
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("ready", Int("port", 8080))
	if err := logger.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	data := sink.Data()
	record, size, err := decodeRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	if size != len(data) || record.message != "ready" || len(record.fields) != 2 {
		t.Fatalf("unexpected decoded record: size=%d bytes=%d record=%#v", size, len(data), record)
	}
	if record.fields[0].key != "service" || record.fields[0].value != `"checkout"` {
		t.Fatalf("context field missing from record: %#v", record.fields)
	}
}

func TestNewRejectsConflictingOrInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
	}{
		{name: "profile and manual buffer", opts: []Option{WithBufferProfile(ProfileHighThroughput), WithBuffer(BufferConfig{ShardCount: 2, ShardCapacity: 128})}},
		{name: "invalid shard count", opts: []Option{WithBuffer(BufferConfig{ShardCount: 0, ShardCapacity: 128})}},
		{name: "invalid flush interval", opts: []Option{WithFlushInterval(-time.Second)}},
		{name: "flush threshold exceeds capacity", opts: []Option{WithBuffer(BufferConfig{ShardCount: 1, ShardCapacity: 1024}), WithFlushSize(2048)}},
		{name: "nil transport", opts: []Option{WithTransport(nil)}},
		{name: "unknown profile", opts: []Option{WithBufferProfile(BufferProfile(99))}},
		{name: "invalid retry policy", opts: []Option{WithRetryPolicy(RetryPolicy{MaxAttempts: 0})}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := New(tt.opts...)
			if err == nil {
				if logger != nil {
					_ = logger.Close()
				}
				t.Fatal("expected configuration error")
			}
		})
	}
}

func TestShardSentinelCapacityAndWraparound(t *testing.T) {
	shard := NewShard(8) // one byte is reserved; seven bytes are usable
	if !shard.push([]byte("1234567")) {
		t.Fatal("expected N-1 bytes to fit")
	}
	if shard.push([]byte("x")) {
		t.Fatal("expected full ring to reject another byte")
	}
	if got := string(shard.read()); got != "1234567" {
		t.Fatalf("full read: got %q", got)
	}
	if !shard.push([]byte("123456")) {
		t.Fatal("expected first wrapped-test push to fit")
	}
	if got := string(shard.read()); got != "123456" {
		t.Fatalf("first read: got %q", got)
	}
	if !shard.push([]byte("abcd")) {
		t.Fatal("expected wrapped push to fit")
	}
	if got := string(shard.read()); got != "abcd" {
		t.Fatalf("wrapped read: got %q, want abcd", got)
	}
}

func TestShardReadReturnsOwnedMemory(t *testing.T) {
	shard := NewShard(16)
	if !shard.push([]byte("before")) {
		t.Fatal("push failed")
	}
	read := shard.read()
	if !shard.push([]byte("after!")) {
		t.Fatal("second push failed")
	}
	if got := string(read); got != "before" {
		t.Fatalf("read buffer was mutated after unlock: %q", got)
	}
}

func TestShardConcurrentPushReadRace(t *testing.T) {
	shard := NewShard(1024)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			data := []byte(fmt.Sprintf("%08d", i))
			for !shard.push(data) {
				_ = shard.read()
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			_ = shard.read()
		}
	}()
	wg.Wait()
	_ = shard.read()
}

type retryTransport struct {
	failUntil int32
	attempts  atomic.Int32
	writes    atomic.Int32
	closed    atomic.Int32
}

func (t *retryTransport) Write(data []byte) error {
	attempt := t.attempts.Add(1)
	if attempt <= t.failUntil {
		return errors.New("temporary test failure")
	}
	t.writes.Add(1)
	return nil
}
func (t *retryTransport) Close() error { t.closed.Add(1); return nil }

func TestTransportRetriesAndCloseIsIdempotent(t *testing.T) {
	sink := &retryTransport{failUntil: 2}
	group := NewTransport(sink)
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	if err := group.Write([]byte("batch")); err != nil {
		t.Fatal(err)
	}
	if sink.attempts.Load() != 3 || sink.writes.Load() != 1 {
		t.Fatalf("attempts=%d successful writes=%d", sink.attempts.Load(), sink.writes.Load())
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	if sink.closed.Load() != 1 {
		t.Fatalf("transport closed %d times", sink.closed.Load())
	}
	if err := group.Write([]byte("after close")); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("expected ErrTransportClosed, got %v", err)
	}
}

func TestHTTPInvalidURLIsPermanent(t *testing.T) {
	transport := NewHTTPTransport("ftp://example.com/logs", nil)
	err := transport.Write([]byte("batch"))
	if err == nil {
		t.Fatal("expected URL validation error")
	}
	if retryable(err) {
		t.Fatalf("invalid URL should not be retried: %v", err)
	}
}

func TestHTTPTransportReturnsStatusErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("backend unavailable"))
	}))
	defer server.Close()

	transport := NewHTTPTransport(server.URL, server.Client())
	group := NewTransport(transport)
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	err := group.Write([]byte("batch"))
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %v", err)
	}
	if statusErr.StatusCode != http.StatusInternalServerError || !strings.Contains(err.Error(), "backend unavailable") {
		t.Fatalf("unexpected status error: %v", err)
	}
	_ = group.Close()
}

func TestHTTPTransportDoesNotRetryClientErrors(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	group := NewTransport(NewHTTPTransport(server.URL, server.Client()))
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 4}); err != nil {
		t.Fatal(err)
	}
	err := group.Write([]byte("invalid request"))
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected HTTP 400 status error, got %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("HTTP client error retried %d times, want once", got)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPTransportRetriesTransientStatuses(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	group := NewTransport(NewHTTPTransport(server.URL, server.Client()))
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	if err := group.Write([]byte("batch")); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("HTTP requests=%d, want 3", got)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileTransportCloseIsIdempotent(t *testing.T) {
	path := t.TempDir() + "/siglog-test.bin"
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	transport := NewFileTransport(file, SyncNever)
	if err := transport.Write([]byte("binary batch")); err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "binary batch" {
		t.Fatalf("file contents %q", got)
	}
}

func TestReceiveInfoReportsItsCaller(t *testing.T) {
	_, wantFile, line, ok := runtime.Caller(0)
	info := ReceiveInfo()
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	if info.File() != wantFile || info.Line() != line+1 {
		t.Fatalf("ReceiveInfo got %s:%d, want %s:%d", info.File(), info.Line(), wantFile, line+1)
	}
	if !strings.HasSuffix(info.String(), "production_test.go:"+fmt.Sprint(line+1)) {
		t.Fatalf("unexpected CallInfo.String(): %q", info.String())
	}
}

func TestMalformedRecordHasStableSentinel(t *testing.T) {
	console := NewConsoleTransport()
	var output bytes.Buffer
	console.WithWriter(&output)
	err := console.Write([]byte("bad"))
	if !errors.Is(err, ErrMalformedRecord) {
		t.Fatalf("expected ErrMalformedRecord, got %v", err)
	}
	if retryable(err) {
		t.Fatalf("malformed binary data must not be retried: %v", err)
	}
}

func TestConsoleTransportFormatsBinaryRecords(t *testing.T) {
	record, err := encodeRecord(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), levelInfo, "hello", []Field{String("service", "api"), Int("port", 8080)}, "app.go", 12)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	console := NewConsoleTransport().WithWriter(&output).WithColor(true)
	if err := console.Write(record); err != nil {
		t.Fatal(err)
	}
	got := output.String()
	for _, expected := range []string{"[INFO]", "hello", `service="api"`, "port=8080", "app.go:12", "\x1b[32m"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("output %q does not contain %q", got, expected)
		}
	}
	if err := console.Write([]byte("not a binary record")); err == nil {
		t.Fatal("expected malformed-record error")
	}
}

func TestPipelineCloseStopsStartAndFlushesFinalBatch(t *testing.T) {
	sink := &testTransport{}
	pipeline := NewPipeline(2, 128, time.Hour, NewTransport(sink))
	if err := pipeline.Push([]byte("final")); err != nil {
		t.Fatal(err)
	}
	go pipeline.Start()
	if err := pipeline.Close(); err != nil {
		t.Fatal(err)
	}
	if got := string(sink.Data()); got != "final" {
		t.Fatalf("got %q", got)
	}
	if err := pipeline.Push([]byte("late")); !errors.Is(err, ErrPipelineClosed) {
		t.Fatalf("expected closed error, got %v", err)
	}
}

func TestPipelineSizeFlush(t *testing.T) {
	sink := &testTransport{}
	pipeline := NewPipeline(2, 128, 0, NewTransport(sink)).SetFlushSize(5)
	go pipeline.Start()
	if err := pipeline.Push([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(sink.Data()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := string(sink.Data()); got != "hello" {
		t.Fatalf("got %q", got)
	}
	if err := pipeline.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLoggerPublicMethodsAndCaller(t *testing.T) {
	sink := &testTransport{}
	pipeline := NewPipeline(2, 4096, 0, NewTransport(sink))
	logger := NewWithPipeline(pipeline, String("app", "siglog")).EnableStats(true)

	wantFile, wantLine := logFromHelper(logger)
	logger.Info("info", String("k", "v"))
	logger.Warn("warn")
	logger.Error("error", String("error_type", "db_timeout"))
	logger.Debug("debug")
	logger.Fatal("fatal")
	logger.LogInfo("log info")
	logger.LogWarn("log warn")
	logger.LogError("log error", String("error_type", "network"))
	logger.LogDebug("log debug")
	logger.LogFatal("log fatal")
	logger.Infof("info %d", 1)
	logger.Warnf("warn %d", 2)
	logger.Errorf("error %d", 3)
	logger.Debugf("debug %d", 4)
	logger.Fatalf("fatal %d", 5)
	logger.TrackErrorTypes(1)
	logger.Error("other one", String("error_type", "cache"))
	logger.Error("other two", String("error_type", "filesystem"))

	if err := logger.Flush(); err != nil {
		t.Fatal(err)
	}
	data := sink.Data()
	var count int
	firstFile := ""
	var firstLine uint32
	foundCallerRecord := false
	for len(data) > 0 {
		record, size, err := decodeRecord(data)
		if err != nil {
			t.Fatal(err)
		}
		if record.message == "caller check" {
			firstFile, firstLine = record.file, record.line
			foundCallerRecord = true
		}
		count++
		data = data[size:]
	}
	if count != 18 {
		t.Fatalf("decoded %d records, want 18", count)
	}
	if !foundCallerRecord || !strings.HasSuffix(firstFile, wantFile) || firstLine != uint32(wantLine) {
		t.Fatalf("caller got %s:%d, want suffix %s:%d", firstFile, firstLine, wantFile, wantLine)
	}
	stats := logger.Stats()
	if stats.Total != 18 || stats.Info != 4 || stats.Warn != 3 || stats.Error != 5 || stats.Debug != 3 || stats.Fatal != 3 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if stats.ErrorsByType["other"] != 1 {
		t.Fatalf("bounded error-type stats: %#v", stats.ErrorsByType)
	}
	if !strings.Contains(stats.Format(true), "\x1b[1;36m") {
		t.Fatal("expected optional ANSI stats formatting")
	}
	if err := pipeline.Close(); err != nil {
		t.Fatal(err)
	}
}

func logFromHelper(l *Logger) (string, int) {
	_, file, line, _ := runtime.Caller(0)
	l.Info("caller check")
	return file, line + 1
}

func TestDetailedErrorStatsAreOptionalAndBounded(t *testing.T) {
	logger := newNoopLogger(t)
	logger.Error("one", String("error_type", "database"))
	if got := logger.Stats().ErrorsByType; len(got) != 0 {
		t.Fatalf("detailed stats should be disabled: %#v", got)
	}
	logger.TrackErrorTypes(1)
	logger.Error("one", String("error_type", "database"))
	logger.Error("two", String("error_type", "network"))
	logger.Error("three", String("error_type", "filesystem"))
	stats := logger.Stats()
	if stats.ErrorsByType["database"] != 1 || stats.ErrorsByType["other"] != 2 {
		t.Fatalf("unexpected bounded map: %#v", stats.ErrorsByType)
	}
	logger.TrackErrorTypes(0)
	if got := logger.Stats().ErrorsByType; len(got) != 0 {
		t.Fatalf("detailed stats should be disabled: %#v", got)
	}
}

func TestPipelineReturnsErrorAndDoesNotRetainFailedBatch(t *testing.T) {
	sink := &retryTransport{failUntil: 1}
	group := NewTransport(sink)
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	pipeline := NewPipeline(1, 128, 0, group)
	if err := pipeline.Push([]byte("first-batch")); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.FlushBuffer(); err == nil {
		t.Fatal("expected first flush to fail")
	}
	if pipeline.LastError() == nil {
		t.Fatal("expected LastError")
	}

	// A later flush sends only new data; it must not silently replay the failed batch.
	if err := pipeline.Push([]byte("second-batch")); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.FlushBuffer(); err != nil {
		t.Fatal(err)
	}
	if got := sink.attempts.Load(); got != 2 {
		t.Fatalf("transport attempts=%d, want 2 (one per batch)", got)
	}
	if got := sink.writes.Load(); got != 1 {
		t.Fatalf("successful writes=%d, want 1", got)
	}
	if err := pipeline.Close(); err == nil {
		t.Fatal("Close should report the earlier unconfirmed batch failure")
	}
}

func TestPipelineFlushCountsUnconfirmedRecords(t *testing.T) {
	sink := &retryTransport{failUntil: 100}
	group := NewTransport(sink)
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	pipeline := NewPipeline(1, 1024, 0, group)
	record, err := encodeRecord(time.Now(), levelInfo, "direct flush", nil, "test.go", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := pipeline.Flush(record); err == nil {
		t.Fatal("expected direct flush to report transport error")
	}
	if got := pipeline.unconfirmedRecords.Load(); got != 1 {
		t.Fatalf("unconfirmed records=%d, want 1", got)
	}
	if err := pipeline.Close(); err == nil {
		t.Fatal("Close should preserve the previous flush error")
	}
}

func TestLoggerRateLimitAndOversizedRecord(t *testing.T) {
	sink := &testTransport{}
	pipeline := NewPipeline(1, 1024, 0, NewTransport(sink))
	logger := NewWithPipeline(pipeline).EnableStats(true).LimitRate(1, time.Minute)
	logger.Info("first")
	logger.Info("rate-limited")
	logger.Info(strings.Repeat("x", 2000))
	stats := logger.Stats()
	if stats.Total != 3 || stats.RateLimited != 2 || stats.Dropped != 2 {
		t.Fatalf("unexpected rate-limit stats: %+v", stats)
	}
	// Remove the limit to check a record that cannot fit into this shard.
	logger.LimitRate(0, 0)
	logger.Info(strings.Repeat("x", 2000))
	stats = logger.Stats()
	if stats.BufferErrors != 1 || stats.Dropped != 3 {
		t.Fatalf("oversized-record stats: %+v", stats)
	}
	if err := pipeline.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFieldKindsRoundTripThroughBinaryFormat(t *testing.T) {
	var id [16]byte
	for i := range id {
		id[i] = byte(i)
	}
	fields := []Field{
		Int("int", -42), String("string", "value"), Float("float", 1.25),
		Bool("bool", true), Uint("uint", 42), Duration("duration", 1500*time.Microsecond),
		RawBytes("raw", []byte{0, 1, 255}), UUID("uuid", id),
		Uint64("uint64_max", ^uint64(0)), Float("negative_zero", math.Copysign(0, -1)),
	}
	record, err := encodeRecord(time.Unix(1, 2), levelInfo, "message", fields, "f.go", 9)
	if err != nil {
		t.Fatal(err)
	}
	decoded, size, err := decodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if size != len(record) || len(decoded.fields) != len(fields) {
		t.Fatalf("size=%d/%d fields=%d", size, len(record), len(decoded.fields))
	}
	want := []string{"-42", `"value"`, "1.25", "true", "42", "1.5ms", "0x0001ff", "00010203-0405-0607-0809-0a0b0c0d0e0f", "18446744073709551615", "-0"}
	for i, value := range want {
		if decoded.fields[i].value != value {
			t.Errorf("field %s: got %q want %q", decoded.fields[i].key, decoded.fields[i].value, value)
		}
	}
}

func TestTracePreservesWrappedError(t *testing.T) {
	sentinel := errors.New("cause")
	err := Trace("operation failed", sentinel)
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is did not find wrapped cause: %v", err)
	}
	if Trace("no error", nil) != nil {
		t.Fatal("Trace(nil) must return nil")
	}
	protocol := &ProtocolError{Protocol: "tcp", Address: "localhost:1", Message: "reset"}
	if !IsProtocolError(protocol) || !strings.Contains(protocol.Error(), "reset") {
		t.Fatalf("bad protocol error: %v", protocol)
	}
}

func TestAggregateStatsCanBeDisabled(t *testing.T) {
	logger := newNoopLogger(t).EnableStats(false)
	logger.Info("not counted")
	logger.Error("not counted", String("error_type", "db"))
	stats := logger.Stats()
	if stats.Total != 0 || stats.Info != 0 || stats.Error != 0 {
		t.Fatalf("stats should stay unchanged: %+v", stats)
	}
	logger.EnableStats(true)
	logger.Info("counted")
	if stats := logger.Stats(); stats.Total != 1 || stats.Info != 1 {
		t.Fatalf("stats not re-enabled: %+v", stats)
	}
}

func FuzzDecodeRecord(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not a record"))
	seed, _ := encodeRecord(time.Unix(0, 0), levelInfo, "seed", nil, "seed.go", 1)
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		for len(data) > 0 {
			_, size, err := decodeRecord(data)
			if err != nil {
				return
			}
			if size <= 0 || size > len(data) {
				t.Fatalf("decoder returned invalid size %d for %d bytes", size, len(data))
			}
			data = data[size:]
		}
	})
}

func BenchmarkLoggerInfo(b *testing.B) {
	sink := &benchTransport{}
	pipeline := NewPipeline(16, 4<<20, time.Hour, NewTransport(sink))
	logger := NewWithPipeline(pipeline, String("service", "benchmark")).EnableStats(true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("request complete", Int("status", 200))
		if i%4096 == 4095 {
			b.StopTimer()
			_ = pipeline.FlushBuffer()
			b.StartTimer()
		}
	}
	b.StopTimer()
	_ = pipeline.Close()
}

func BenchmarkLoggerInfoNoStats(b *testing.B) {
	sink := &benchTransport{}
	pipeline := NewPipeline(16, 4<<20, time.Hour, NewTransport(sink))
	logger := NewWithPipeline(pipeline, String("service", "benchmark")).EnableStats(false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("request complete", Int("status", 200))
		if i%4096 == 4095 {
			b.StopTimer()
			_ = pipeline.FlushBuffer()
			b.StartTimer()
		}
	}
	b.StopTimer()
	_ = pipeline.Close()
}

func TestPipelineDoesNotRetryAnExhaustedBatchAutomatically(t *testing.T) {
	sink := &retryTransport{failUntil: 1_000_000}
	group := NewTransport(sink)
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	pipeline := NewPipeline(4, 1024, 0, group).SetFlushSize(1)
	go pipeline.Start()
	if err := pipeline.Push([]byte("failed-once")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for sink.attempts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := sink.attempts.Load(); got != 1 {
		t.Fatalf("initial attempts=%d, want 1", got)
	}
	time.Sleep(80 * time.Millisecond)
	if got := sink.attempts.Load(); got != 1 {
		t.Fatalf("failed batch was retried by Pipeline: attempts=%d", got)
	}
	if pipeline.LastError() == nil {
		t.Fatal("expected asynchronous failure to remain observable")
	}
	if err := pipeline.Close(); err == nil {
		t.Fatal("Close should report the unconfirmed batch failure")
	}
	if got := sink.attempts.Load(); got != 1 {
		t.Fatalf("Close retried failed batch: attempts=%d", got)
	}
}

func TestClockReturnsDetachedBytesAndCloses(t *testing.T) {
	clock := NewClock("15:04:05.000")
	defer clock.Close()

	first := clock.TimeBytes()
	if len(first) == 0 {
		t.Fatal("expected formatted clock bytes")
	}
	first[0] = '!'
	if got := clock.TimeBytes(); len(got) == 0 || got[0] == '!' {
		t.Fatalf("caller modified cached clock value: %q", got)
	}
	appended := clock.AppendTime([]byte("time="))
	if string(appended[:5]) != "time=" || len(appended) != 5+len(clock.TimeBytes()) {
		t.Fatalf("unexpected AppendTime result: %q", appended)
	}
	clock.Close()
	clock.Close()
}

func TestLoggerStatsIncludesPipelineFlushErrors(t *testing.T) {
	sink := &retryTransport{failUntil: 100}
	group := NewTransport(sink)
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	pipeline := NewPipeline(2, 1024, 0, group)
	logger := NewWithPipeline(pipeline).EnableStats(true)
	logger.Info("batch will fail")
	if err := logger.Flush(); err == nil {
		t.Fatal("expected flush error")
	}
	stats := logger.Stats()
	if stats.FlushErrors != 1 || stats.FlushRecordsUnconfirmed == 0 {
		t.Fatalf("flush diagnostics were not exposed: %+v", stats)
	}
	_ = pipeline.Close() // close performs one final flush, which is expected to fail
}

func TestTCPWriteDeadlineBoundsBlockedConnection(t *testing.T) {
	writer, reader := net.Pipe()
	defer reader.Close()
	transport := (&TCPTransport{conn: writer, writeTimeout: 10 * time.Millisecond})
	payload := make([]byte, 1<<20)
	start := time.Now()
	err := transport.Write(payload)
	if err == nil {
		t.Fatal("expected blocked TCP write to hit its deadline")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout error, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("write deadline was not respected: took %s", time.Since(start))
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("second close should be idempotent, got %v", err)
	}
}

func TestContextFieldsAreEncodedWithoutChangingOverrideSemantics(t *testing.T) {
	sink := &testTransport{}
	pipeline := NewPipeline(2, 1024, 0, NewTransport(sink))
	logger := NewWithPipeline(pipeline, String("service", "checkout"), String("error_type", "context")).TrackErrorTypes(4)
	logger.Error("failed", String("error_type", "request"), Int("attempt", 3))
	if err := logger.Flush(); err != nil {
		t.Fatal(err)
	}
	decoded, _, err := decodeRecord(sink.Data())
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.fields) != 4 {
		t.Fatalf("got %d fields, want two context and two call fields", len(decoded.fields))
	}
	if decoded.fields[0].key != "service" || decoded.fields[1].value != `"context"` || decoded.fields[2].value != `"request"` {
		t.Fatalf("field order/values changed: %#v", decoded.fields)
	}
	stats := logger.Stats()
	if stats.ErrorsByType["request"] != 1 || stats.ErrorsByType["context"] != 0 {
		t.Fatalf("per-call error type should override context: %#v", stats.ErrorsByType)
	}
	if err := pipeline.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAggregateStatisticsAreOptIn(t *testing.T) {
	logger := newNoopLogger(t)
	logger.Info("not counted until enabled")
	if got := logger.Stats().Total; got != 0 {
		t.Fatalf("stats should be disabled by default, got total %d", got)
	}
	logger.EnableStats(true)
	logger.Info("counted")
	if got := logger.Stats().Total; got != 1 {
		t.Fatalf("expected one counted call after enabling, got %d", got)
	}
}

func TestPipelineCloseWaitsForConcurrentProducers(t *testing.T) {
	sink := &testTransport{}
	pipeline := NewPipeline(8, 4096, time.Millisecond, NewTransport(sink))
	go pipeline.Start()

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				err := pipeline.Push([]byte("x"))
				if errors.Is(err, ErrPipelineClosed) {
					return
				}
				if err != nil {
					t.Errorf("push failed unexpectedly: %v", err)
					return
				}
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)
	if err := pipeline.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err := pipeline.Push([]byte("after-close")); !errors.Is(err, ErrPipelineClosed) {
		t.Fatalf("expected ErrPipelineClosed, got %v", err)
	}
}

type selectiveFailTransport struct {
	mu        sync.Mutex
	failed    []string
	succeeded []string
}

func (t *selectiveFailTransport) Write(data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	value := string(data)
	if value == "old-batch" {
		t.failed = append(t.failed, value)
		return errors.New("old batch permanently rejected")
	}
	t.succeeded = append(t.succeeded, value)
	return nil
}
func (t *selectiveFailTransport) Close() error { return nil }

func TestPipelineSendsFreshBatchAfterOldBatchFailureWithoutReplayingIt(t *testing.T) {
	sink := &selectiveFailTransport{}
	group := NewTransport(sink)
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	pipeline := NewPipeline(2, 128, 0, group)
	if err := pipeline.Push([]byte("old-batch")); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.FlushBuffer(); err == nil {
		t.Fatal("expected old batch failure")
	}
	if err := pipeline.Push([]byte("new-batch")); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.FlushBuffer(); err != nil {
		t.Fatalf("new batch should succeed independently: %v", err)
	}
	sink.mu.Lock()
	succeeded := append([]string(nil), sink.succeeded...)
	failed := append([]string(nil), sink.failed...)
	sink.mu.Unlock()
	if len(succeeded) != 1 || succeeded[0] != "new-batch" {
		t.Fatalf("fresh batch was not attempted: %#v", succeeded)
	}
	if len(failed) != 1 {
		t.Fatalf("old batch should not be replayed, got failures %#v", failed)
	}
	// Close reports that an earlier batch was not confirmed.
	if err := pipeline.Close(); err == nil {
		t.Fatal("Close should report the earlier unconfirmed batch failure")
	}
}

func BenchmarkLoggerInfoParallel(b *testing.B) {
	benchmarkLoggerInfoParallel(b, true)
}

func BenchmarkLoggerInfoParallelNoStats(b *testing.B) {
	benchmarkLoggerInfoParallel(b, false)
}

func benchmarkLoggerInfoParallel(b *testing.B, statsEnabled bool) {
	sink := &benchTransport{}
	pipeline := NewPipeline(64, 1<<20, time.Millisecond, NewTransport(sink))
	logger := NewWithPipeline(pipeline, String("service", "parallel-benchmark")).EnableStats(statsEnabled)
	go pipeline.Start()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			logger.Info("request complete", Int("status", 200))
		}
	})
	b.StopTimer()
	_ = pipeline.Close()
}

func TestPipelineDoesNotRetryPermanentHTTPFailure(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	group := NewTransport(NewHTTPTransport(server.URL, server.Client()))
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	pipeline := NewPipeline(2, 1024, 5*time.Millisecond, group).SetFlushSize(1)
	go pipeline.Start()
	if err := pipeline.Push([]byte("permanent-failure")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for requests.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if requests.Load() != 1 {
		t.Fatalf("initial request count = %d, want 1", requests.Load())
	}
	time.Sleep(80 * time.Millisecond) // several periodic flush ticks
	if got := requests.Load(); got != 1 {
		t.Fatalf("permanent HTTP 400 was retried by Pipeline: requests=%d", got)
	}

	err := pipeline.Close()
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("Close should report the permanent HTTP status error, got %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("Close resent permanent batch: requests=%d", got)
	}
}

func TestPipelineDoesNotRetryExhaustedTransientBatch(t *testing.T) {
	sink := &retryTransport{failUntil: 1}
	group := NewTransport(sink)
	if err := group.SetRetryPolicy(RetryPolicy{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	pipeline := NewPipeline(2, 1024, 0, group).SetFlushSize(1)
	go pipeline.Start()
	if err := pipeline.Push([]byte("transient-failure")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for sink.attempts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := sink.attempts.Load(); got != 1 {
		t.Fatalf("attempts=%d, want exactly 1", got)
	}
	time.Sleep(30 * time.Millisecond)
	if got := sink.writes.Load(); got != 0 {
		t.Fatalf("failed batch unexpectedly succeeded later: writes=%d", got)
	}
	if pipeline.LastError() == nil {
		t.Fatal("expected asynchronous failure to remain observable")
	}
	if err := pipeline.Close(); err == nil {
		t.Fatal("Close should report the unconfirmed batch failure")
	}
	if got := sink.attempts.Load(); got != 1 {
		t.Fatalf("Close retried failed batch: attempts=%d", got)
	}
}

func TestTCPTransportPublicConstructorAndWrite(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	want := []byte("tcp-binary-batch")
	readDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			readDone <- err
			return
		}
		defer conn.Close()
		got := make([]byte, len(want))
		if _, err := io.ReadFull(conn, got); err != nil {
			readDone <- err
			return
		}
		if !bytes.Equal(got, want) {
			readDone <- fmt.Errorf("TCP payload = %q, want %q", got, want)
			return
		}
		readDone <- nil
	}()

	transport, err := NewTCPTransport(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.WithWriteTimeout(time.Second).Write(want); err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUDPTransportPublicConstructorAndWrite(t *testing.T) {
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	want := []byte("udp-datagram")
	readDone := make(chan error, 1)
	go func() {
		if err := server.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			readDone <- err
			return
		}
		buf := make([]byte, 1024)
		n, _, err := server.ReadFrom(buf)
		if err != nil {
			readDone <- err
			return
		}
		if !bytes.Equal(buf[:n], want) {
			readDone <- fmt.Errorf("UDP payload = %q, want %q", buf[:n], want)
			return
		}
		readDone <- nil
	}()

	transport, err := NewUDPTransport(server.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.WithWriteTimeout(time.Second).Write(want); err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLoggerStatsFormatting(t *testing.T) {
	stats := LoggerStats{Total: 12, Info: 8, Error: 4, ErrorsByType: map[string]uint64{"z_timeout": 2, "a_db": 2}}
	plain := stats.String()
	if strings.Contains(plain, "\x1b[") {
		t.Fatalf("plain stats contain ANSI color codes: %q", plain)
	}
	if !strings.Contains(plain, "a_db=2, z_timeout=2") {
		t.Fatalf("error types are not deterministically sorted: %q", plain)
	}
	colored := stats.Format(true)
	if !strings.Contains(colored, "\x1b[") {
		t.Fatalf("colored format missing ANSI escapes: %q", colored)
	}
}
