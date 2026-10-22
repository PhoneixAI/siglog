<div align="center">

# siglog

**A compact, structured logging library for high-throughput Go services.**

Built around a sharded ring buffer and a length-prefixed binary record format — with readable console output when you need it.

![Go](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go&logoColor=white)
![Status](https://img.shields.io/badge/status-pre--release-orange)
![Format](https://img.shields.io/badge/records-binary-blue)

</div>

> **Pre-release notice:** siglog is still evolving. Its current ring-buffer overflow policy can discard previously buffered records when all tried shards are full. Do not use this version for loss-intolerant audit or financial records. See [Delivery and limitations](#delivery-and-current-limitations).

## Why siglog?

- **Sharded ring buffer** to spread writes across independently protected byte rings.
- **Length-prefixed binary records** so readers can move to the next record without decoding the current payload.
- **Typed fields** for strings, integers, floats, booleans, durations, timestamps, raw bytes, and UUIDs.
- **Multiple transports**: console, file, TCP, UDP, and HTTP.
- **Automatic interval- and size-triggered flushing** behind the `Logger` API.
- **Bounded transport retries** with errors returned after the configured attempts are exhausted.
- **Optional statistics and rate limiting** for monitoring without forcing aggregate counters onto every logging call.

## Installation

After publishing, replace `PhoneixAI` with the GitHub account or organization that owns this repository:

```bash
go get github.com/PhoneixAI/siglog
```

The final module path in `go.mod` must match this import path before release.

## Quick start

Create a logger with balanced defaults. By default, siglog writes readable, color-aware lines to the console while keeping its internal record format binary.

```go
package main

import (
    "errors"
    "log"
    "time"

    "github.com/PhoneixAI/siglog"
)

func main() {
    logger, err := siglog.New(
        siglog.WithFields(siglog.String("service", "checkout")),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer logger.Close()
    logger.EnableStats(true).TrackErrorTypes(16)

    logger.Info("service started", siglog.Int("port", 8080))
    logger.Warn("slow request", siglog.Duration("elapsed", 280*time.Millisecond))
    logger.Error(
        "request failed",
        siglog.String("error_type", "db_timeout"),
        siglog.NamedErr("err", errors.New("database deadline exceeded")),
    )
    logger.Infof("worker %d is ready", 3)

    if err := logger.Flush(); err != nil {
        log.Printf("siglog flush failed: %v", err)
    }

    log.Print(logger.Stats().Format(true))
}
```

`New` starts background flushing automatically. Always call `Close` when the logger is no longer needed; it stops the flusher, flushes remaining records, and closes transports. `Info` and `Error` only enqueue encoded records, so transport failures are reported by `Flush`, `LastError`, statistics, and `Close`, not by the logging call itself.

## Configure the buffer: three levels

### 1. Defaults

```go
logger, err := siglog.New()
```

The balanced default profile uses four shards with 64 KiB physical capacity per shard, a 64 KiB flush threshold, and a 250 ms periodic flush interval. These are starting values, not throughput guarantees.

### 2. Choose a profile

```go
logger, err := siglog.New(
    siglog.WithBufferProfile(siglog.ProfileHighThroughput),
)
```

Available profiles:

| Profile | Shards | Physical capacity per shard | Flush threshold | Interval |
|---|---:|---:|---:|---:|
| `ProfileBalanced` (default) | 8 | 64 KiB | 64 KiB | 250 ms |
| `ProfileLowMemory` | 4 | 16 KiB | 8 KiB | 100 ms |
| `ProfileHighThroughput` | 32 | 256 KiB | 256 KiB | 250 ms |

Profiles are convenient presets. Benchmark them with your own message sizes, goroutine count, and transport before selecting one for production.

### 3. Set both shard parameters manually

```go
logger, err := siglog.New(
    siglog.WithBuffer(siglog.BufferConfig{
        ShardCount:    8,
        ShardCapacity: 64 * siglog.KB, // physical capacity, including one reserved byte
    }),
    siglog.WithFlushSize(32 * siglog.KB),
    siglog.WithFlushInterval(100 * time.Millisecond),
)
```

`WithBuffer` and `WithBufferProfile` are mutually exclusive. The constructor rejects invalid configurations, including a flush threshold larger than the total writable capacity. A physical shard capacity of `N` stores at most `N-1` bytes, and each encoded record must fit inside one shard.

### Select the destination

Without `WithTransport`, output goes to the readable console transport. Supply a different transport when you want binary records written to a file or network endpoint:

```go
file, err := os.OpenFile("app.siglog", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
if err != nil {
    return err
}

logger, err := siglog.New(
    siglog.WithTransport(siglog.NewFileTransport(file, siglog.SyncNever)),
    siglog.WithBufferProfile(siglog.ProfileBalanced),
)
if err != nil {
    _ = file.Close()
    return err
}
defer logger.Close()
```

When a non-console transport is selected, it receives the binary record stream. The console transport is the one that decodes records into human-readable output.

## Basic API

### Logging levels

```go
logger.Debug("cache lookup", siglog.String("key", "user:42"))
logger.Info("request completed", siglog.Int("status", 200))
logger.Warn("retrying request", siglog.Int("attempt", 2))
logger.Error("database query failed", siglog.String("error_type", "db_timeout"))
logger.Fatalf("configuration is invalid: %s", reason) // logs; does not exit the process
```

`Debug`, `Info`, `Warn`, `Error`, and `Fatal` accept a message plus typed fields. The `*f` methods format a message with `fmt.Sprintf` semantics. `Fatal` logs at the fatal level but intentionally does **not** terminate the process.

### Context fields

Use `With` for fields that should accompany every record from a derived logger:

```go
requestLogger := logger.With(
    siglog.String("request_id", requestID),
    siglog.String("component", "payments"),
)
requestLogger.Info("payment authorized", siglog.String("currency", "EUR"))
```

A derived logger shares the parent pipeline, statistics, and rate-limit configuration.

### Structured fields

Available constructors include:

| Constructor | Use |
|---|---|
| `String(key, value)` | Text values |
| `Int(key, value)` | Signed integers |
| `Uint(key, value)` / `Uint64(key, value)` | Unsigned integers |
| `Float(key, value)` | Floating-point values |
| `Bool(key, value)` | Booleans |
| `Time(key, value)` | `time.Time` values |
| `Duration(key, value)` | `time.Duration` values |
| `NamedErr(key, err)` | Error message as a field |
| `RawBytes(key, value)` | Arbitrary bytes |
| `UUID(key, value)` | A 16-byte UUID value |

For error statistics, use a stable classification such as `siglog.String("error_type", "db_timeout")` rather than unique error messages or request IDs.

## Buffering and shutdown

Most applications do not need to construct a `Pipeline`; `New` creates and starts it internally. Use `Logger.Flush()` to synchronously attempt to send the current batch and `Logger.Close()` to stop background work, flush remaining records, and close transports.

`WithFlushSize` requests a flush when the approximate buffered byte count reaches the configured threshold. `WithFlushInterval` controls periodic flushing; zero disables timer-based flushing, but size-triggered and explicit flushes still work.

`NewWithPipeline` remains available as a low-level integration hook for tests or advanced setups that manage a `Pipeline` themselves. Its caller must start that pipeline. Prefer `New` with functional options for normal application code.

## Transports

`NewTransport(...)` creates a `MultiTransport` fan-out group. It sends a batch to each configured destination and returns combined errors.

### Console

```go
console := siglog.NewConsoleTransport() // human-readable, ANSI color auto-detected
console.WithColor(false)                // explicitly disable colors
```

The console transport decodes binary records to readable lines. Colors are automatically enabled only for terminal output and respect `NO_COLOR`. `WithWriter` can direct output to another `io.Writer`.

### File

```go
file, err := os.OpenFile("app.siglog", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
if err != nil {
    return err
}
fileTransport := siglog.NewFileTransport(file, siglog.SyncNever)
```

Use `SyncAlways` when each batch should call `File.Sync`; this has a performance cost. The transport owns and closes the supplied file.

### TCP, UDP, and HTTP

```go
tcp, err := siglog.NewTCPTransport("127.0.0.1:9000")
if err != nil {
    return err
}
udp, err := siglog.NewUDPTransport("127.0.0.1:9001")
if err != nil {
    return err
}
httpTransport := siglog.NewHTTPTransport("https://logs.example.com/ingest", nil)
httpTransport.Headers.Set("Authorization", "Bearer <token>")
```

TCP uses a persistent connection. UDP sends a batch as one datagram, so keep the batch small enough for your network. HTTP sends batches with `POST` and reports non-2xx statuses as errors. Configure timeouts and headers according to your environment.

## Retries and error handling

- `RetryPolicy.MaxAttempts` includes the initial attempt. The default policy allows three attempts with a short bounded backoff.
- HTTP `429` and `5xx` responses are retryable. Other HTTP `4xx` statuses are treated as permanent errors.
- Custom transports can implement `Retryable() bool` on their error type to classify failures. Unclassified custom transport errors are retried up to the configured limit.
- File and TCP write failures are not replayed automatically if a partial write may already have occurred; replaying the full batch could duplicate a prefix and corrupt the byte stream.
- After the configured attempts are exhausted, `Logger.Flush()` returns an error. The failed batch is not retained or retried in the background; it is counted as unconfirmed and may be lost.
- Periodic flush errors can be inspected with `logger.LastError()`, `logger.Stats()`, and the error returned by `logger.Close()`.

Logging methods such as `Info` and `Error` do not return transport errors because records are buffered. Use explicit `logger.Flush()` when the caller needs to handle a flush error immediately.

## Rate limiting and statistics

```go
logger.LimitRate(1_000, time.Second) // at most 1,000 log calls per fixed one-second window
logger.EnableStats(true)             // aggregate statistics are disabled by default
logger.TrackErrorTypes(32)           // optional bounded map of error classifications

stats := logger.Stats()
log.Print(stats)              // plain text
log.Print(stats.Format(true)) // ANSI-colored text
```

Rate-limit settings and statistics are shared by loggers created with `With`. The rate limiter uses fixed windows, which may permit a burst near a window boundary. The error-type map is bounded; additional distinct classifications are grouped under `other`.

Statistics include attempted calls by level, dropped/rate-limited calls, encoding and buffer errors, flush errors, unconfirmed records, and optionally counts by error type. These are diagnostics, not a durable audit ledger.

## Binary record format

The current record format is length-prefixed and uses big-endian integers:

| Part | Size | Purpose |
|---|---:|---|
| Total record length | 4 bytes | Skip to the next record without decoding this one |
| Timestamp | 8 bytes | Unix timestamp in nanoseconds |
| Level | 1 byte | `DEBUG`, `INFO`, `WARN`, `ERROR`, or `FATAL` |
| Payload header | 12 bytes | Caller-file length, caller line, message length, field count |
| Payload | Variable | Caller path, message, and typed field entries |

Every field entry contains a type byte, key length, value length, key bytes, and value bytes. Numeric values occupy fixed-width binary slots; strings and raw bytes are stored directly. This is intended for fast sequential traversal and compact batching, not for direct display in a terminal.

The binary format is not yet versioned as a stable public wire protocol. Treat it as an implementation detail until a format version and compatibility policy are published.

## Build and test

```bash
gofmt -w *.go
go test ./...
go test -race ./...
go vet ./...
go test -cover ./...
```

Run the benchmarks with:

```bash
go test -run '^$' -bench . -benchmem ./...
```

Always benchmark on the target hardware and workload. Throughput, allocation counts, and contention depend on shard count, record size, flush interval, transport latency, and concurrency.

## CLI

siglog is a **Go library**, not a standalone command-line application. It does not currently provide commands such as `siglog stats` or `siglog decode`. `ConsoleTransport` provides readable terminal output inside an application, and `LoggerStats.Format(true)` formats colored statistics. A separate binary-log inspection CLI can be added after the binary format is versioned.

## Delivery and current limitations

- The ring buffer uses fixed-capacity shards. Its current overflow path can discard previously buffered bytes when the attempted shards are full; this must be resolved before relying on lossless behavior.
- Record ordering across shards is not globally deterministic.
- Delivery is best-effort. Retries do not provide exactly-once delivery, and multi-transport fan-out can have partial success.
- Failed batches are not retained after the bounded retry policy is exhausted. Monitor `LastError`, flush errors, and unconfirmed-record statistics.
- Do not use this pre-release as the sole storage path for audit, compliance, or financial events that must never be lost.

## License

Choose a license and add the corresponding `LICENSE` file before publishing this repository.
