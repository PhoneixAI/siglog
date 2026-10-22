# siglog validation report

Date: 2026-10-21

This report applies to the documented candidate in this archive, including the functional-option constructor API.

## API changes in this candidate

- `New(opts ...Option) (*Logger, error)` creates and starts the logger's internal pipeline. The `Logger` no longer exports its `Pipeline` or context-field slice.
- `WithBufferProfile(ProfileBalanced|ProfileLowMemory|ProfileHighThroughput)` applies a preset for shard count, physical per-shard capacity, flush threshold, and interval.
- `WithBuffer(BufferConfig{ShardCount, ShardCapacity})` applies explicit shard settings; `ShardCapacity` includes the reserved byte.
- `WithFlushSize`, `WithFlushInterval`, `WithTransport`, `WithRetryPolicy`, and `WithFields` configure a logger before the pipeline starts.
- `Logger.Flush`, `Logger.LastError`, and `Logger.Close` expose the normal lifecycle API. `NewWithPipeline` remains as a low-level integration hook.

## Checks run

All commands below ran from the module root (`siglog/`):

```text
go test ./...                         PASS
go test -race -count=3 ./...          PASS
go vet ./...                          PASS
go test -cover ./...                  PASS; 79.0% statement coverage
go test -run '^$' -fuzz '^FuzzDecodeRecord$' -fuzztime=3s
                                      PASS; 51,023 executions, 11 new interesting inputs
go test -c -o /tmp/siglog_386.test    PASS (cross-compilation only)
GOARCH=arm64 go test -c -o /tmp/siglog_arm64.test
                                      PASS (cross-compilation only)
```

The cross-architecture checks compile test binaries; they do not execute them on 386 or arm64 hardware.

## Sample benchmark results

Machine: AMD EPYC 9V74, linux/amd64, Go 1.23.2. Three runs with `-benchmem`; medians shown. These are local measurements, not universal throughput promises.

| Benchmark | Median time | Bytes/op | Allocs/op |
|---|---:|---:|---:|
| `BenchmarkBufferPush` | 26.54 ns/op | 11 B/op | 0 |
| `BenchmarkPipelinePush` | 32.44 ns/op | 0 B/op | 0 |
| `BenchmarkPipelineBatch` | 3.265 µs/op | 1,152 B/op | 1 |

Benchmarks should be repeated on target hardware with realistic message sizes, concurrency, and destinations before choosing a profile.

## Configuration profile values

| Profile | Shards | Physical capacity per shard | Flush threshold | Interval |
|---|---:|---:|---:|---:|
| `ProfileBalanced` | 4 | 64 KiB | 64 KiB | 250 ms |
| `ProfileLowMemory` | 2 | 16 KiB | 8 KiB | 100 ms |
| `ProfileHighThroughput` | 8 | 256 KiB | 256 KiB | 250 ms |

The presets are starting points. They are not claims that a given messages-per-second rate is guaranteed.

## Known limitations before a production release

- The buffer's historical overflow path can still drain and discard buffered data when all attempted shards are full. This remains documented as a pre-release limitation and should be addressed before claiming lossless logging.
- Record order across shards is not globally deterministic.
- Failed batches are not retained after bounded transport retries are exhausted. This is a best-effort logging policy, not durable audit storage.
- The module path is currently `siglog` for local reproducible tests. Replace it with the actual GitHub module path before publishing, and update the README install command.
- Choose a license and add `LICENSE` before publishing.
