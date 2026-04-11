# Building a Log Ingestor in Go with Double-Buffered Sharded Arenas

*How to absorb millions of writes per second without ever locking a mutex or pausing the world.*

---

## The Problem: Writing Fast Is Surprisingly Hard

Imagine a service handling tens of thousands of concurrent connections — an API gateway, a telemetry collector, a high-frequency trading feed. Every request wants to log something: a trace span, an access record, a metric. Naively, you funnel all of them through a single `sync.Mutex` protecting a shared buffer. This works fine at low volume. Then traffic doubles, your P99 latency spikes, and a profiler tells you the culprit is a single goroutine waiting on a lock that some other goroutine holds while flushing a 4 MB buffer to disk.

The core tension is this: **producers are many and parallel; the destination (disk, network socket, stdout) is singular and slow**. You need a design where the act of writing into a buffer is completely decoupled from the act of draining that buffer to its destination.

`bytearena` is a Go library built around one answer to this problem: **double-buffered, sharded memory arenas with lock-free reservation**.

---

## The Solution in One Sentence

Two fixed-size byte buffers rotate roles — producers fill one while a single consumer drains the other — and each buffer is partitioned into eight independently addressable sub-regions so producers never contend with each other either.

Let us unpack every word of that.

---

## Building Block 1: The Arena

An arena is just a large, pre-allocated `[]byte`. Nothing is heap-allocated at write time. There is no GC pressure. The only question is: *which bytes inside this buffer does this particular goroutine own?*

The answer is a per-sub-region atomic cursor. Each goroutine calls a compare-and-swap (CAS) loop against the cursor for its assigned sub-region:

```text
[cursor] ──CAS──► [cursor + n]
```

If the CAS succeeds, the goroutine owns `buf[cursor : cursor+n]` exclusively — no lock, no contention from other sub-regions. It writes directly into that slice and declares itself done.

```go
type paddedCursor struct {
    value atomic.Uint32
    _     [60]byte // pad to exactly one 64-byte cache line
}

type arena struct {
    epoch           atomic.Uint64   // bumped on every reset
    _               [56]byte
    numberWriters   atomic.Int32    // in-flight producer count
    _               [60]byte
    rollbackCounter atomic.Int32    // failed reservations
    _               [60]byte
    buf             []byte
    subRegions      [8]subRegion
    subRegionCursors [8]paddedCursor
}
```

The padding between atomics is deliberate and critical. Two atomic variables sharing a 64-byte CPU cache line means every write to one invalidates the other in every other core's L1 cache. With padding, each hot atomic lives alone on its own cache line.

---

## Building Block 2: Sharding Eliminates CAS Contention

A single CAS cursor for the whole arena still creates contention when tens of goroutines hammer it simultaneously. Failed CAS operations retry immediately, generating a storm of cache-line invalidation traffic across all cores.

The solution is to **shard the arena into 8 sub-regions**, each with its own independent cursor. Producers are round-robined across sub-regions using a global atomic request counter:

```go
regionIdx := m.counterRequests.Add(1) & 7  // bit-mask: always 0–7
```

Contention across 8 cursors is ~8× lower than contention on a single cursor. Goroutines that happen to land on the same sub-region still contend, but in practice the distribution flattens quickly under load.

Each sub-region occupies a contiguous slice of the arena buffer:

```text
Arena buffer (e.g. 4 MB)
├── SubRegion 0  [0, 512K)       cursor₀
├── SubRegion 1  [512K, 1024K)   cursor₁
├── SubRegion 2  [1024K, 1536K)  cursor₂
│   ...
└── SubRegion 7  [3584K, 4096K)  cursor₇
```

A message never crosses a sub-region boundary. If `payload > sub-region capacity`, the write is rejected upfront. This is not a limitation in practice — `maxMessageSize` is set to the sub-region size at construction time and validated before any CAS attempt.

---

## Building Block 3: Double Buffering Separates Producers from the Consumer

With a single arena, the consumer (the code that flushes to disk) has to pause all producers while it reads the buffer. That pause is exactly the latency spike we set out to eliminate.

With two arenas, the roles rotate:

```text
Time ──────────────────────────────────────────────────────►

Arena A:  [fill] [fill] [fill] ──seal──► [drain] [reset]
Arena B:                        [fill]   [fill]  [fill] ──seal──►
```

Producers always write into the **active** arena. The consumer seals it (by atomically swapping the active pointer to the other arena), waits for any in-flight writes to finish, flushes the sealed arena to the `io.Writer`, resets it, and it becomes the next arena to be sealed. The two roles leapfrog each other perpetually.

The active arena pointer is an `atomic.Pointer[arena]` — an atomic store with the required memory ordering guarantees, with no mutex and no memory allocation.

---

## Building Block 4: The Writer Handshake (Enter/Leave)

### Enter

There is a subtle race between a producer successfully reading the active arena pointer and the consumer rotating away from it. The sequence:

1. Producer loads `active` → gets arena A.
2. Consumer seals A, swaps active to B, begins waiting for writers to drain from A.
3. Producer calls `Enter()` on A — now it is a writer on a sealed arena.

To handle this, every producer does a **post-enter active check**:

```go
arena.Enter()

if m.active.Load() != arena {
    arena.Leave()
    return WriteRegion{}, ErrWriteActiveArenaMismatch
}
// safe to proceed — we hold a counted reference on the active arena
```

### Leave

`numberWriters` is an `atomic.Int32` on its own cache line. The consumer's wait is bounded by a configurable timeout (default 50 ms); if writers have not drained by then, the sealed data is dropped and the arena is reset. This makes the system explicitly lossy under pathological stalls, trading durability for bounded latency.

```go
for writers.Load() != 0 {
    // spin < 20:  PAUSE instruction (hardware back-off hint)
    // spin < 100: runtime.Gosched() (yield to scheduler)
    // default:    time.Sleep(5µs)
}
```

This three-tier backoff avoids both busy-waiting at scale and unnecessary sleep latency when the wait is brief.

---

## Building Block 5: The Seal Heuristic

The consumer loop ticks on a configurable interval (default 50ms) and also accepts an out-of-band flush signal from producers. On each tick it asks: *should the active arena be sealed now?*

```go
func (m *Ingestor) shouldSeal(a *arena) bool {
    if a.rollbackCounter.Load() > 0 {
        return true
    }
    for ix, threshold := range m.arenaSealThresholds {
        if a.subRegionCursors[ix].value.Load() >= threshold {
            return true
        }
    }
    return false
}
```

The ticker fires every 50 ms (can be set with `WithTickMilliseconds`) but does no work if shouldSeal returns false — a quiet arena with writes below the threshold sits undisturbed across many intervals. Only when at least one cursor crosses its watermark, or a rollback has been recorded, does the consumer rotate.

Two signals trigger a seal:

**Cursor threshold** — any sub-region cursor reaches a pre-computed watermark (default 90% of capacity). Thresholds are computed once at startup and stored as a plain `[8]uint32` array — no arithmetic at runtime.

**Rollback pressure** — a producer failed to reserve space (the sub-region was full or it did not have enough spare capacity). This is a signal that the arena is under pressure and should be rotated immediately, even if other sub-regions still have room.

When a producer fails, it also sends a non-blocking signal on a channel:

```go
select {
case m.chFlush <- struct{}{}:
default: // signal already pending
}
```

The consumer's `select` has a third case for this channel alongside the ticker and context done — so pressure from producers feeds back into flush timing without any synchronous coordination.  

When a sub-region is full, `Write` does not return `ErrWriteSubRegionFull` to the caller — that error is internal. Instead, the producer captures the current arena's epoch, then yields in a `runtime.Gosched()` loop until either the active pointer changes (consumer rotated) or the epoch increments (consumer reset the arena after flushing). Only then does it attempt one final (internal) `beginWrite`. This means back-pressure from a full arena is absorbed as scheduler yields inside the `Write` call, invisible to the caller but visible in latency — which is why benchmarks should measure end-to-end time rather than just reservation cost.

---

## Building Block 6: Two Flush Strategies

The library ships two flusher implementations, selectable at construction time.

**Per-region flush** (default) — iterates sub-regions and calls `writer.Write(data)` once per non-empty sub region. Produces multiple smaller writes (around 8x smaller than size). Better for writers that buffer internally (e.g., a `bufio.Writer` or a TCP connection).

**Isolated-buffer flush** — copies all sub-region data into a scratch buffer (`flushScratch`, pre-allocated and reused across cycles) and issues a single `writer.Write`. Better for writers where syscall overhead dominates, such as direct file I/O.

Both strategies handle partial writes (the `bytesWritten < len(data)` case) and zero-progress errors without looping indefinitely.

---

## Shutdown: Flushing Without Data Loss

Context cancellation triggers a deliberate two-rotation shutdown sequence:

```go
firstSealed  := m.rotate()   // seal whatever is currently active (A)
secondSealed := m.rotate()   // seal whatever just became active (B)
m.active.Store(nil)          // close the door: no new producers enter
```

The double rotation ensures that any producer who was bumped from A during the first rotation and retried into B is also captured before the door closes. Setting active to `nil` causes all subsequent `beginWrite` calls to return `ErrWriteNoActiveArena` immediately. The two sealed arenas are drained in sequence: the second-sealed arena is flushed first, because any producer bumped from the first rotation may have retried into it and could still be draining — its writers must be waited on before reading its contents. The first-sealed arena follows.

---

## The Result: What You Get

- **Zero allocation on the write path.** No `new`, no `make`, no interface boxing after initialization.
- **No mutexes in the critical path.** CAS + atomic swap everywhere.
- **False-sharing eliminated.** Every hot atomic sits alone on a 64-byte cache line.
- **Backpressure without blocking.** A full arena yields inside `Write` via `runtime.Gosched()` until the consumer rotates — no error surfaces to the caller, no producer goroutine blocks on synchronization primitives.
- **Configurable flush strategy.** One `io.Writer` interface; swap between per-region and isolated-buffer flushers with an option.
- **Structured telemetry.** Every error type has a padded atomic counter in the `ErrorsRegistry`; a `Snapshot()` call harvests and resets all counts atomically.

### Measured Performance

- The benchmark reports end-to-end ingestion latency (including asynchronous flush completion), not just the cost of the `Write` call.  
- The stabilization detector waits until the total written byte counter stops changing, ensuring all asynchronous flushes have completed.  
- The benchmark uses a zero-cost writer to isolate ingestion overhead from I/O constraints.  
- Payload size is fixed at 32 bytes to isolate allocator and contention effects.  
- Parallelism is fixed to 16 goroutines to simulate high contention independent of CPU core count.
- Ryzen 7 5800H with Rocky 10 operating system was used to run the benchmark.

BenchmarkIngestor_Parallel-16    	41500706	        29.62 ns/op	         8.631 Gb/s	       0 B/op

```go
func BenchmarkIngestor_Parallel(b *testing.B) {
	writer := helpers.CountWriterNoBuffer{}

	ingestor, _ := NewIngestor(
		Size1M(),
		&writer,
	)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	time.Sleep(10 * time.Millisecond) // warmup

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") // 32 bytes

	b.ReportAllocs()
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				_, _ = ingestor.Write(payload)
			}
		},
	)

	// Throughput
	bytesWritten := float64(writer.TotalBytesWritten.Load())
	seconds := float64(b.Elapsed().Nanoseconds()) / 1e9
	gbps := (bytesWritten * 8) / (seconds * 1e9)

	b.ReportMetric(gbps, "Gb/s")

	cancel()
	<-chIngestionEnd
}
```

---

## When to Use This Pattern

Double-buffered sharded arenas shine when:

- Write throughput is the bottleneck (log ingestion, telemetry pipelines, event streaming).
- Writes are small and homogeneous (fits within one sub-region per message).
- The downstream `io.Writer` is slower than the producers (buffered disk, compressed network stream).
- You need predictable latency rather than just high average throughput.

They are overkill when the write rate is low, when messages can be arbitrarily large, or when ordering across sub-regions must be strictly preserved (sub-regions drain independently, so inter-region ordering is not guaranteed).  
This pattern introduces significant complexity and should only be used once contention is measured and identified as a bottleneck.

---

## Closing Thought

The design demonstrates something worth internalizing: **contention is not inevitable, it is a design choice**. Every time you reach for a mutex, ask whether the problem could instead be solved by partitioning ownership — in space (shards), in time (double buffering), or in role (multiple producers vs. single consumer). Often it can, and the result is a system that scales linearly with cores rather than collapsing under them.

The source is available on GitHub at [github.com/TudorHulban/bytearena](https://github.com/TudorHulban/bytearena). Benchmarks, test helpers (including a suite of adversarial `io.Writer` implementations), and helpers are all included.

---

*Tags: `go` `golang` `performance` `concurrency` `systems` `lockfree`*
