# Arena‑Based Ingestor Architecture

The document describes the high‑performance, lock‑free, double‑buffered byte arena system based on atomic region reservation, and writers‑in‑flight tracking.  
It is designed for **x86**, **Linux** hosts that need high‑throughput ingestion with predictable memory usage and bounded behavior under load.

## 1. Overview

The system consists of:

- Many producers (goroutines generating byte entries).
- One consumer (goroutine flushing arenas).
- Two fixed‑size arenas used in a double‑buffer rotation: Arena A, Arena B.

At any moment:

- One arena is **active** (producers write here).
- One arena is **sealed** (consumer flushes here).

This ensures:

- Lock‑free producer writes.
- No per‑entry allocations.
- Bounded memory usage.
- Predictable backpressure behavior.

## 2. Arena Structure

Each arena contains:

- A fixed‑size byte buffer (`buf []byte`).
- Eight independently addressed sub‑regions, each with its own cache‑line‑padded atomic CAS cursor (`subRegionCursors [8]paddedCursor`).
- An atomic writers‑in‑flight counter (`numberWriters atomic.Int32`).
- An atomic rollback counter (`rollbackCounter atomic.Int32`), incremented by producers on failed reservations.
- An atomic epoch counter (`epoch atomic.Uint64`), bumped on every reset so producers can detect arena recycling.

Arenas never grow, shrink, or reallocate.

## 3. Producer API

The public API is a single method that satisfies `io.Writer`:

```go
func (ing *Ingestor) Write(payload []byte) (int, error)
```

Producers never interact with arenas directly and do not need to be reconfigured during rotation.

Internally, `Write` delegates through:

```text
Write → write → TryWrite → beginWrite → reserveBytes → defered End write — signals end of a write by decrementing the number of writers
```

### Direct copy to the arena

Direct copies would request a reservation and write directly to the arena memory thus saving allocations.

```go
region, _ := ingestor.TryWrite(pass message number of bytes)
copy(region.Buf(), message)
ingestor.EndWrite(region)
```

## 4. Producer Write Algorithm

For each ingestion:

1. Load the active arena pointer atomically.
2. Call `Enter()` — increments `numberWriters` on that arena.
3. Re‑check the active pointer; if it has rotated away since step 1, call `Leave()` and return `errWriteActiveArenaMismatch`.
4. Select a sub‑region via round‑robin: `regionIdx = counterRequests.Add(1) & 7`.
5. Run a CAS loop on `subRegionCursors[regionIdx]` to atomically advance the cursor by `N` bytes and obtain a unique region `[offset, offset+N)`.
6. Copy the payload directly into `arena.buf[offset : offset+N]`.
7. Call `Leave()` — decrements `numberWriters`.

This path is **lock‑free**. It is not wait‑free: when the active arena is full the producer yields until the consumer rotates (see §9a).

### CAS reservation

Space is reserved with a compare‑and‑swap loop, not a fetch‑add:

```go
for {
    cur := cursor.Load()
    if cur < lower || cur > limit {
        return 0, ErrWriteSubRegionFull
    }
    if cursor.CompareAndSwap(cur, cur+N) {
        return cur, nil
    }
    // CAS lost the race — retry (after pause)
}
```

### Producer asking for unavailable space

If `N > maxMessageSize` (one sub‑region's capacity), the request is rejected immediately with `errWriteMessageTooLarge` — no CAS is attempted.

If the CAS loop determines the cursor is beyond the sub‑region limit:

- `rollbackCounter` is incremented.
- A non‑blocking flush signal is sent to the consumer.
- `ErrWriteSubRegionFull` is returned to the internal caller.

`Write` does not surface `errWriteSubRegionFull` to callers. Instead it captures the current epoch and yields via `runtime.Gosched()` until the consumer rotates the arena or resets it (epoch increment), then retries once. See §9a.  

Near the end of an arena many producers may attempt reservations concurrently. Some of these reservations may exceed the arena size and fail. This is expected behavior. Once the consumer seals the arena and rotates to the next one, producers will automatically obtain space in the new arena.

## 5. Consumer Monitoring Loop

The consumer's `tick()` is driven by below event sources:

1. A configurable periodic ticker for normal traffic (`tickThreshold`, `WithTickThresholdMilliseconds`).
2. A configurable periodic ticker for low traffic (`tickIfData`, `WithTickIfDataMilliseconds`).
3. A non‑blocking flush channel (`chFlush`) signaled by producers on rollback.
4. Context cancellation (initiates shutdown).

Signals that trigger a seal:

- **Cursor threshold** — any sub‑region cursor reaches a pre‑computed watermark (default 90% of sub‑region capacity). Thresholds are computed once at startup as a `[8]uint32` array; no arithmetic at runtime.
- **Rollback pressure** — at least one producer failed to reserve space, indicating the arena is under pressure.
- **Data existence** for low traffic periods.

## 6. Sealing Protocol

Sealing is performed exclusively by the consumer via `rotate()`:

1. Read the current active arena pointer (call it X).
2. Determine the other arena (Y).
3. Atomically store Y as the new active pointer — all new writes now go to Y.
4. Return X as the sealed arena.

X is sealed: no new writes will start on it. Producers already past the active check are still finishing current writes; `numberWriters` tracks them.

## 7. Waiting for Writers to Finish

After sealing arena X the consumer calls `waitForWriters`, which blocks until `numberWriters == 0` using adaptive backoff:

| Spin count | Strategy |
|---|---|
| < 20 | `PAUSE` instruction (x86 hardware hint) |
| < 100 | `runtime.Gosched()` |
| ≥ 100 | `time.Sleep(1 µs)` |

The wait is wrapped in a context with a configurable timeout (default 50 ms, `WithUnblockMilliseconds`). If the timeout expires before all writers drain, the sealed data is dropped, `TErrDroppedSealedData` is recorded, and the arena is reset. This prioritizes ingestor liveness over durability.

## 8. Flushing the Sealed Arena

Once `numberWriters == 0`, the consumer calls the configured flusher. Two strategies are available:

**Per‑region flush** (default) — one `writer.Write` call per non‑empty sub‑region. Produces up to 8 smaller writes. Preferred when the downstream writer buffers internally (e.g. `bufio.Writer`, TCP socket).

**Isolated‑buffer flush** — copies all sub‑region data into a pre‑allocated scratch buffer (`flushScratch`, reused across cycles) and issues a single `writer.Write`. Preferred when syscall overhead dominates (e.g. direct file I/O).

Both strategies handle partial writes and zero‑progress errors without looping indefinitely.

After flushing, `reset()` is called:

- Each sub‑region cursor is restored to its `Lower` bound (not zero).
- `rollbackCounter` is cleared.
- `epoch` is incremented — producers spinning on a full arena detect this and retry.
- `numberWriters` is **not** reset; it reaches zero naturally via `Leave()` calls and must not be touched here to avoid a race with in‑flight producers.

## 9. Backpressure and Timeout Behavior

### a. Producer Backpressure on Full Arena

When all slots in the selected sub‑region are exhausted, `Write` does not return an error to the caller. Instead it:

1. Records the current epoch of the stale arena.
2. Yields in a `runtime.Gosched()` loop until either the active pointer changes (consumer rotated) or the epoch increments (consumer recycled the arena).
3. Attempts one final `beginWrite`.

Back‑pressure is therefore absorbed as scheduler yields inside `Write`, invisible to the caller but measurable as latency. This is why benchmarks must measure end‑to‑end time rather than just reservation cost.

### b. Data Loss on Writer Drain Timeout

When `waitForWriters` times out:

- The sealed arena is reset without flushing.
- Any buffered data is dropped.
- The event is recorded via `Registry.Inc(TErrDroppedSealedData)`.

### c. Writer Blocking Semantics

- The ingestor does **not** guarantee non‑blocking behavior if the injected `io.Writer` blocks.
- If non‑blocking or cancellable semantics are required, wrap the writer to handle that externally.
- The ingestor will not allocate additional memory, spawn extra goroutines, or create hidden queues to compensate for a blocked writer.
- Slow or intermittently blocking writers are fully supported; ingestion remains bounded and correct, but throughput follows the writer's speed.

### (internal) d. Flusher Synchronization Requirement

The `flusher` function **must complete synchronously** before returning.

If flushing is asynchronous it must either:

1. Copy the arena data inside `flusher` before returning, OR
2. Block until the async operation completes before `flusher` returns.

Violating this causes a data race: `tick()` calls `reset()` immediately after `flusher` returns, zeroing the arena while async operations may still be reading it.

## 10. Shutdown

Context cancellation triggers a deliberate two‑rotation sequence:

```go
firstSealed  := rotate()   // seal the current active arena (A)
secondSealed := rotate()   // seal whatever just became active (B)
active.Store(nil)          // close the door — no new producers enter
```

The double rotation captures any producer that was bumped from A and retried into B. Setting active to `nil` causes all subsequent `beginWrite` calls to return `ErrWriteNoActiveArena` immediately.

Flush order: `secondSealed` first (producers who retried may still be draining), then `firstSealed`. Each flush is guarded by `waitForWriters` with the same configurable timeout.

## 11. Design Guarantees

| Property | Status |
|---|---|
| Lock‑free producer writes | ✓ |
| No per‑entry allocations | ✓ |
| No per‑producer reconfiguration on rotation | ✓ |
| Safe sealing without races | ✓ |
| Writers‑in‑flight correctness | ✓ |
| Bounded memory usage | ✓ |
| Explicit backpressure | ✓ |
| Clean shutdown via context cancellation | ✓ |
| Arenas with zero successful writes are never flushed | ✓ |
| Partial writes on the downstream `io.Writer` are handled | ✓ |

### Hot atomics separation

All hot atomics in `arena` and `Ingestor` are placed on separate 64‑byte cache lines via explicit padding:

```go
type arena struct {
    epoch  atomic.Uint64  // 8 bytes
    _      [56]byte       // pad to 64 bytes

    numberWriters atomic.Int32  // 4 bytes
    _             [60]byte      // pad to 64 bytes

    rollbackCounter atomic.Int32  // 4 bytes
    _               [60]byte      // pad to 64 bytes

    buf             []byte
    subRegionCursors [8]paddedCursor  // each cursor on its own 64-byte line
}
```

Without this separation, writes to adjacent atomics cause MESI coherence traffic — the cache line migrates between cores on every update.

### NUMA topology

On multi‑socket machines, producers on socket 1 writing to arena memory allocated on socket 0 pay a cross‑NUMA penalty on every access. Explicit NUMA‑aware allocation (`numactl` or `mmap` with NUMA policy) is planned for a future release.

## 12. Benchmarks and tests

Benchmarks measure most times end‑to‑end ingestion time including asynchronous flush completion, using a zero‑cost writer to isolate ingestion overhead from I/O.

### 1. Operating system

Benchmarks were run on Rocky 10.

### 2. Payload

Payload used in most cases is  32 bytes (fixed, to isolate allocator and contention effects).

### 3. Parallelism

Parallelism used is 16 with most significant GOMAXPROCS values 1 and 2.  
The jump between GOMAXPROCS = 1 and GOMAXPROCS = 2 helps understand the system behavior with even higher values.

### 4. Test Stabilization

Some tests use a stabilization detector that waits until the total written byte counter stops changing before recording results, ensuring all async flushes have completed.

### 5. Single Core

For latency-critical deployments of the bytearena ingestor:

Pin to a single core:

```go
 // Isolate ingestor logic to 1 core via runtime.LockOSThread + affinity
    go func() {
        runtime.LockOSThread()
       
        runIngestorLoop()
    }()
```

Use Arena2M for 1KB messages (reduces rotation frequency).
Expected latency (1KB message, pinned):

- p50:  48 ns
- p99:  116 ns
- p99.9: 276–368 ns

If horizontal scaling is needed, assess multiple single-core instances rather than increasing GOMAXPROCS.

Rationale: `Multi-core` execution introduces cache-coherency overhead on atomic operations (26% of CPU time), inflating tail latency by ~3,000×.  
`Single-core` execution eliminates this overhead while maintaining excellent throughput (~16M ops/sec at 62 ns/op).

## 13. Rationale for an ingestor‑based architecture  

Package ingestor implements a high‑throughput, allocation‑free logging and
telemetry ingestion pipeline. The design intentionally differs from traditional
per‑call logging libraries (e.g., zerolog, zap, slog), which emit one formatted
log entry per call and write directly to an io.Writer.  
It is appropriate for systems that require consistent performance under load and clear separation between ingestion and output responsibilities.

The author identified below advantages for this architecture:

1. Allocation‑free ingestion
   The ingestor writes log records directly into preallocated arena memory.
   No per‑record []byte allocations occur on the hot path. This eliminates
   transient heap objects, reduces GC load, and provides stable, predictable
   latency under sustained concurrency.

2. Decoupled ingestion and output
   Traditional loggers perform I/O coordination in the caller’s goroutine.  
   This couples application latency to logging cost.
   The ingestor separates these concerns: writers append to memory only, while
   a dedicated reader processes and flushes the inactive arena asynchronously.

3. Sharded concurrency model
   The ingestion arena is partitioned into independent shards. Writers operate
   on distinct shards without contending on shared state. This avoids the
   serialization and cacheline contention inherent in per‑call logging models,
   enabling throughput to scale with available CPU cores.

4. Deterministic double buffering
   The system maintains two arenas: one active for writers and one inactive for
   readers. A controlled arena flip provides a clear, race‑free snapshot
   boundary. Readers always operate on a stable, immutable arena, simplifying
   correctness and eliminating the need for fine‑grained synchronization.

5. Throughput and latency characteristics
   By removing per‑call allocations, avoiding shared writer coordination, and
   deferring formatting and output to a separate stage, the ingestor achieves
   significantly higher throughput and more deterministic latency than
   traditional logger designs. This architecture is intended for workloads
   where logging volume is high, concurrency is significant, and predictable
   performance is required.  

## 14. Design Lineage  

The ingestion architecture in this package draws on several established systems‑engineering techniques, each originating from different domains.  
This section documents the conceptual lineage of these designs.

### Double‑buffered memory regions

Double buffering is a long‑standing technique used in graphics pipelines, real‑time DSP systems, and high‑frequency trading engines to provide a stable snapshot boundary between producers and consumers. The ingestor adopts this principle at the arena level: one arena is active for writers while the other is reserved for readers and flushing. This ensures deterministic behavior and eliminates the need for fine‑grained synchronization.

### Sharded concurrency

Sharding is widely used in high‑performance systems to reduce contention by partitioning work across independent execution paths. Examples include Redis I/O threading, lock‑free hash maps, and multi‑producer ring buffers. The ingestor applies this concept by dividing each arena into multiple independent shards, allowing concurrent writers to operate without contending on shared state.

### Preallocated, arena‑based memory

Arena allocation is common in game engines, embedded systems, and low‑latency trading platforms where predictable memory behavior is required. By writing directly into preallocated arenas, the ingestor avoids per‑record allocations and eliminates transient heap objects, resulting in stable latency and minimal GC overhead.

### Epoch‑based coordination

Epoch flipping is a technique found in Read‑Copy‑Update (RCU) systems and some lock‑free memory reclamation strategies. The ingestor uses a simplified form of epoch coordination: an atomic flip switches the active arena, providing a clear and race‑free boundary between ingestion and flushing phases.

### Writers‑in‑flight tracking

Tracking active writers is a concept derived from hazard pointers and epoch‑based reclamation. In the ingestor, this mechanism ensures that an arena is not reclaimed or flushed while writers are still operating within it, preserving correctness without requiring locks.  

### Relation to RCU (Read‑Copy‑Update) systems

RCU uses epoch‑based coordination to provide readers with a stable,
immutable view while writers update data in parallel. The ingestor adopts
the same conceptual boundary: an atomic arena flip creates a new epoch,
ensuring that readers always operate on a consistent snapshot without
requiring locks or fine‑grained synchronization.

### Relation to the LMAX Disruptor systems

The Disruptor popularized sharded, cache‑friendly concurrency using a
preallocated ring buffer and sequence‑based coordination. The ingestor
shares the emphasis on preallocated memory and false‑sharing avoidance,
but differs fundamentally: it uses two global arenas instead of a single
cyclic buffer, and it provides deterministic snapshot boundaries rather
than continuous sequencing.

### Relation to kernel ring buffers systems

Operating systems often use fixed‑size ring buffers for logging and
tracing, emphasizing predictable memory behavior and minimal overhead.
The ingestor follows the same principle of allocation‑free writes into
preallocated memory, but extends it with sharded concurrency and a
double‑buffered design that cleanly separates ingestion from flushing.

The ingestor does not replicate any of these systems directly. Instead, it
synthesizes their underlying principles — epoch flipping, preallocated memory,
sharded concurrency, and stable snapshots — into a design tailored for
high‑throughput, allocation‑free log and telemetry ingestion.
