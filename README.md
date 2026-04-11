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
Write → write → tryWrite → beginWrite → reserveBytes → deffered end write — signals end of a write by decrementing the number of writers
```

## 4. Producer Write Algorithm

For each ingestion:

1. Load the active arena pointer atomically.
2. Call `Enter()` — increments `numberWriters` on that arena.
3. Re‑check the active pointer; if it has rotated away since step 1, call `Leave()` and return `ErrWriteActiveArenaMismatch`.
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

If `N > maxMessageSize` (one sub‑region's capacity), the request is rejected immediately with `ErrWriteMessageTooLarge` — no CAS is attempted.

If the CAS loop determines the cursor is beyond the sub‑region limit:

- `rollbackCounter` is incremented.
- A non‑blocking flush signal is sent to the consumer.
- `ErrWriteSubRegionFull` is returned to the internal caller.

`Write` does not surface `ErrWriteSubRegionFull` to callers. Instead it captures the current epoch and yields via `runtime.Gosched()` until the consumer rotates the arena or resets it (epoch increment), then retries once. See §9a.  

Near the end of an arena many producers may attempt reservations concurrently. Some of these reservations may exceed the arena size and fail. This is expected behavior. Once the consumer seals the arena and rotates to the next one, producers will automatically obtain space in the new arena.

## 5. Consumer Monitoring Loop

The consumer's `tick()` is driven by three event sources:

1. A configurable periodic ticker (default 50 ms, `WithTickMilliseconds`).
2. A non‑blocking flush channel (`chFlush`) signaled by producers on rollback.
3. Context cancellation (initiates shutdown).

On each tick, `shouldSeal` is evaluated:

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

If `shouldSeal` returns false the tick is a no‑op — a quiet arena sits undisturbed.

Two signals trigger a seal:

- **Cursor threshold** — any sub‑region cursor reaches a pre‑computed watermark (default 90% of sub‑region capacity). Thresholds are computed once at startup as a `[8]uint32` array; no arithmetic at runtime.
- **Rollback pressure** — at least one producer failed to reserve space, indicating the arena is under pressure.

## 6. Sealing Protocol

Sealing is performed exclusively by the consumer via `rotate()`:

1. Read the current active arena pointer (call it X).
2. Determine the other arena (Y).
3. Atomically store Y as the new active pointer — all new writes now go to Y.
4. Return X as the sealed arena.

X is sealed: no new writes will start on it. Producers already past the active check are still finishing current writes; `numberWriters` tracks them.

## 7. Waiting for Writers to Finish

After sealing arena X the consumer calls `waitForWritersCtx`, which blocks until `numberWriters == 0` using adaptive backoff:

| Spin count | Strategy |
|---|---|
| < 20 | `PAUSE` instruction (x86 hardware hint) |
| < 100 | `runtime.Gosched()` |
| ≥ 100 | `time.Sleep(5 µs)` |

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

When `waitForWritersCtx` times out:

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

Flush order: `secondSealed` first (producers who retried may still be draining), then `firstSealed`. Each flush is guarded by `waitForWritersCtx` with the same configurable timeout.

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

## 11. Benchmarks

Benchmarks were run on Rocky 10. They measure end‑to‑end ingestion time including asynchronous flush completion, using a zero‑cost writer to isolate ingestion overhead from I/O.

```text
BenchmarkIngestor_Parallel-16    41500706    29.62 ns/op    8.631 Gb/s    0 B/op
```

- Payload: 32 bytes (fixed, to isolate allocator and contention effects).
- Parallelism: 16 goroutines (fixed, to simulate high contention independent of core count).
- The stabilization detector waits until the total written byte counter stops changing before recording results, ensuring all async flushes have completed.
