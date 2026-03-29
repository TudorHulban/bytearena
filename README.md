# Arena‑Based Ingestor Architecture

The document describes the high‑performance, lock‑free, double‑buffered byte arena system based on atomic region reservation, and writers‑in‑flight tracking.  
It is designed for high throughput ingestion with predictable memory usage and bounded behavior under load.

## 1. Overview

The system consists of:

- Many producers (goroutines generating byte entries).
- One consumer (goroutine flushing arenas).
- Two fixed‑size arenas used in a double‑buffer rotation: Arena A, Arena B.

At any moment:

- One arena is active (producers write here)
- One arena is sealed (consumer flushes here)

This ensures:

- Lock‑free producer writes
- No per‑entry allocations
- Bounded memory usage
- Predictable backpressure behavior

## 2. Arena Structure

Each arena contains:

- A fixed‑size byte buffer
- An atomic cursor for region reservation
- An atomic writers‑in‑flight counter
- Metadata for consumer flushing

Arenas never grow, shrink, or reallocate.

## 3. Producer API

Producers never know about arenas directly.  
Producers do not need to be reconfigured during rotation.
They only interact with a stable API:

1. Write — signal start of a write
2. try write(N) — atomically reserve N bytes
3. deffered end write — signals end of a write by decrementing the number of writers

## 4. Producer Write Algorithm

For each ingestion:

1. load current arena context  (this gives the correct enter/leave closures)
2. enter()
3. increments writers‑in‑flight for that arena
4. reserve(N)
5. atomic fetch‑add on the arena’s cursor
6. returns a unique region [offset, offset+N)
7. write bytes directly into the arena buffer
8. leave()
9. decrement writers‑in‑flight

This path is lock‑free and wait‑free for producers.  
The closures must capture the arena pointer and not reload it internally.

### Producer asking for unavailable space

After reserving space:

```go
// increases the cursor to block space and provides the offset
offset := atomic.Add(cursor, N) - N  
```

The producer checks whether the reserved region fits inside the arena.

If `offset + N > arena_size`  
a. if `N > arena_size` the size is bigger than the arena (flooding) the request should be ignored and a corresponding flood error returned.  
b. the arena is sealed and the producer is rolled back and the switch to the free arena should allow the producer to retry to write.  

Near the end of an arena many producers may attempt reservations concurrently. Some of these reservations may exceed the arena size and fail. This is expected behavior. Once the consumer seals the arena and rotates to the next one, producers will automatically obtain space in the new arena.

## 5. Consumer Monitoring Loop

The consumer periodically checks:

1. the active arena’s cursor
2. whether it exceeds an “almost full” threshold
3. a rollback number counter that is incremented by producers when they cannot write.  
The rollback counter is reset as part of the arena reset.

If below threshold → continue.  
If above threshold → seal the arena.

## 6. Sealing Protocol

The sealing is done by the consumer only.  
When sealing Arena X:

1. switch the active arena to the other one (Y)
2. atomic pointer swap with all new writes go to Y and no new writes will start on X

Arena X is now sealed some producers may still be finishing writes but no new writes will begin.

This is the core of the double‑buffer rotation.

## 7. Waiting for Writers to Finish (with Context Cancellation)

After sealing Arena X:

The consumer waits until that `writers_in_flight[X] == 0`.  
This guarantees:

- all producers that started writes on X have finished
- no producer is touching X anymore

### Context cancellation support  

If the application is shutting down, the consumer must:

- stop waiting earlier if the context is canceled
- flush whatever is currently in both arenas
- exit cleanly

This ensures messages are flushed even during shutdown.

## 8. Flushing the Sealed Arena

Once safe to flush:

1. Read X’s cursor
2. Flush bytes [0 .. cursor) to the sink
3. Reset cursor to 0
4. Reset writers‑in‑flight to 0
5. Mark X as the next empty arena

The system is now ready for the next rotation.

## 9. Backpressure Behavior

If both arenas are:

- sealed
- full
- or otherwise unavailable

Then producers do not receive a region to write.

This is correct and necessary for a bounded system.  

Possible policies:

- drop the message ingested with emiting log entries about the drop through other methods / loggers
- block until space is available
- return an error
- degrade to minimal ingestion

The system does not allocate new memory or create hidden queues.

## 10. Design Guarantees

This architecture provides:

- Lock‑free producer writes
- No per‑entry allocations
- No per‑producer reconfiguration
- Safe sealing without races
- Writers‑in‑flight correctness
- Bounded memory usage
- Explicit backpressure
- Clean shutdown via context cancellation
- Arenas with zero successful writes are never flushed
- Partial writes on the downward io.Writer are handled

Memory ordering is missing as the code relies on Go atomic sequentially consistency for correctness.

### Hot atomics separation

The arena struct should separate hot atomics to avoid cache line contention.  
The atomics should be in different cache lines:  

```go
type arena struct {
    cursor  atomic.Int32
    _      [60]byte

    writers atomic.Int32
    _      [60]byte

    buf []byte
}
```

Otherwise heavy ingestion causes cache contention. If they sit in the same cache line, every update causes cache line bouncing between cores. The cache line constantly migrates between cores, creating MESI coherence traffic.

Cache line is 64 bytes. atomic.Int64 is 8 bytes value therefore 8 bytes + padding = 64 bytes.  
This ensures each atomic occupies its own line relative to the struct layout.

### NUMA topology

On multi-socket machines, if producers on socket 1 are writing to an arena whose memory is allocated on socket 0, you pay a significant cross-NUMA penalty on every memcopy.  
Arena memory allocated on the socket where producers run, with the consumer following the data. numactl or explicit mmap with NUMA policy at arena allocation time is the fix but at current is planned for future releases.

## 11. Benchmarks

Benchmark results provide ingestion only time with no I/O timings.
