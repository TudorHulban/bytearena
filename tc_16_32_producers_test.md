# Arena Configuration Guide

## Overview

Benchmark hardware: AMD Ryzen 5 5600U (6 cores / 12 threads).  
All throughput figures are sustained writes/sec over a 3-second fixed window.  
Write path allocates **0 bytes per operation** — the ingestor is allocation-free on the hot path.

The number of producers refers to goroutines simultaneously pushing messages at high contention rate.

---

## Quick Reference

| Expected Producers | Recommended Arena | Expected Throughput | Double-buffer Footprint |
|---|---|---|---|
| 1–4 | 1 MB | 15–15.5M writes/sec | 2 MB |
| 5–8 | 1–2 MB | 15–17M writes/sec | 2–4 MB |
| 9–16 | 4 MB | 8–8.3M writes/sec | 8 MB |
| 17–32 | 8 MB | ~6M writes/sec (extrapolated) | 16 MB |

**Seal percentage: set to anything in 90–99%**. Empirically it has no measurable effect on throughput under load. See [Seal Percentage](#3-seal-percentage-is-inert) for why.

---

## Key Findings

### 1. The contention cliff is at the hardware thread count

The 5600U has 6 physical cores / 12 hyperthreads. The throughput cliff falls between 8 and 16 producers — exactly straddling the SMT boundary. This is not a design flaw; it is the point where goroutines start sharing physical cores and CAS contention on `cursor` and `numberWriters` becomes brutal.

The cliff is hardware-specific. On an 8-core / 16-thread machine (e.g. Ryzen 7 5800H) it falls between 16 and 32 producers for the same reason.

### 2. Arena size is the only lever that matters

At the cliff, arena size is the single variable that controls how badly throughput degrades. Larger arenas mean fewer rotations per second, which means the consumer spends less time in `waitForWriters` blocking new CAS attempts.

At 16 producers, the effect is 40x across the tested range:

| Arena | Throughput | vs 100K |
|---|---|---|
| 100K | 206K writes/sec | baseline |
| 500K | 1.05M writes/sec | 5× |
| 1M | 2.07M writes/sec | 10× |
| 2M | 4.15M writes/sec | 20× |
| 4M | 8.30M writes/sec | **40×** |

The relationship is linear: doubling arena size roughly doubles throughput at 16+ producers. This holds consistently across the full dataset.

### 3. Seal percentage is inert

Across every combination of arena size and producer count, varying seal percentage from 90% to 99% produces differences within run-to-run variance (< ±3%). Example at 4M arena, 16 producers:

```sh
S_90:  8,325,168 writes/sec
S_95:  8,264,533 writes/sec
S_97:  8,263,313 writes/sec
S_99:  8,319,093 writes/sec
```

The reason: under any sustained load, `rollbackCounter` fires before the cursor threshold is ever reached. Producers fill the arena faster than the percentage threshold matters — `shouldSeal` short-circuits on the rollback check regardless of the configured percentage. The threshold only activates in low-write-rate scenarios (single producer, large arena, sparse writes) where tuning it would not matter much.

---

## Detailed Performance Data

### Low Concurrency: 1–8 Producers

Throughput is stable and high across all arena sizes ≥ 500K. The gain from 500K → 1M is real; gains beyond 1M are marginal at this producer count.

| Producers | Arena | Throughput |
|---|---|---|
| 4 | 100K | 12.4–12.6M/sec |
| 4 | 500K | 14.7–14.9M/sec |
| 4 | 1M | 15.1–15.3M/sec |
| 4 | 2M | 15.0–15.5M/sec |
| 4 | 4M | 15.3M/sec |
| 8 | 100K | 13.2–13.4M/sec |
| 8 | 500K | 15.7–16.1M/sec |
| 8 | 1M | 16.4–17.1M/sec ← peak |
| 8 | 2M | 16.7–16.8M/sec |
| 8 | 4M | 16.7–16.9M/sec |

**Recommendation:** 1M arena. Going larger wastes memory without throughput gain. Going below 500K costs ~15% throughput for no benefit.

### Medium Concurrency: 16 Producers

This is where arena size becomes critical. The system has crossed the SMT boundary.

| Arena | Throughput | Notes |
|---|---|---|
| 100K | ~206K/sec | Severe contention, not recommended |
| 500K | ~1.03M/sec | 5× over 100K |
| 1M | ~2.06M/sec | 10× over 100K |
| 2M | ~4.15M/sec | 20× over 100K |
| 4M | ~8.30M/sec | **40× over 100K, recommended minimum** |

**Recommendation:** 4M arena. The 2× cost over 2M arena buys another 2× throughput.

### High Concurrency: 32 Producers

32 producers on a 12-thread machine means every goroutine is time-sliced. Throughput figures reflect scheduler overhead as much as ingestor design. The arena-size scaling relationship holds but absolute numbers are lower.

| Arena | Throughput |
|---|---|
| 100K | ~80K/sec |
| 500K | ~404K/sec |
| 1M | ~799K/sec |
| 2M | ~1.69M/sec |
| 4M | ~3.20M/sec |

**Recommendation:** 4M+ arena, but reconsider whether 32 producer goroutines is the right architecture for your workload. A bounded producer pool (≤8 goroutines feeding the ingestor) will outperform 32 unbounded producers by an order of magnitude on consumer grade hardware.

---

## Contention Resilience by Arena Size

How badly does throughput drop when crossing the 8→16 producer boundary?

| Arena | P=8 | P=16 | Drop |
|---|---|---|---|
| 100K | 13.4M/sec | 206K/sec | −98% |
| 500K | 16.1M/sec | 1.03M/sec | −94% |
| 1M | 17.1M/sec | 2.07M/sec | −88% |
| 2M | 16.8M/sec | 4.15M/sec | −75% |
| 4M | 16.9M/sec | 8.30M/sec | **−51%** |

The 4M arena is the only configuration where the system remains practically usable past the SMT boundary. A 51% drop sounds bad until you compare it to 98%.

---

## Tuning Checklist

1. **Count your real producers.** Not goroutine count — how many goroutines are simultaneously blocked on `Write` at peak load.
2. **Pick arena size from the table above.** It is the only variable that matters.
3. **Set seal percentage to 95% and never touch it again.** It has no measurable effect.
4. **If you have > 8 simultaneous producers**, consider whether a bounded worker pool is viable. 8 workers feeding the ingestor from a channel will outperform 32 direct producers significantly.
5. **Memory budget:** arena size × 2 (double-buffer). A 4M arena costs 8MB. That is the total cost; there are no per-write allocations.

---

## What the Benchmarks Do Not Cover

- **NUMA systems**: on a dual-socket machine, remote cache line ownership for `active` (the atomic pointer producers load on every write) would add ~40ns per write for producers on the remote socket. Arena placement and the `active` pointer's cache line residency would need separate analysis.
- **Write sizes > 1KB**: the benchmarks use a fixed 25-byte payload. Very large writes amortize the Enter/Leave overhead differently.
- **Bursty workloads**: all figures are sustained throughput. A bursty workload (idle → 16 producers → idle) will have different rotation dynamics than sustained hammering.