# Arena Configuration Guide

## Recommended Arena Sizes and Seal Percentages

Based on test case 16 benchmarking with AMD Ryzen 7 5800H (16 threads), the following configurations provide optimal performance across different concurrency levels. These settings balance throughput, memory usage, and rotation frequency.  
The number of producers is for producers simultaneously pushing messages at high contention rate.

### Quick Reference Table

| Expected Producers | Recommended Arena Size | Recommended Seal % | Expected Throughput | Memory Usage (Double-buffered) |
|-------------------|----------------------|-------------------|---------------------|------------------------------|
| 1-4 | 1 MB | 85% | 13-15M writes/sec | 2 MB |
| 5-8 | 2 MB | 85-90% | 15-17M writes/sec | 4 MB |
| 9-16 | 4 MB | 90% | 2-4M writes/sec | 8 MB |
| 17-32 | 8 MB | 95% | 3-7M writes/sec | 16 MB |
| 33-64 | 16 MB | 95% | 4-6M writes/sec* | 32 MB |
| 65-128 | 32 MB | 98% | 2-4M writes/sec* | 64 MB |

*Projected performance based on scaling trends

### Detailed Performance Analysis

#### Low Concurrency (1-8 Producers)
For workloads with up to 8 concurrent writers, smaller arenas provide optimal performance with minimal memory overhead.

| Producers | Arena Size | Seal % | Throughput | Notes |
|-----------|------------|--------|------------|-------|
| 1 | 512 KB - 1 MB | 80-85% | 12-14M/s | Excellent for single-threaded use |
| 2 | 1 MB | 85% | 13-15M/s | Linear scaling |
| 4 | 1 MB | 85% | 14-15M/s | Peak efficiency |
| 8 | 2 MB | 85-90% | 16-17M/s | Sweet spot for moderate concurrency |

#### Medium Concurrency (16-32 Producers)
This range requires larger arenas to reduce rotation frequency while maintaining high throughput.

| Producers | Arena Size | Seal % | Throughput | Rotation Frequency |
|-----------|------------|--------|------------|-------------------|
| 16 | 2 MB | 85% | 1.0M/s | High (frequent rotation) |
| 16 | 4 MB | 90% | 2.1M/s | Medium |
| 16 | 8 MB | 95% | 4.1M/s | Low (optimal) |
| 32 | 2 MB | 85% | 0.6M/s | Very high (contention) |
| 32 | 4 MB | 90% | 1.2M/s | High |
| 32 | 8 MB | 95% | 3.2M/s | Medium |
| 32 | 16 MB | 95% | ~4.7M/s* | Low (projected) |

#### High Concurrency (64-128 Producers)
At high concurrency levels, arena size becomes critical for maintaining acceptable throughput.

| Producers | Arena Size | Seal % | Throughput | Notes |
|-----------|------------|--------|------------|-------|
| 64 | 2 MB | 85% | 0.25M/s | Not recommended |
| 64 | 4 MB | 90% | 0.46M/s | Marginal |
| 64 | 8 MB | 95% | 1.06M/s | Acceptable |
| 64 | 16 MB | 95% | ~2.0M/s* | Good |
| 128 | 8 MB | 95% | 0.47M/s | Adequate |
| 128 | 16 MB | 95% | ~1.1M/s* | Recommended |
| 128 | 32 MB | 98% | ~2.5M/s* | Optimal |

*Projected values based on scaling patterns observed in 2-8MB testing

### Key Principles

1. **Arena Size Impact**
   - Doubling arena size approximately doubles throughput up to 8MB
   - Returns diminish beyond 16MB for most workloads
   - 2MB to 8MB provides the best improvement curve (up to 3.5x gain)

2. **Seal Percentage Strategy**
   - Lower seal percentages (85%) for small arenas to prevent overflow
   - Higher seal percentages (95-98%) for large arenas to maximize utilization
   - Increase seal percentage as arena size grows

3. **Memory Considerations**
   - Double-buffering doubles the memory footprint (active + sealed)
   - 8MB arena = 16MB total memory
   - Acceptable for most production environments

### Performance Scaling Patterns

The following visualizations illustrate how throughput scales with arena size and producer count based on empirical benchmark data.

#### Throughput vs Arena Size at 32 Producers

| Arena Size | Throughput | Visual |
|------------|------------|--------|
| 2 MB | 3.17M writes/sec | `********************` |
| 4 MB | 4.69M writes/sec | `****************************` |
| 8 MB | 7.37M writes/sec | `***********************************************` |
| 16 MB | ~9.2M writes/sec* | `***********************************************************` |

*Projected based on scaling trend*

---

#### Throughput vs Arena Size at 64 Producers

| Arena Size | Throughput | Visual |
|------------|------------|--------|
| 2 MB | 1.06M writes/sec | `************` |
| 4 MB | 1.95M writes/sec | `**********************` |
| 8 MB | 3.66M writes/sec | `******************************************` |
| 16 MB | ~5.5M writes/sec* | `***************************************************************` |

*Projected based on scaling trend*

---

#### Throughput vs Producer Count (2MB Arena)

| Producers | Throughput | Visual |
|-----------|------------|--------|
| 4 | 14.8M/s | `****************************************************************************************************` |
| 8 | 16.9M/s | `******************************************************************************************************************` |
| 16 | 1.0M/s | `*****` |
| 32 | 0.6M/s | `***` |
| 64 | 0.25M/s | `*` |
| 128 | 1.03M/s | `*****` |

---

#### Throughput vs Producer Count (4MB Arena)

| Producers | Throughput | Visual |
|-----------|------------|--------|
| 4 | 14.6M/s | `**************************************************************************************************` |
| 8 | 16.4M/s | `****************************************************************************************************************` |
| 16 | 2.1M/s | `**********` |
| 32 | 1.2M/s | `******` |
| 64 | 0.46M/s | `**` |
| 128 | 1.06M/s | `*****` |

---

#### Throughput vs Producer Count (8MB Arena)

| Producers | Throughput | Visual |
|-----------|------------|--------|
| 4 | 14.8M/s | `**************************************************************************************************` |
| 8 | 16.9M/s | `******************************************************************************************************************` |
| 16 | 4.1M/s | `********************` |
| 32 | 3.2M/s | `****************` |
| 64 | 1.1M/s | `*****` |
| 128 | 0.5M/s | `**` |

---

#### Throughput vs Producer Count (16MB Arena - Projected)

| Producers | Throughput | Visual |
|-----------|------------|--------|
| 4 | ~14.9M/s | `**************************************************************************************************` |
| 8 | ~17.0M/s | `*******************************************************************************************************************` |
| 16 | ~6.5M/s | `********************************` |
| 32 | ~4.7M/s | `***********************` |
| 64 | ~2.0M/s | `**********` |
| 128 | ~1.1M/s | `*****` |

---

#### Scaling Summary

| Arena Size | Optimal Producer Range | Peak Throughput | Scaling Characteristic |
|------------|------------------------|-----------------|------------------------|
| 1-2 MB | 4-8 producers | 15-17M writes/sec | Best for low concurrency |
| 4 MB | 8-16 producers | 2-4M writes/sec | Moderate contention tolerance |
| 8 MB | 16-32 producers | 3-7M writes/sec | Good balance of size and throughput |
| 16+ MB | 32-128 producers | 2-6M writes/sec | Best for high concurrency workloads |

**Key Observations:**

- 8 producers achieves peak throughput across all arena sizes (16-17M writes/sec)
- 16+ producers see 5-10x throughput drop with small arenas (1-2MB)
- 8MB arena provides 2.3-3.5x improvement over 2MB at 32-64 producers
- Returns diminish beyond 8-16MB for most workloads

### Tuning Guidelines

- Start with the recommended size based on expected concurrency
- Monitor rollback counts - high rollbacks indicate arena is too small
- Increase seal percentage for larger arenas to improve utilization
- Benchmark with your workload - message size affects optimal configuration
- Consider CPU cores - size for max expected parallelism, not current load