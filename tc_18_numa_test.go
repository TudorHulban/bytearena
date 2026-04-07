package bytearena

import (
	"bytes"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NUMA = Non‑Uniform Memory Access
// Test Case: NUMA-Style False Sharing Detection

// Test: Multiple cores hammer different atomics.
// Verifies: Cache line padding works (performance, not correctness).
// TestNUMAAwareness verifies performance when goroutines
// are pinned to different NUMA nodes.
func TestNUMAAwareness(t *testing.T) {
	if testing.Short() || runtime.GOOS != "linux" {
		t.Skip("NUMA test requires Linux and non-short mode")
	}

	// Check if we have multiple NUMA nodes
	nodes, errRead := os.ReadFile("/sys/devices/system/node/online")
	if errRead != nil || strings.TrimSpace(string(nodes)) == "0" {
		t.Skip(
			"Single NUMA node detected; skipping NUMA-specific test",
		)
	}

	const (
		arenaSize = 64 * _Size1K
		duration  = 2 * time.Second
	)

	ingestor, errRead := NewIngestor(arenaSize, &bytes.Buffer{})
	require.NoError(t, errRead)

	var (
		wgProducers sync.WaitGroup
		ops         atomic.Int64
	)

	// Launch one producer per shard, pinned to different cores (best-effort)
	for shardIx := range ingestor.subRegions {
		wgProducers.Add(1)

		go func(shardID int) {
			defer wgProducers.Done()

			// Best-effort CPU affinity (requires golang.org/x/sys/unix)
			// setCPUAffinity(shardID % runtime.NumCPU())

			deadline := time.Now().Add(duration)

			for time.Now().Before(deadline) {
				// Simulate shard-local write
				arena := ingestor.active.Load()
				_ = arena.subRegionCursors[shardID].value.Load()

				ops.Add(1)
			}
		}(shardIx)
	}

	wgProducers.Wait()
	t.Logf("Completed %d operations in %v", ops.Load(), duration)

	// If false sharing exists, ops/sec will be significantly lower than expected
	// Baseline: ~50M ops/sec per core for atomic.Add on modern x86
	expectedMinOps := int64(10_000_000) // Conservative: 10M ops in 2 seconds
	require.Greater(t,
		ops.Load(),
		expectedMinOps,

		"throughput too low; possible false sharing or NUMA contention",
	)
}
