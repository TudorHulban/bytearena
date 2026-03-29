package bytearena

// Test Case: Backpressure policy

// Test: Writer stops indefinitely after n writes.
// Verifies logger enters full mode
// with silently drop (common for high-perf logging).
