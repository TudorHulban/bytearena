package bytearena

import "sync/atomic"

type Metrics struct {
	NumberRollbacks atomic.Uint64
}

func (m *Metrics) IncrementRollback() {
	m.NumberRollbacks.Add(1)
}

func (m *Metrics) Reset() {
	m.NumberRollbacks.Store(0)
}
