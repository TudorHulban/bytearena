package bytearena

import (
	"sync/atomic"
)

type Metrics struct {
	NumberRollbacks atomic.Uint64

	// below are picked directly
	// arena first epoch
	// arena second epoch
}

func (m *Metrics) IncrementRollback(with uint64) {
	m.NumberRollbacks.Add(with)
}
