package helpers

import (
	"sync"
	"testing"
)

func DumpSyncMap(t *testing.T, m *sync.Map) {
	t.Helper()

	m.Range(
		func(k, v any) bool {
			t.Logf("%v: %v", k, v)

			return true
		},
	)
}
