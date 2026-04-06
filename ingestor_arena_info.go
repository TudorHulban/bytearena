package bytearena

import "bytes"

func (m *Ingestor) GetArenaEpochs() (uint64, uint64) {
	return m.arenaFirst.epoch.Load(), m.arenaSecond.epoch.Load()
}

// getArenaData gathers all written data from the given arena's subregions.
// For each subregion, it reads buf[Lower : cursor] where cursor is the current
// atomic value from getCursorValues(). Returns a bytes.Buffer containing the
// concatenated data from all active subregions, with newline separators between
// shards to prevent message merging across boundaries.
//
// Note: This assumes messages do not span subregion boundaries. For typical
// small payloads (< region size), this is safe.
func (m *Ingestor) getArenaData(a *arena) *bytes.Buffer {
	cursors := a.getCursorValues()

	var result bytes.Buffer

	for i := range 8 {
		region := m.subRegions[i]
		cursor := cursors[i]

		// Skip empty subregions (cursor hasn't advanced from Lower bound)
		if cursor <= region.Lower {
			continue
		}

		// Extract data written in this subregion: buf[Lower : cursor]
		start := region.Lower
		end := cursor

		// Safety clamp: never read beyond the region's Upper bound
		if end > region.Upper {
			end = region.Upper
		}

		if start < end {
			result.Write(a.buf[start:end])
			// Add separator to ensure clean line scanning across shard boundaries
			result.WriteByte('\n')
		}
	}

	return &result
}
