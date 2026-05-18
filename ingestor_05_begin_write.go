package bytearena

type beginWrite func(ing *Ingestor, toReserve uint32) (writeRegion, error)

// beginWriteCounterCPU attempts to reserve n bytes in the current active arena.
//
// On success:
//   - writers-in-flight is incremented
//   - a region is returned
//   - caller MUST call EndWrite
//
// On failure:
//   - writers-in-flight is decremented
//   - reservation if reversed
//   - rollback counter is incremented
func (ing *Ingestor) beginWriteCounterCPU(toReserve uint32) (writeRegion, error) {
	if toReserve > ing.maxMessageSize {
		return writeRegion{},
			errWriteMessageTooLarge
	}

	arena := ing.active.Load()
	if arena == nil {
		return writeRegion{},
			errWriteNoActiveArena
	}

	arena.Enter()

	if ing.active.Load() != arena {
		arena.Leave()

		return writeRegion{},
			errWriteActiveArenaMismatch
	}

	// Round-robin: select sub-region using request counter (bit-mask for power-of-2)
	regionIdx := _CounterCoreCPU.Next() & 7

	subRegion := ing.subRegions[regionIdx]

	// Fast-fail if message does not fit this sub-region
	if toReserve > (subRegion.Upper - subRegion.Lower) {
		arena.AddRollback()
		arena.Leave()
		ing.signalFlush()

		return writeRegion{},
			errWriteSubRegionFull
	}

	offset, errReserve := ing.reserveBytes(&arena.subRegionCursors[regionIdx].value, toReserve, subRegion.Lower, subRegion.Upper)
	if errReserve != nil {
		arena.AddRollback()
		arena.Leave()
		ing.signalFlush()

		return writeRegion{},
			errReserve
	}

	// Success: return write handle
	return writeRegion{
			arena:  arena,
			offset: offset,
			size:   toReserve,
		},
		nil
}

func (ing *Ingestor) beginWriteCounterAtomic(toReserve uint32) (writeRegion, error) {
	if toReserve > ing.maxMessageSize {
		return writeRegion{},
			errWriteMessageTooLarge
	}

	arena := ing.active.Load()
	if arena == nil {
		return writeRegion{},
			errWriteNoActiveArena
	}

	arena.Enter()

	if ing.active.Load() != arena {
		arena.Leave()

		return writeRegion{},
			errWriteActiveArenaMismatch
	}

	// Round-robin: select sub-region using request counter (bit-mask for power-of-2)
	regionIdx := _CounterAtomic.Add(1) & 7

	subRegion := ing.subRegions[regionIdx]

	// Fast-fail if message does not fit this sub-region
	if toReserve > (subRegion.Upper - subRegion.Lower) {
		arena.AddRollback()
		arena.Leave()
		ing.signalFlush()

		return writeRegion{},
			errWriteSubRegionFull
	}

	offset, errReserve := ing.reserveBytes(&arena.subRegionCursors[regionIdx].value, toReserve, subRegion.Lower, subRegion.Upper)
	if errReserve != nil {
		arena.AddRollback()
		arena.Leave()
		ing.signalFlush()

		return writeRegion{},
			errReserve
	}

	// Success: return write handle
	return writeRegion{
			arena:  arena,
			offset: offset,
			size:   toReserve,
		},
		nil
}
