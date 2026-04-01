package bytearena

func (m *Ingestor) EstimateMessages(averagePayload uint32, writerWrites uint64) uint64 {
	if averagePayload <= 0 || writerWrites <= 0 {
		return 0
	}

	// How many messages fit in one arena.
	messagesPerArena := m.arenaSize / averagePayload
	if messagesPerArena <= 0 {
		return 0
	}

	e1, e2 := m.GetArenaEpochs()

	// Only as many arenas can be flushed as writer writes allow.
	usableEpochs := e1 + e2 - 1

	if writerWrites < usableEpochs {
		usableEpochs = writerWrites
	}

	return uint64(messagesPerArena) * usableEpochs
}
