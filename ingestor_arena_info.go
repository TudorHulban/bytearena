package bytearena

func (m *Ingestor) GetArenaEpochs() (uint64, uint64) {
	return m.arenaFirst.epoch.Load(), m.arenaSecond.epoch.Load()
}
