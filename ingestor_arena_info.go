package bytearena

func (ing *Ingestor) GetArenaEpochs() (uint64, uint64) {
	return ing.arenaFirst.epoch.Load(), ing.arenaSecond.epoch.Load()
}
