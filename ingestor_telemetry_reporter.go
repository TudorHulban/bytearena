package bytearena

// Reporter defines how the ingestor should announce dropped events.
type Reporter interface {
	GetDrops(drops map[string]uint64)
}
