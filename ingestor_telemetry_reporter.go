package bytearena

// Reporter defines how the ingestor should announce dropped events.
type Reporter interface {
	ReportDrops(drops map[string]uint64)
}

var _ Reporter = &Ingestor{}
