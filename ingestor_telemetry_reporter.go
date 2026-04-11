package bytearena

// IReporter defines how the ingestor should announce dropped events.
type IReporter interface {
	ReportDrops(drops map[string]uint64)
	ReportMetrics()
}

var _ IReporter = &Ingestor{}
