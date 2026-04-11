package bytearena

import "encoding/json"

func (ing *Ingestor) ReportDrops(drops map[string]uint64) {
	if len(drops) == 0 {
		return
	}

	logEntry, _ := json.Marshal(
		map[string]any{
			"level":  "warn",
			"msg":    "ingestor_drops",
			"counts": drops,
		},
	)

	_, _ = ing.writerTelemetry.Write(append(logEntry, '\n'))
}

func (ing *Ingestor) ReportMetrics() {
	logEntry, _ := json.Marshal(
		map[string]any{
			"level":        "warn",
			"msg":          "ingestor_metrics",
			"rollbacks":    ing.Metrics.NumberRollbacks.Swap(0),
			"epoch_arena1": ing.arenaFirst.epoch.Load(),
			"epoch_arena2": ing.arenaSecond.epoch.Load(),
		},
	)

	_, _ = ing.writerTelemetry.Write(append(logEntry, '\n'))
}

func (ing *Ingestor) ReportTelemetry(reporter IReporter) {
	reporter.ReportDrops(
		ing.Registry.Snapshot(),
	)

	reporter.ReportMetrics()
}
