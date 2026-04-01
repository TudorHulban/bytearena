package bytearena

import "encoding/json"

func (m *Ingestor) ReportDrops(drops map[string]uint64) {
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

	_, _ = m.writerTelemetry.Write(append(logEntry, '\n'))
}

func (m *Ingestor) ReportMetrics() {
	logEntry, _ := json.Marshal(
		map[string]any{
			"level":        "warn",
			"msg":          "ingestor_metrics",
			"rollbacks":    m.Metrics.NumberRollbacks.Swap(0),
			"epoch_arena1": m.arenaFirst.epoch.Load(),
			"epoch_arena2": m.arenaSecond.epoch.Load(),
		},
	)

	_, _ = m.writerTelemetry.Write(append(logEntry, '\n'))
}

func (m *Ingestor) ReportTelemetry(reporter Reporter) {
	reporter.ReportDrops(
		m.Registry.Snapshot(),
	)

	reporter.ReportMetrics()
}
