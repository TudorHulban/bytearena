package bytearena

import "encoding/json"

func (m *Ingestor) ReportTelemetry() {
	drops := m.telemetry.Snapshot()
	if len(drops) == 0 {
		return
	}

	// Because this is the consumer/telemetry thread, we can use
	// standard JSON encoding or your Sprintf helpers without
	// affecting the Producers' latency.
	logEntry, _ := json.Marshal(
		map[string]any{
			"level":  "warn",
			"msg":    "ingestor_drops",
			"counts": drops,
		},
	)

	_, _ = m.writer.Write(append(logEntry, '\n'))
}
