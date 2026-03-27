package bytearena

import "encoding/json"

var _ Reporter = &Ingestor{}

func (m *Ingestor) GetDrops(drops map[string]uint64) {
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

	_, _ = m.writer.Write(append(logEntry, '\n'))
}

func (m *Ingestor) Telemetry(reporter Reporter) {
	reporter.GetDrops(
		m.telemetry.Snapshot(),
	)
}
