package bytearena

func (m *Ingestor) Write(payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}

	// Fast path: try once
	if errWrite := m.write(
		uint32(len(payload)), //nolint:gosec

		func(destination []byte) {
			copy(destination, payload)
		},
	); errWrite != nil {
		return 0, errWrite
	}

	return len(payload), nil
}
