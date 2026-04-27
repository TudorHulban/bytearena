package bytearena

func (a *arena) getCursorValues() []uint32 {
	result := make([]uint32, len(a.subRegionCursors))

	for ix := range len(a.subRegionCursors) {
		result[ix] = a.subRegionCursors[ix].value.Load()
	}

	return result
}

func (a *arena) getSubregionLoads() ([]uint32, uint32) {
	result := make([]uint32, len(a.subRegionCursors))

	var total uint32

	for ix := range len(a.subRegionCursors) {
		result[ix] = a.subRegionCursors[ix].value.Load() - a.subRegions[ix].Lower

		total = total + result[ix]
	}

	return result, total
}
