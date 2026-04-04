package bytearena

type SubRegion struct {
	Lower uint32
	Upper uint32
}

var SubRegions [8]SubRegion

// NewSubRegions partitions the arena into 8 contiguous sub-regions.
// Last region absorbs any remainder if arenaSize % 8 != 0.
func NewSubRegions(arenaSize uint32) [8]SubRegion {
	var regions [8]SubRegion

	regionSize := arenaSize / 8

	for i := range 8 {
		regions[i] = SubRegion{
			Lower: uint32(i) * regionSize,
			Upper: uint32(i+1) * regionSize,
		}
	}

	// Guarantee full coverage: last region ends exactly at arenaSize
	regions[7].Upper = arenaSize

	return regions
}
