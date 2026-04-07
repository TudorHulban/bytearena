package bytearena

type SubRegion struct {
	Lower uint32
	Upper uint32
}

var SubRegions [8]SubRegion

// NewSubRegions partitions the arena into 8 contiguous sub-regions.
// Last region absorbs any remainder if arenaSize % 8 != 0.
func NewSubRegions(arenaSize uint32) ([8]SubRegion, uint32) {
	var regions [8]SubRegion

	regionSize := arenaSize / uint32(len(regions))

	for ix := range regions {
		regions[ix] = SubRegion{
			Lower: uint32(ix) * regionSize,   //nolint:gosec
			Upper: uint32(ix+1) * regionSize, //nolint:gosec
		}
	}

	// Guarantee full coverage: last region ends exactly at arenaSize
	regions[len(regions)-1].Upper = arenaSize

	return regions, regionSize
}
