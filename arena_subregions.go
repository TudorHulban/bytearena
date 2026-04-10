package bytearena

type subRegion struct {
	Lower uint32
	Upper uint32
}

// newSubRegions partitions the arena into 8 contiguous sub-regions.
// Last region absorbs any remainder if arenaSize % 8 != 0.
func newSubRegions(arenaSize uint32) ([8]subRegion, uint32) {
	var regions [8]subRegion

	regionSize := arenaSize / uint32(len(regions))

	for ix := range regions {
		regions[ix] = subRegion{
			Lower: uint32(ix) * regionSize,   //nolint:gosec
			Upper: uint32(ix+1) * regionSize, //nolint:gosec
		}
	}

	// Guarantee full coverage: last region ends exactly at arenaSize
	regions[len(regions)-1].Upper = arenaSize

	return regions, regionSize
}

func precomputeThresholds(subregions [8]subRegion, sealPercentage uint32) [8]uint32 {
	var result [8]uint32

	for ix, subRegion := range subregions {
		result[ix] = subRegion.Lower + ((subRegion.Upper - subRegion.Lower) * sealPercentage / 100)
	}

	return result
}
