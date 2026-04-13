package bytearena

// writeRegion describes a reserved region inside an arena.
type writeRegion struct {
	arena *arena

	offset uint32
	size   uint32
}

// Buf returns the writable slice for the reserved region.
func (r writeRegion) Buf() []byte {
	return r.arena.buf[r.offset : r.offset+r.size]
}
