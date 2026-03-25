package bytearena

// WriteRegion describes a reserved region inside an arena.
type WriteRegion struct {
	arena *arena

	offset uint32
	size   uint32
}

// Buf returns the writable slice for the reserved region.
func (r WriteRegion) Buf() []byte {
	return r.arena.buf[r.offset : r.offset+r.size]
}

// EndWrite decrements writers-in-flight.
func (*Ingestor) EndWrite(r WriteRegion) {
	r.arena.Leave()
}
