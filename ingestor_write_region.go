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

// endWrite decrements writers-in-flight.
//
// endWrite must be called before the context is cancelled if wait on chIngestionEnd is used.
// TryWrite/beginWrite increments the writers-in-flight counter and flushOnShutdown will spin on it indefinitely.
// Using defer for endWrite is only safe when the caller is not also waiting for ingestion to drain.
func (*Ingestor) endWrite(r writeRegion) {
	r.arena.Leave()
}
