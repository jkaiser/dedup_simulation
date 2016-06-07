package common

// Representation of a chunk
type Chunk struct {
	Size      uint32
	Digest    []byte
	ChunkHash int64
}

func (c *Chunk) CopyFrom(from *Chunk) {
	c.Size = from.Size
	c.ChunkHash = from.ChunkHash
	c.Digest = make([]byte, len(from.Digest))
	copy(c.Digest, from.Digest)
}
