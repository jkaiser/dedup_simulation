package common

// Representation of a chunked File
type File struct {
	Filename   string
	FileSize   uint64
	FileType   string
	FileLabel  string
	ChunkCount uint32
}
