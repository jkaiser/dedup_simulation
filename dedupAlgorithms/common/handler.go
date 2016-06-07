package common

import "github.com/jkaiser/dedup_simulations/common"

// A FileEntry is the representation of a traced file and appears
// in a trace data set.
type FileEntry struct {
	TracedFile common.File
	Chunks     []common.Chunk
	isPartial  bool
}

// Main interface for all dedup algorithms
type FileDataHandler interface {
	BeginTraceFile(number int, name string)
	EndTraceFile(number int)
	HandleFiles(fileEntries <-chan *FileEntry, fileEntryReturn chan<- *FileEntry)
	Quit()
}
