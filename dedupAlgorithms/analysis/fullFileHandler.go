package analysis

import "os"

import "bytes"
import "encoding/json"
import log "github.com/cihub/seelog"
import "hash"
import "hash/fnv"

import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type FullFileConfig struct {
	DataSet string
}

type FullFileStatistics struct {
	FileNumber int

	ChunkCount         int
	FileCount          int
	RedundantFileCount int
	UniqueFileCount    int

	RedundantFilesRatio float32

	TotalVolume          int64
	UniqueVolume         int64
	RedundantVolume      int64
	RedundantVolumeRatio float32
}

func (fs *FullFileStatistics) finish() {

	if fs.FileCount > 0 {
		fs.RedundantFilesRatio = float32(float64(fs.RedundantFileCount) / float64(fs.FileCount))
	} else {
		fs.RedundantFilesRatio = 0
	}

	if fs.TotalVolume > 0 {
		fs.RedundantVolumeRatio = float32(float64(fs.RedundantVolume) / float64(fs.TotalVolume))
	} else {
		fs.RedundantVolumeRatio = 0
	}
}

// Algorithm for full file duplicates. This represents a naive incrementel backup.
type FullFileHandler struct {
	fileIndex map[string]uint64

	fileHasher     hash.Hash64
	outputFilename string

	Statistics FullFileStatistics
	StatsList  []FullFileStatistics
}

func NewFullFileHandler(outFilename string) *FullFileHandler {
	return &FullFileHandler{
		fileIndex:      make(map[string]uint64, 1E6),
		fileHasher:     fnv.New64(),
		outputFilename: outFilename,
		StatsList:      make([]FullFileStatistics, 0),
	}
}

func (f *FullFileHandler) BeginTraceFile(number int, name string) {
	log.Info("begin Tracefile ", number, ": ", name)
	f.Statistics.FileNumber = number
}

func (f *FullFileHandler) EndTraceFile(number int) {
	log.Info("end Tracefile ", number)

	// collect statistics
	f.Statistics.finish()

	f.StatsList = append(f.StatsList, f.Statistics)

	if encodedStats, err := json.MarshalIndent(f.Statistics, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
	}

	// cleanup
	f.Statistics = FullFileStatistics{}
}

func (f *FullFileHandler) Quit() {
	if f.outputFilename == "" {
		return
	}

	if encodedStats, err := json.MarshalIndent(f.StatsList, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(f.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}

func (f *FullFileHandler) HandleFiles(fileEntries <-chan *common.FileEntry, fileEntryReturn chan<- *common.FileEntry) {

	for fileEntry := range fileEntries {
		if len(fileEntry.Chunks) > 0 {
			f.handleChunks(fileEntry)
		}
		fileEntryReturn <- fileEntry
	}
}

func (f *FullFileHandler) handleChunks(fileEntry *common.FileEntry) {

	var volume int64
	f.Statistics.FileCount++
	f.fileHasher.Reset()

	for i := range fileEntry.Chunks {
		f.Statistics.ChunkCount++
		volume += int64(fileEntry.Chunks[i].Size)
		f.fileHasher.Write(fileEntry.Chunks[i].Digest)
	}

	if hash, ok := f.fileIndex[fileEntry.TracedFile.Filename]; ok && (hash == f.fileHasher.Sum64()) {
		f.Statistics.RedundantFileCount++
		f.Statistics.RedundantVolume += volume

	} else {
		f.fileIndex[fileEntry.TracedFile.Filename] = f.fileHasher.Sum64()
		f.Statistics.UniqueFileCount++
		f.Statistics.UniqueVolume += volume
	}

	f.Statistics.TotalVolume += volume
}
