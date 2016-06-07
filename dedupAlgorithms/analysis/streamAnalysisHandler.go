package analysis

import "os"

import "bytes"
import "encoding/json"
import log "github.com/cihub/seelog"
import "hash"
import "hash/fnv"

import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type StreamAnalysisConfig struct {
	DataSet string
}

type StreamAnalysisStatistics struct {
	FileNumber int

	ChunkCount           int
	UniqueChunkCount     int
	RedundantChunkCount  int
	RedundantChunksRatio float32

	FileCount          int
	RedundantFileCount int
	UniqueFileCount    int

	RedundantFilesRatio float32

	TotalVolume          int64
	UniqueVolume         int64
	RedundantVolume      int64
	RedundantVolumeRatio float32
}

func (s *StreamAnalysisStatistics) add(that StreamAnalysisStatistics) {
	s.ChunkCount += that.ChunkCount
	/*s.UniqueChunkCount += that.UniqueChunkCount*/
	/*s.RedundantChunkCount += that.RedundantChunkCount*/

	s.FileCount += that.FileCount
	s.RedundantFileCount += that.RedundantFileCount
	s.UniqueFileCount += that.UniqueFileCount

	s.TotalVolume += that.TotalVolume
	s.UniqueVolume += that.UniqueVolume
}

func (s *StreamAnalysisStatistics) finish() {

	if s.ChunkCount > 0 {
		s.RedundantChunksRatio = float32(float64(s.RedundantChunkCount) / float64(s.ChunkCount))
	} else {
		s.RedundantChunksRatio = 0
	}

	if s.FileCount > 0 {
		s.RedundantFilesRatio = float32(float64(s.RedundantFileCount) / float64(s.FileCount))
	} else {
		s.RedundantFilesRatio = 0
	}

	if s.TotalVolume > 0 {
		s.RedundantVolumeRatio = float32(float64(s.RedundantVolume) / float64(s.TotalVolume))
	} else {
		s.RedundantVolumeRatio = 0
	}
}

// Algorithm for full file duplicates. This represents a naive incrementel backup.
type StreamAnalysisHandler struct {
	chunkIndex   map[[12]byte]uint32
	chunkHashBuf [12]byte // buffer for all chunk hashes. Used to make the Digest useable in maps.

	fileIndex      map[string]uint64
	fileHasher     hash.Hash64
	outputFilename string

	Statistics StreamAnalysisStatistics
	StatsList  []StreamAnalysisStatistics
}

func NewStreamAnalysisHandler(outFilename string) *StreamAnalysisHandler {
	return &StreamAnalysisHandler{
		chunkIndex:     make(map[[12]byte]uint32, 1E7),
		fileIndex:      make(map[string]uint64, 1E6),
		fileHasher:     fnv.New64(),
		outputFilename: outFilename,
		StatsList:      make([]StreamAnalysisStatistics, 0),
	}
}

func (sa *StreamAnalysisHandler) BeginTraceFile(number int, name string) {
	log.Info("begin Tracefile ", number, ": ", name)
	sa.Statistics.FileNumber = number
}

func (sa *StreamAnalysisHandler) EndTraceFile() {
	log.Info("end Tracefile ")

	// collect statistics
	sa.Statistics.finish()

	sa.StatsList = append(sa.StatsList, sa.Statistics)

	if encodedStats, err := json.MarshalIndent(sa.Statistics, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
	}

	// cleanup
	sa.Statistics = StreamAnalysisStatistics{}
}

func (sa *StreamAnalysisHandler) Quit() {
	if sa.outputFilename == "" {
		return
	}

	if encodedStats, err := json.MarshalIndent(sa.StatsList, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(sa.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}

func (sa *StreamAnalysisHandler) HandleFiles(fileEntries <-chan *common.FileEntry, fileEntryReturn chan<- *common.FileEntry) {

	for fileEntry := range fileEntries {
		if len(fileEntry.Chunks) > 0 {
			sa.handleChunks(fileEntry)
		}
		fileEntryReturn <- fileEntry
	}
}

func (sa *StreamAnalysisHandler) handleChunks(fileEntry *common.FileEntry) {

	var volume int64
	sa.Statistics.FileCount++
	sa.fileHasher.Reset()

	sa.Statistics.ChunkCount += len(fileEntry.Chunks)

	for i := range fileEntry.Chunks {
		copy(sa.chunkHashBuf[:], fileEntry.Chunks[i].Digest)
		// chunk stats
		if chunkCount, ok := sa.chunkIndex[sa.chunkHashBuf]; ok {
			sa.Statistics.RedundantChunkCount++
			sa.chunkIndex[sa.chunkHashBuf] = chunkCount + 1
		} else {
			sa.Statistics.UniqueChunkCount++
			sa.chunkIndex[sa.chunkHashBuf] = 1
		}

		// file stats
		sa.Statistics.ChunkCount++
		volume += int64(fileEntry.Chunks[i].Size)
		sa.fileHasher.Write(fileEntry.Chunks[i].Digest)
	}

	if hash, ok := sa.fileIndex[fileEntry.TracedFile.Filename]; ok && (hash == sa.fileHasher.Sum64()) {
		sa.Statistics.RedundantFileCount++
		sa.Statistics.RedundantVolume += volume

	} else {
		sa.fileIndex[fileEntry.TracedFile.Filename] = sa.fileHasher.Sum64()
		sa.Statistics.UniqueFileCount++
		sa.Statistics.UniqueVolume += volume
	}

	sa.Statistics.TotalVolume += volume
}
