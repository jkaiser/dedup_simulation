package sortedChunkIndexing

import "os"
import "bytes"
import "time"
import "sync"
import "math/rand"
import "encoding/json"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/common"
import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type SortedMergeChunkIndexingConfig struct {
	IndexMemoryLimit   int64
	ChunkCacheSize     int
	ChunkIndexPageSize int
	DataSet            string
}

func (c *SortedMergeChunkIndexingConfig) SanityCheck() {
	if c.IndexMemoryLimit == 0 {
		log.Warn("Index memory limit is 0. This disables that index!")
	}
	if c.ChunkCacheSize == 0 {
		log.Warn("Chunk cache size is 0. This disables that cache!")
	}
	if c.ChunkIndexPageSize == 0 {
		log.Warn("Index page size is 0!")
	}
}

type SortedMergeChunkIndexingStatistics struct {
	FileNumber int
	StreamID   string

	ChunkCount                int
	UniqueChunkCount          int
	RedundantChunkCount       int
	ConsecRedundantChunkCount int

	UniqueChunkRatio          float32
	RedundantChunkRatio       float32
	ConsecRedundantChunkRatio float32

	ChunkIndexStats SortedChunkIndexStatistics

	ChainRuns        int
	StorageCount     int64
	StorageByteCount int64

	UsedPagesCount int
	AvgPageUtility float32
}

func (cs *SortedMergeChunkIndexingStatistics) add(that SortedMergeChunkIndexingStatistics) {

	cs.ChunkCount += that.ChunkCount
	cs.UniqueChunkCount += that.UniqueChunkCount
	cs.RedundantChunkCount += that.RedundantChunkCount
	cs.ConsecRedundantChunkCount += that.ConsecRedundantChunkCount

	cs.ChunkIndexStats.add(that.ChunkIndexStats)

	cs.ChainRuns += that.ChainRuns
	cs.StorageCount += that.StorageCount
	cs.StorageByteCount += that.StorageByteCount

	cs.UsedPagesCount += that.UsedPagesCount
}

// computes final statistics
func (cs *SortedMergeChunkIndexingStatistics) finish() {

	cs.ChunkIndexStats.finish()

	cs.StorageCount = cs.ChunkIndexStats.StorageCount
	cs.StorageByteCount = cs.ChunkIndexStats.StorageByteCount

	if cs.ChunkCount > 0 {
		cs.UniqueChunkRatio = float32(float64(cs.UniqueChunkCount) / float64(cs.ChunkCount))
		cs.RedundantChunkRatio = float32(float64(cs.RedundantChunkCount) / float64(cs.ChunkCount))
		cs.ConsecRedundantChunkRatio = float32(float64(cs.ConsecRedundantChunkCount) / float64(cs.ChunkCount))

	} else {
		cs.UniqueChunkRatio = 0.0
		cs.RedundantChunkRatio = 0.0
		cs.ConsecRedundantChunkRatio = 0.0
	}
}

type SortedMergeChunkIndexing struct {
	avgChunkSize int
	config       SortedMergeChunkIndexingConfig

	chunkIndex    *SortedChunkIndex
	randGenerator *rand.Rand

	freeSpace uint32

	Statistics                   SortedMergeChunkIndexingStatistics
	chunkIndexStatistics         SortedChunkIndexStatistics
	chunkHashBuf                 [12]byte // buffer for all chunk hashes. Used to make the Digest useable in maps.
	currentHashBuf               [12]byte
	biggestHashInCurrentOpenPage [12]byte

	traceFileCount int
	StatsList      []SortedMergeChunkIndexingStatistics
	outputFilename string
	traceChan      chan<- string

	currentChunkIndexPage uint32

	isFinishedMut              sync.RWMutex
	isFinished                 bool
	streamId                   string
	NextRequestedChunk         chan [12]byte
	NextBiggestChunkInOpenPage chan [12]byte // always holds the biggest chunk fp in the freshly opened page
}

func NewSortedMergeChunkIndexing(config SortedMergeChunkIndexingConfig,
	avgChunkSize int,
	chunkIndex *SortedChunkIndex,
	outFilename string,
	traceChan chan<- string) *SortedMergeChunkIndexing {

	sc := new(SortedMergeChunkIndexing)
	sc.avgChunkSize = avgChunkSize
	sc.config = config
	sc.chunkIndex = chunkIndex
	sc.randGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))
	sc.outputFilename = outFilename
	sc.traceChan = traceChan

	return sc
}

func (sc *SortedMergeChunkIndexing) BeginTraceFile(number int, name string) {
	log.Info("begin Tracefile ", number, ": ", name)
	sc.streamId = name
	sc.traceFileCount++
	sc.Statistics.StreamID = name
	sc.Statistics.FileNumber = sc.traceFileCount
	sc.chunkHashBuf = [12]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
	sc.biggestHashInCurrentOpenPage = [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	sc.NextRequestedChunk = make(chan [12]byte)
	sc.NextBiggestChunkInOpenPage = make(chan [12]byte)

	sc.isFinishedMut.RLock()
	sc.isFinished = false
	sc.isFinishedMut.RUnlock()
}

func (sc *SortedMergeChunkIndexing) EndTraceFile() {
	log.Info("end Tracefile")

	sc.chunkIndex.Statistics.finish()

	// collect statistics
	sc.chunkIndex.Finish(&sc.Statistics.ChunkIndexStats)

	sc.Statistics.ChunkIndexStats.finish()

	//if sc.Statistics.UsedPagesCount > 0 {
	//// TODO: das passt hier so nicht mehr, weil der CI nicht mehr mitzählt
	//sc.Statistics.AvgPageUtility = float32(float64(sc.Statistics.ChunkIndexStats.DiskIndexHits) / (float64(sc.Statistics.UsedPagesCount) * float64(sc.chunkIndex.getNumChunksPerPage())))
	//}
	sc.Statistics.finish()

	if encodedStats, err := json.MarshalIndent(sc.Statistics, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
	}

	sc.StatsList = append(sc.StatsList, sc.Statistics)
	sc.Statistics = SortedMergeChunkIndexingStatistics{}

	sc.isFinishedMut.RLock()
	sc.isFinished = true
	sc.isFinishedMut.RUnlock()

	close(sc.NextBiggestChunkInOpenPage)
	close(sc.NextRequestedChunk)
}

func (sc *SortedMergeChunkIndexing) Quit() {
	if sc.outputFilename == "" {
		return
	}

	if encodedStats, err := json.MarshalIndent(sc.StatsList, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(sc.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}

func (sc *SortedMergeChunkIndexing) HandleFiles(fileEntries <-chan *algocommon.FileEntry, fileEntryReturn chan<- *algocommon.FileEntry) {
	for fileEntry := range fileEntries {
		sc.handleChunks(fileEntry.Chunks)
		fileEntryReturn <- fileEntry
	}
}

func (sc *SortedMergeChunkIndexing) handleSingleFileEntry(fileEntry *algocommon.FileEntry) {
	sc.handleChunks(fileEntry.Chunks)
}

func (sc *SortedMergeChunkIndexing) handleChunks(chunks []common.Chunk) {

	for i := range chunks {
		sc.Statistics.ChunkCount++

		copy(sc.currentHashBuf[:], chunks[i].Digest)
		if bytes.Equal(sc.chunkHashBuf[:], sc.currentHashBuf[:]) {
			sc.Statistics.RedundantChunkCount++
			sc.Statistics.ConsecRedundantChunkCount++
			continue
		} else {
			copy(sc.chunkHashBuf[:], chunks[i].Digest)
		}

		// if in current page:
		if bytes.Compare(sc.currentHashBuf[:], sc.biggestHashInCurrentOpenPage[:]) > 0 {
			// bigger => need new page!
			// 1) markDone
			sc.NextRequestedChunk <- sc.currentHashBuf
			// 2) get new page
			if nbc, ok := <-sc.NextBiggestChunkInOpenPage; ok {
				sc.biggestHashInCurrentOpenPage = nbc
			} else {
				log.Error("nbc-channel was closed before own end!.")
			}
			sc.Statistics.UsedPagesCount++
		}

		// smaller than => is still in the loaded page
		sc.Statistics.ChainRuns++

		// run algorithm
		ok := sc.chunkIndex.CheckWithoutChange(&sc.chunkHashBuf) // this simulates a check in the current page AND the in-memory part of the chunk index. This ONLY causes no IO if we additionally assume that there are NO MERGE OPERATIONS during processing one page because only then new chunks from other handlers are in memory.Otherwise, the current page is outdated and we would have to check the disk-index to ensure exact deduplication.
		if ok {                                                  //duplicate
			sc.Statistics.RedundantChunkCount++
		} else {
			sc.Statistics.UniqueChunkCount++
			sc.chunkIndex.Add(&sc.chunkHashBuf, &sc.Statistics.ChunkIndexStats)
		}
	}
}
