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

type SortedMergeCIPageUsageConfig struct {
	IndexMemoryLimit        int64
	ChunkCacheSize          int
	ChunkIndexPageSize      int
	Seed                    int64
	NumClientsStepSize      int
	NumGenerationsBeforeSim int // number of generations to simulate before switching to the actual simulation. The actual simulation takes place with generation (NumGenerationsBeforeSim + 1).
	FinalPageSize           int // PageSize to use after  NumGenerationsBeforeSim generations

	DataSet string
}

func (c *SortedMergeCIPageUsageConfig) SanityCheck() {
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

type SortedMergeCIPageUsage struct {
	avgChunkSize int
	config       SortedMergeCIPageUsageConfig

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

func NewSortedMergeCIPageUsage(config SortedMergeCIPageUsageConfig,
	avgChunkSize int,
	chunkIndex *SortedChunkIndex,
	outFilename string,
	traceChan chan<- string) *SortedMergeCIPageUsage {

	sc := new(SortedMergeCIPageUsage)
	sc.avgChunkSize = avgChunkSize
	sc.config = config
	sc.chunkIndex = chunkIndex
	sc.randGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))
	sc.outputFilename = outFilename
	sc.traceChan = traceChan

	return sc
}

func (sc *SortedMergeCIPageUsage) BeginTraceFile(number int, name string) {
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

func (sc *SortedMergeCIPageUsage) EndTraceFile() {
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

func (sc *SortedMergeCIPageUsage) Quit() {
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

func (sc *SortedMergeCIPageUsage) HandleFiles(fileEntries <-chan *algocommon.FileEntry, fileEntryReturn chan<- *algocommon.FileEntry) {
	for fileEntry := range fileEntries {
		sc.handleChunks(fileEntry.Chunks)
		fileEntryReturn <- fileEntry
	}
}

func (sc *SortedMergeCIPageUsage) handleSingleFileEntry(fileEntry *algocommon.FileEntry) {
	sc.handleChunks(fileEntry.Chunks)
}

func (sc *SortedMergeCIPageUsage) handleChunks(chunks []common.Chunk) {

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
			if sc.chunkIndex.Add(&sc.chunkHashBuf, &sc.Statistics.ChunkIndexStats) {
				sc.Statistics.UniqueChunkCount++
			} else {
				sc.Statistics.RedundantChunkCount++
			}
		}
	}
}
