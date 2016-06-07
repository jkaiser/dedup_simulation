package sortedChunkIndexing

import "os"
import "bytes"
import "time"
import "math/rand"
import "encoding/json"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/common"
import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type SortedChunkIndexingConfig struct {
	IndexMemoryLimit          int64
	ChunkCacheSize            int
	ChunkIndexPageSize        int
	NegativeLookupProbability float32
	SimilarityThreshold       float32
	DataSet                   string
}

func (c *SortedChunkIndexingConfig) SanityCheck() {
	if c.IndexMemoryLimit == 0 {
		log.Warn("Index memory limit is 0. This disables that index!")
	}
	if c.ChunkCacheSize == 0 {
		log.Warn("Chunk cache size is 0. This disables that cache!")
	}
	if c.ChunkIndexPageSize == 0 {
		log.Warn("Index page size is 0!")
	}
	if c.NegativeLookupProbability == 0 {
		log.Warn("Negative probability is 0!")
	}
}

type SortedChunkIndexingStatistics struct {
	FileNumber int
	StreamID   string

	ChunkCount                int
	UniqueChunkCount          int
	RedundantChunkCount       int
	ConsecRedundantChunkCount int

	UniqueChunkRatio          float32
	RedundantChunkRatio       float32
	ConsecRedundantChunkRatio float32

	PredictedPageHitrate    float32
	AvgPredictedPageHitrate float32

	ChunkIndexStats SortedChunkIndexStatistics

	FalsePositives   int
	ChainRuns        int
	StorageCount     int64
	StorageByteCount int64

	UsedPagesCount       int
	PredictedPageUtility float32
	AvgPageUtility       float32
}

func (cs *SortedChunkIndexingStatistics) add(that SortedChunkIndexingStatistics) {

	cs.ChunkCount += that.ChunkCount
	cs.UniqueChunkCount += that.UniqueChunkCount
	cs.RedundantChunkCount += that.RedundantChunkCount
	cs.ConsecRedundantChunkCount += that.ConsecRedundantChunkCount

	cs.ChunkIndexStats.add(that.ChunkIndexStats)

	cs.FalsePositives += that.FalsePositives
	cs.ChainRuns += that.ChainRuns
	cs.StorageCount += that.StorageCount
	cs.StorageByteCount += that.StorageByteCount

	cs.UsedPagesCount += that.UsedPagesCount
}

// computes final statistics
func (cs *SortedChunkIndexingStatistics) finish() {

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

type SortedChunkIndexing struct {
	avgChunkSize int
	config       SortedChunkIndexingConfig

	chunkCache    *algocommon.ChunkCache
	chunkIndex    *SortedChunkIndex
	randGenerator *rand.Rand

	freeSpace uint32

	Statistics           SortedChunkIndexingStatistics
	chunkIndexStatistics SortedChunkIndexStatistics
	chunkHashBuf         [12]byte // buffer for all chunk hashes. Used to make the Digest useable in maps.
	currentHashBuf       [12]byte

	traceFileCount int
	StatsList      []SortedChunkIndexingStatistics
	outputFilename string
	traceChan      chan<- string

	currentChunkIndexPage uint32

	streamId string
}

func NewSortedChunkIndexing(config SortedChunkIndexingConfig,
	avgChunkSize int,
	chunkIndex *SortedChunkIndex,
	outFilename string,
	traceChan chan<- string) *SortedChunkIndexing {

	sc := new(SortedChunkIndexing)
	sc.avgChunkSize = avgChunkSize
	sc.config = config
	sc.chunkCache = algocommon.NewChunkCache(sc.config.ChunkCacheSize)
	sc.chunkIndex = chunkIndex
	sc.randGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))
	sc.outputFilename = outFilename
	sc.traceChan = traceChan

	return sc
}

func (sc *SortedChunkIndexing) BeginTraceFile(number int, name string) {
	log.Info("begin Tracefile ", number, ": ", name)
	sc.streamId = name
	sc.traceFileCount++
	sc.Statistics.StreamID = name
	sc.Statistics.FileNumber = sc.traceFileCount
}

func (sc *SortedChunkIndexing) EndTraceFile() {
	log.Info("end Tracefile")

	sc.chunkIndex.Statistics.finish()

	// collect statistics
	sc.chunkIndex.Finish(&sc.Statistics.ChunkIndexStats)

	sc.Statistics.ChunkIndexStats.finish()

	if sc.Statistics.UsedPagesCount > 0 {
		sc.Statistics.AvgPageUtility = float32(float64(sc.Statistics.ChunkIndexStats.DiskIndexHits) / (float64(sc.Statistics.UsedPagesCount) * float64(sc.chunkIndex.getNumChunksPerPage())))
	}
	sc.Statistics.finish()

	sc.chunkCache.Statistics.Reset()

	if encodedStats, err := json.MarshalIndent(sc.Statistics, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
	}

	sc.StatsList = append(sc.StatsList, sc.Statistics)
	sc.Statistics = SortedChunkIndexingStatistics{}
}

func (sc *SortedChunkIndexing) Quit() {
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

func (sc *SortedChunkIndexing) HandleFiles(fileEntries <-chan *algocommon.FileEntry, fileEntryReturn chan<- *algocommon.FileEntry) {
	for fileEntry := range fileEntries {
		sc.handleChunks(fileEntry.Chunks)
		fileEntryReturn <- fileEntry
	}
}

func (sc *SortedChunkIndexing) handleSingleFileEntry(fileEntry *algocommon.FileEntry) {
	sc.handleChunks(fileEntry.Chunks)
}

/*func (sc *SortedChunkIndexing) handleChunks(fileEntry *algocommon.FileEntry) {*/
func (sc *SortedChunkIndexing) handleChunks(chunks []common.Chunk) {
	var diceThrow float32

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

		// run algorithm
		ok := sc.chunkIndex.CheckWithoutChange(&sc.chunkHashBuf)
		if !ok {
			diceThrow = sc.randGenerator.Float32()
		}

		if ok || (!ok && (diceThrow < sc.config.NegativeLookupProbability)) {
			// The simulated Bloom filter gave a positive. Note that this also might be a false positive!
			sc.Statistics.ChainRuns++

			// cache miss
			// now _really_ do the lookup
			pagenum, _, memIndexHit := sc.chunkIndex.Check(&sc.chunkHashBuf, true, sc.currentChunkIndexPage, &sc.Statistics.ChunkIndexStats, sc.streamId)

			if ok { // duplicate
				sc.Statistics.RedundantChunkCount++
				if (!memIndexHit) && (sc.currentChunkIndexPage != pagenum) {
					sc.Statistics.UsedPagesCount++
					sc.currentChunkIndexPage = pagenum
				}

			} else { // unique chunks -> false positive
				sc.chunkIndex.Add(&sc.chunkHashBuf, &sc.Statistics.ChunkIndexStats)
				// don't update pagenum here because the return value of check is the position where it theoretically would be stored, but isn't.

				sc.Statistics.FalsePositives++
				sc.Statistics.UniqueChunkCount++
			}

		} else {
			// The simulated Bloom filter gave a negative
			sc.Statistics.UniqueChunkCount++

			sc.chunkIndex.Add(&sc.chunkHashBuf, &sc.Statistics.ChunkIndexStats)
		}

		/*sc.chunkCacheMutex.Lock()*/
		sc.chunkCache.Update(sc.chunkHashBuf)
		/*sc.chunkCacheMutex.Unlock()*/
	}
}
