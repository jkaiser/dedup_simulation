package blc

import "os"
import "bytes"
import "time"
import "sync"
import "math/rand"
import "encoding/json"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type BlockIndexFilterConfig struct {
	DataSet                          string
	BlockSize                        int
	BlockIndexPageSize               int
	ChunkIndexPageSize               int
	DiffCacheSize                    int
	BlockCacheSize                   int
	ChunkCacheSize                   int
	MinDiffValue                     int
	MaxBackupGenerationsInBlockIndex int
	NegativeLookupProbability        float32
}

type BlockIndexFilterStatistics struct {
	FileNumber int

	ChunkCount              int
	UniqueChunkCount        int
	RedundantChunkCount     int
	DetectedDuplicatesCount int
	DetectedUniqueChunks    int // the number of detected unique chunks (because they are hooks)
	WriteCacheHits          int

	UniqueChunkRatio    float32
	RedundantChunkRatio float32

	ChunkCacheStats         common.ChunkCacheStatistics
	ChunkIndexStats         common.ChunkIndexStatistics
	BlockLocalityCacheStats BlockLocalityCacheStatistics
	BlockIndexStats         BlockIndexStatistics

	StorageCount     int64
	StorageByteCount int64
}

func (b *BlockIndexFilterStatistics) add(that BlockIndexFilterStatistics) {
	b.ChunkCount += that.ChunkCount
	b.UniqueChunkCount += that.UniqueChunkCount
	b.RedundantChunkCount += that.RedundantChunkCount
	b.DetectedDuplicatesCount += that.DetectedDuplicatesCount
	b.DetectedUniqueChunks += that.DetectedUniqueChunks
	b.WriteCacheHits += that.WriteCacheHits

	b.ChunkCacheStats.Add(that.ChunkCacheStats)
	b.ChunkIndexStats.Add(that.ChunkIndexStats)
	b.BlockLocalityCacheStats.add(that.BlockLocalityCacheStats)
	b.BlockIndexStats.add(that.BlockIndexStats)

	b.StorageCount += that.StorageCount
	b.StorageByteCount += that.StorageByteCount
}

// computes final statistics
func (b *BlockIndexFilterStatistics) finish() {

	b.StorageCount = b.BlockIndexStats.StorageCount + b.ChunkIndexStats.StorageCount
	b.StorageByteCount = b.BlockIndexStats.StorageByteCount + b.ChunkIndexStats.StorageByteCount

	if b.ChunkCount > 0 {
		b.UniqueChunkRatio = float32(float64(b.UniqueChunkCount) / float64(b.ChunkCount))
		b.RedundantChunkRatio = float32(float64(b.RedundantChunkCount) / float64(b.ChunkCount))
	} else {
		b.UniqueChunkRatio = 0.0
		b.RedundantChunkRatio = 0.0
	}
}

type BlockIndexFilter struct {
	avgChunkSize     int
	chunkIndex       map[[12]byte]uint32
	chunkIndexRWLock *sync.RWMutex

	blockIndex                *BlockIndex
	chunkCache                *common.ChunkCache
	blockChunkCache           *BlockLocalityCache
	randGenerator             *rand.Rand
	Statistics                BlockIndexFilterStatistics
	chunkIndexStatistics      common.ChunkIndexStatistics
	config                    BlockIndexFilterConfig
	chunkHashBuf              [12]byte // buffer for all chunk hashes. Used to make the Digest useable in maps.
	currentBlockMapping       BlockMapper
	currentBlockMappingOffset uint32
	currentBlockId            uint32
	blockIdAtStartSlice       []uint32
	blockIdAtStopSlice        []uint32
	// TODO: blockIdAtStopSlice? Das gibt im Zweifel auch aufschluss, wieviel fetches auf fremde Streams gehen
	StatsList      []BlockIndexFilterStatistics
	outputFilename string

	streamId string
}

func NewBlockIndexFilter(config BlockIndexFilterConfig,
	avgChunkSize int,
	chunkInd map[[12]byte]uint32,
	chunkIndexLock *sync.RWMutex,
	bIndex *BlockIndex,
	outFilename string) *BlockIndexFilter {

	b := new(BlockIndexFilter)
	b.config = config
	b.avgChunkSize = avgChunkSize

	b.chunkIndex = chunkInd
	b.chunkIndexRWLock = chunkIndexLock
	b.blockIndex = bIndex

	b.chunkCache = common.NewChunkCache(config.ChunkCacheSize)
	b.blockChunkCache = NewBlockLocalityCache(config.MinDiffValue,
		config.BlockCacheSize,
		config.DiffCacheSize,
		b.blockIndex)

	b.randGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))
	b.currentBlockMapping = NewBlockMapping(config.BlockSize / avgChunkSize)
	b.chunkIndexStatistics = common.ChunkIndexStatistics{} // placed here because we have no ChunkIndex instance
	b.blockIdAtStartSlice = make([]uint32, 0)
	b.StatsList = make([]BlockIndexFilterStatistics, 0, 15)
	b.outputFilename = outFilename
	return b
}

func (b *BlockIndexFilter) BeginTraceFile(number int, name string) {
	log.Info("begin Tracefile ", number, ": ", name)
	b.streamId = name
	b.Statistics.FileNumber = number

	b.blockIdAtStartSlice = append(b.blockIdAtStartSlice, b.currentBlockId)
	b.blockChunkCache.beginNewGeneration(b.currentBlockId)
}

func (b *BlockIndexFilter) EndTraceFile() {
	log.Info("end Tracefile ")

	// finish open blocks
	if b.currentBlockMapping.Size() > 0 {
		b.blockIndex.Add(b.currentBlockId, b.currentBlockMapping)
		b.currentBlockMapping = NewBlockMapping(b.config.BlockSize / b.avgChunkSize)
		b.currentBlockMappingOffset = 0
		b.currentBlockId++
	}

	b.blockIdAtStopSlice = append(b.blockIdAtStopSlice, b.currentBlockId)

	// collect statistics
	b.chunkCache.Statistics.Finish()
	b.Statistics.BlockIndexStats.finish()
	b.Statistics.ChunkCacheStats = b.chunkCache.Statistics

	b.chunkIndexRWLock.RLock()
	b.chunkIndexStatistics.ItemCount = int64(len(b.chunkIndex))
	b.chunkIndexRWLock.RUnlock()

	b.chunkIndexStatistics.Finish()
	b.Statistics.ChunkIndexStats = b.chunkIndexStatistics
	b.Statistics.BlockLocalityCacheStats = b.blockChunkCache.finish()
	b.Statistics.finish()

	b.chunkCache.Statistics.Reset()
	b.chunkIndexStatistics.Reset()

	if encodedStats, err := json.MarshalIndent(b.Statistics, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
	}

	b.StatsList = append(b.StatsList, b.Statistics)
	b.Statistics = BlockIndexFilterStatistics{}

	// remove part of block index if necessary
	// out commented because of memcache migration
	//if len(b.blockIdAtStartSlice) >= b.config.MaxBackupGenerationsInBlockIndex {
	//	low := len(b.blockIdAtStartSlice) - b.config.MaxBackupGenerationsInBlockIndex
	//	b.blockIndex.Delete(b.blockIdAtStartSlice[low], b.blockIdAtStartSlice[low+1])
	//}
}

func (b *BlockIndexFilter) Quit() {
	if b.outputFilename == "" {
		return
	}

	if encodedStats, err := json.MarshalIndent(b.StatsList, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(b.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}

func (b *BlockIndexFilter) HandleFiles(fileEntries <-chan *common.FileEntry, fileEntryReturn chan<- *common.FileEntry) {

	for fileEntry := range fileEntries {
		b.handleChunks(fileEntry)
		fileEntryReturn <- fileEntry
	}
}

func (b *BlockIndexFilter) handleChunks(fileEntry *common.FileEntry) {

	var diceThrow float32

	for i := range fileEntry.Chunks {
		b.Statistics.ChunkCount++
		copy(b.chunkHashBuf[:], fileEntry.Chunks[i].Digest)

		// add chunk to current block mapping and put it into BlockIndex if full
		if (b.currentBlockMappingOffset + fileEntry.Chunks[i].Size) >= uint32(b.config.BlockSize) {
			// finish it and add to block index after check
			b.currentBlockMapping.Add(&b.chunkHashBuf)
			b.blockIndex.Add(b.currentBlockId, b.currentBlockMapping)
			b.currentBlockMapping = NewBlockMapping(b.config.BlockSize / b.avgChunkSize)
			b.currentBlockMappingOffset = fileEntry.Chunks[i].Size
			b.currentBlockId++
		} else {
			b.currentBlockMappingOffset += fileEntry.Chunks[i].Size
		}

		// run algorithm
		b.chunkIndexRWLock.RLock()
		blockHint, ok := b.chunkIndex[b.chunkHashBuf]
		b.chunkIndexRWLock.RUnlock()
		if !ok {
			diceThrow = b.randGenerator.Float32()
		}

		if ok || (!ok && (diceThrow < b.config.NegativeLookupProbability)) {
			// The simulated Bloom filter gave a positive. Note that this also might be a false positive!
			if b.chunkCache.Contains(b.chunkHashBuf) { // check LRU cache
				b.Statistics.RedundantChunkCount++
				b.Statistics.DetectedDuplicatesCount++
			} else if b.currentBlockMapping.Contains(&b.chunkHashBuf) { // check current block mapping
				b.Statistics.WriteCacheHits++
				b.Statistics.RedundantChunkCount++
				b.Statistics.DetectedDuplicatesCount++
			} else if b.blockChunkCache.contains(&b.chunkHashBuf, b.currentBlockId, &b.Statistics.BlockIndexStats, b.streamId) { // check BLC
				b.Statistics.RedundantChunkCount++
				b.Statistics.DetectedDuplicatesCount++
			} else { // cache miss
				b.blockChunkCache.update(&b.chunkHashBuf, b.currentBlockId, blockHint)

				b.chunkIndexStatistics.LookupCount++
				b.chunkIndexStatistics.StorageCount++
				b.chunkIndexStatistics.StorageByteCount += int64(b.config.ChunkIndexPageSize)

				if ok {
					b.Statistics.RedundantChunkCount++
					b.chunkIndexStatistics.StorageCountDuplicates++
				} else { // unique chunks -> false positive
					b.Statistics.UniqueChunkCount++
					b.chunkIndexStatistics.UpdateCount++ // since the assignment above creates a new entry
					b.chunkIndexStatistics.StorageCountUnique++
				}
			}

		} else {
			// The simulated Bloom filter gave a negative
			b.blockChunkCache.update(&b.chunkHashBuf, b.currentBlockId, blockHint)

			b.chunkIndexStatistics.UpdateCount++
			b.Statistics.UniqueChunkCount++
		}

		b.chunkIndexRWLock.Lock()
		b.chunkIndex[b.chunkHashBuf] = b.currentBlockId
		b.chunkIndexRWLock.Unlock()
		b.chunkCache.Update(b.chunkHashBuf)
		b.currentBlockMapping.Add(&b.chunkHashBuf)
	}
}
