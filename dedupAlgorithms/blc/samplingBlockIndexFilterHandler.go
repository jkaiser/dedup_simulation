package blc

import "os"
import "bytes"
import "sync"
import "encoding/json"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type SamplingBlockIndexFilterConfig struct {
	DataSet                          string
	BlockSize                        int
	BlockIndexPageSize               int
	ChunkIndexPageSize               int
	DiffCacheSize                    int
	BlockCacheSize                   int
	ChunkCacheSize                   int
	MinDiffValue                     int
	SampleFactor                     int
	MaxBackupGenerationsInBlockIndex int
	NegativeLookupProbability        float32
}

type SamplingBlockIndexFilterStatistics struct {
	FileNumber int

	ChunkCount                int
	UniqueChunkCount          int
	RedundantChunkCount       int
	DetectedDuplicatesCount   int
	UndetectedDuplicatesCount int
	DetectedUniqueChunks      int // the number of detected unique chunks (because they are hooks)
	WriteCacheHits            int

	UniqueChunkRatio                         float32
	RedundantChunkRatio                      float32
	UndetectedDuplicatesRatio                float32 // the number of stored duplicates compared to the total number of chunks
	UndetectedDuplicatesToAllDuplicatesRatio float32 // the number of undetected duplicates compare to the number of duplicates

	ChunkCacheStats         common.ChunkCacheStatistics
	ChunkIndexStats         common.ChunkIndexStatistics
	BlockLocalityCacheStats BlockLocalityCacheStatistics
	BlockIndexStats         BlockIndexStatistics

	StorageCount     int64
	StorageByteCount int64
}

func (s *SamplingBlockIndexFilterStatistics) add(that SamplingBlockIndexFilterStatistics) {
	s.ChunkCount += that.ChunkCount
	s.UniqueChunkCount += that.UniqueChunkCount
	s.RedundantChunkCount += that.RedundantChunkCount
	s.DetectedDuplicatesCount += that.DetectedDuplicatesCount
	s.UndetectedDuplicatesCount += that.UndetectedDuplicatesCount
	s.DetectedUniqueChunks += that.DetectedUniqueChunks
	s.WriteCacheHits += that.WriteCacheHits

	s.ChunkCacheStats.Add(that.ChunkCacheStats)
	s.ChunkIndexStats.Add(that.ChunkIndexStats)
	s.BlockLocalityCacheStats.add(that.BlockLocalityCacheStats)
	s.BlockIndexStats.add(that.BlockIndexStats)

	s.StorageCount += that.StorageCount
	s.StorageByteCount += that.StorageByteCount
}

// computes final statistics
func (s *SamplingBlockIndexFilterStatistics) finish() {

	s.StorageCount = s.BlockIndexStats.StorageCount
	s.StorageByteCount = s.BlockIndexStats.StorageByteCount

	if s.ChunkCount > 0 {
		s.UniqueChunkRatio = float32(float64(s.UniqueChunkCount) / float64(s.ChunkCount))
		s.RedundantChunkRatio = float32(float64(s.RedundantChunkCount) / float64(s.ChunkCount))
		s.UndetectedDuplicatesRatio = float32(float64(s.UndetectedDuplicatesCount) / float64(s.ChunkCount))
	} else {
		s.UniqueChunkRatio = 0.0
		s.RedundantChunkRatio = 0.0
		s.UndetectedDuplicatesRatio = 0.0
	}

	if s.RedundantChunkCount > 0 {
		s.UndetectedDuplicatesToAllDuplicatesRatio = float32(float64(s.UndetectedDuplicatesCount) / float64(s.RedundantChunkCount))
	} else {
		s.UndetectedDuplicatesToAllDuplicatesRatio = 0.0
	}
}

type SamplingBlockIndexFilter struct {
	avgChunkSize              int
	chunkIndex                map[[12]byte]uint32
	chunkIndexRWLock          *sync.RWMutex
	blockIndex                *BlockIndex
	chunkCache                *common.ChunkCache
	blockChunkCache           *BlockLocalityCache
	Statistics                SamplingBlockIndexFilterStatistics
	chunkIndexStatistics      common.ChunkIndexStatistics
	config                    SamplingBlockIndexFilterConfig
	chunkHashBuf              [12]byte // buffer for all chunk hashes. Used to make the Digest useable in maps.
	currentBlockMapping       BlockMapper
	currentBlockMappingOffset uint32
	currentBlockId            uint32
	traceFileCount            int
	blockIdAtStartSlice       []uint32
	StatsList                 []SamplingBlockIndexFilterStatistics
	outputFilename            string

	streamId string
}

func NewSamplingBlockIndexFilter(config SamplingBlockIndexFilterConfig,
	avgChunkSize int,
	chunkInd map[[12]byte]uint32,
	chunkIndexLock *sync.RWMutex,
	bIndex *BlockIndex,
	outFilename string) *SamplingBlockIndexFilter {

	s := new(SamplingBlockIndexFilter)
	s.config = config
	s.avgChunkSize = avgChunkSize
	s.chunkIndex = chunkInd
	s.chunkIndexRWLock = chunkIndexLock
	s.blockIndex = bIndex

	s.chunkCache = common.NewChunkCache(config.ChunkCacheSize)
	s.blockChunkCache = NewBlockLocalityCache(config.MinDiffValue,
		config.BlockCacheSize,
		config.DiffCacheSize,
		s.blockIndex)

	s.currentBlockMapping = NewBlockMapping(config.BlockSize / avgChunkSize)
	s.chunkIndexStatistics = common.ChunkIndexStatistics{} // placed here because we have no ChunkIndex instance
	s.blockIdAtStartSlice = make([]uint32, 0)
	s.StatsList = make([]SamplingBlockIndexFilterStatistics, 0, 15)
	s.outputFilename = outFilename
	return s
}

func (s *SamplingBlockIndexFilter) BeginTraceFile(number int, name string) {
	log.Info("begin Tracefile ", number, ": ", name)
	s.streamId = name
	s.Statistics.FileNumber = number
	s.blockIdAtStartSlice = append(s.blockIdAtStartSlice, s.currentBlockId)
	s.blockChunkCache.beginNewGeneration(s.currentBlockId)
}

func (s *SamplingBlockIndexFilter) EndTraceFile() {
	log.Info("end Tracefile")

	// finish open blocks
	if s.currentBlockMapping.Size() > 0 {
		s.blockIndex.Add(s.currentBlockId, s.currentBlockMapping)
		s.currentBlockMapping = NewBlockMapping(s.config.BlockSize / s.avgChunkSize)
		s.currentBlockMappingOffset = 0
		s.currentBlockId++
	}

	// collect statistics
	s.chunkCache.Statistics.Finish()
	s.Statistics.ChunkCacheStats = s.chunkCache.Statistics
	s.Statistics.BlockIndexStats.finish()
	s.chunkIndexRWLock.RLock()
	s.chunkIndexStatistics.ItemCount = int64(len(s.chunkIndex))
	s.chunkIndexRWLock.RUnlock()
	s.chunkIndexStatistics.Finish()
	s.Statistics.ChunkIndexStats = s.chunkIndexStatistics
	s.Statistics.BlockLocalityCacheStats = s.blockChunkCache.finish()
	s.Statistics.finish()

	s.chunkCache.Statistics.Reset()
	s.chunkIndexStatistics.Reset()

	if encodedStats, err := json.MarshalIndent(s.Statistics, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
	}

	s.StatsList = append(s.StatsList, s.Statistics)
	s.Statistics = SamplingBlockIndexFilterStatistics{}

	// remove part of block index if necessary
	if len(s.blockIdAtStartSlice) >= s.config.MaxBackupGenerationsInBlockIndex {
		low := len(s.blockIdAtStartSlice) - s.config.MaxBackupGenerationsInBlockIndex
		_ = low
		//out commented because of memcache migration
		//s.blockIndex.Delete(s.blockIdAtStartSlice[low], s.blockIdAtStartSlice[low+1])
	}
}

func (s *SamplingBlockIndexFilter) Quit() {
	if s.outputFilename == "" {
		return
	}

	if encodedStats, err := json.MarshalIndent(s.StatsList, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(s.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}

func (s *SamplingBlockIndexFilter) isHook(fp *[12]byte) bool {
	for i := 0; i < s.config.SampleFactor/8; i++ {
		if fp[i] != byte(0) {
			return false
		}
	}

	shifts := uint32(s.config.SampleFactor % 8)
	mask := byte(^(0xFF << shifts))
	return (fp[s.config.SampleFactor/8] & mask) == 0
}

func (s *SamplingBlockIndexFilter) HandleFiles(fileEntries <-chan *common.FileEntry, fileEntryReturn chan<- *common.FileEntry) {

	for fileEntry := range fileEntries {
		s.handleChunks(fileEntry)
		fileEntryReturn <- fileEntry
	}
}

func (s *SamplingBlockIndexFilter) handleChunks(fileEntry *common.FileEntry) {

	for i := range fileEntry.Chunks {
		s.Statistics.ChunkCount++
		copy(s.chunkHashBuf[:], fileEntry.Chunks[i].Digest)

		// add chunk to current block mapping and put it into BlockIndex if full
		if (s.currentBlockMappingOffset + fileEntry.Chunks[i].Size) >= uint32(s.config.BlockSize) {
			// finish it and add to block index after check
			s.currentBlockMapping.Add(&s.chunkHashBuf)
			s.blockIndex.Add(s.currentBlockId, s.currentBlockMapping)
			s.currentBlockMapping = NewBlockMapping(s.config.BlockSize / s.avgChunkSize)
			s.currentBlockMappingOffset = fileEntry.Chunks[i].Size
			s.currentBlockId++
		} else {
			s.currentBlockMappingOffset += fileEntry.Chunks[i].Size
		}

		// check LRU cache
		if s.chunkCache.Contains(s.chunkHashBuf) {
			s.Statistics.RedundantChunkCount++
			s.Statistics.DetectedDuplicatesCount++
		} else if s.currentBlockMapping.Contains(&s.chunkHashBuf) { // check current block mapping
			s.Statistics.WriteCacheHits++
			s.Statistics.RedundantChunkCount++
			s.Statistics.DetectedDuplicatesCount++
		} else if s.blockChunkCache.contains(&s.chunkHashBuf, s.currentBlockId, &s.Statistics.BlockIndexStats, s.streamId) { // check BLC
			s.Statistics.RedundantChunkCount++
			s.Statistics.DetectedDuplicatesCount++
		} else {

			if s.isHook(&s.chunkHashBuf) {

				s.chunkIndexRWLock.RLock()
				blockHint, ok := s.chunkIndex[s.chunkHashBuf]
				s.chunkIndexRWLock.RUnlock()

				if ok { // chunk is duplicate
					s.blockChunkCache.update(&s.chunkHashBuf, s.currentBlockId, blockHint)
					s.Statistics.RedundantChunkCount++
					s.Statistics.DetectedDuplicatesCount++

				} else { // chunk is new
					s.Statistics.UniqueChunkCount++
					s.Statistics.DetectedUniqueChunks++
				}
				s.chunkIndexStatistics.LookupCount++
				s.chunkIndexStatistics.UpdateCount++

			} else {
				// the chunk is no hook. That is, any sampling version of the index would certainly miss (so treat it as a miss).
				// check if the chunk _really_ is new and add it to the chunkIndex if not yet appeared
				s.chunkIndexRWLock.RLock()
				_, ok := s.chunkIndex[s.chunkHashBuf]
				s.chunkIndexRWLock.RUnlock()
				if ok { // chunk is duplicate
					// actually the chunk isn't new -> count as duplicate
					s.Statistics.UndetectedDuplicatesCount++
					s.Statistics.RedundantChunkCount++
				} else {
					s.Statistics.UniqueChunkCount++

					s.chunkIndexRWLock.Lock()
					s.chunkIndex[s.chunkHashBuf] = s.currentBlockId
					s.chunkIndexRWLock.Unlock()
				}
			}
		}

		if s.isHook(&s.chunkHashBuf) { // always update the sampled index if chunk is a hook
			s.chunkIndexRWLock.Lock()
			s.chunkIndex[s.chunkHashBuf] = s.currentBlockId
			s.chunkIndexRWLock.Unlock()
		}

		s.chunkCache.Update(s.chunkHashBuf)
		s.currentBlockMapping.Add(&s.chunkHashBuf)
	}
}
