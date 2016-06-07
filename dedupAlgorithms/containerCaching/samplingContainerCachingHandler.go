package containerCaching

import "os"
import "bytes"
import "time"
import "sync"
import "math/rand"
import "encoding/json"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type SamplingContainerCachingConfig struct {
	ContainerSize      int
	CacheSize          int
	ChunkCacheSize     int
	ContainerPageSize  int
	ChunkIndexPageSize int
	SampleFactor       int
	DataSet            string
}

type SamplingContainerCachingStatistics struct {
	FileNumber int

	ChunkCount                int
	UniqueChunkCount          int
	RedundantChunkCount       int
	DetectedDuplicatesCount   int
	UndetectedDuplicatesCount int
	DetectedUniqueChunks      int // the number of detected unique chunks (because they are hooks)

	UniqueChunkRatio                         float32
	RedundantChunkRatio                      float32
	UndetectedDuplicatesRatio                float32 // the number of stored duplicates compared to the total number of chunks
	UndetectedDuplicatesToAllDuplicatesRatio float32 // the number of undetected duplicates compare to the number of duplicates

	CurrentContainerHits int

	ChunkCacheStats       common.ChunkCacheStatistics
	ChunkIndexStats       common.ChunkIndexStatistics
	ContainerCacheStats   ContainerCacheStatistics
	ContainerStorageStats ContainerStorageStatistics

	StorageCount     int64
	StorageByteCount int64
}

func (cs *SamplingContainerCachingStatistics) add(that SamplingContainerCachingStatistics) {
	cs.ChunkCount += that.ChunkCount
	cs.UniqueChunkCount += that.UniqueChunkCount
	cs.RedundantChunkCount += that.RedundantChunkCount
	cs.CurrentContainerHits += that.CurrentContainerHits

	cs.ChunkCacheStats.Add(that.ChunkCacheStats)
	cs.ChunkIndexStats.Add(that.ChunkIndexStats)
	cs.ContainerCacheStats.add(that.ContainerCacheStats)
	cs.ContainerStorageStats.add(that.ContainerStorageStats)

	cs.StorageCount += that.StorageCount
	cs.StorageByteCount += that.StorageByteCount
}

// computes final statistics
func (cs *SamplingContainerCachingStatistics) finish() {

	cs.StorageCount = cs.ContainerStorageStats.StorageCount
	cs.StorageByteCount = cs.ContainerStorageStats.StorageByteCount

	if cs.ChunkCount > 0 {
		cs.UniqueChunkRatio = float32(float64(cs.UniqueChunkCount) / float64(cs.ChunkCount))
		cs.RedundantChunkRatio = float32(float64(cs.RedundantChunkCount) / float64(cs.ChunkCount))
		cs.UndetectedDuplicatesRatio = float32(float64(cs.UndetectedDuplicatesCount) / float64(cs.ChunkCount))
	} else {
		cs.UniqueChunkRatio = 0.0
		cs.RedundantChunkRatio = 0.0
		cs.UndetectedDuplicatesRatio = 0.0
	}

	if cs.RedundantChunkCount > 0 {
		cs.UndetectedDuplicatesToAllDuplicatesRatio = float32(float64(cs.UndetectedDuplicatesCount) / float64(cs.RedundantChunkCount))
	} else {
		cs.UndetectedDuplicatesToAllDuplicatesRatio = 0.0
	}
}

type SamplingContainerCaching struct {
	avgChunkSize int
	config       SamplingContainerCachingConfig

	chunkCache       *common.ChunkCache
	containerCache   *ContainerCache
	currentContCache *CurrentContainerCache
	containerStorage *ContainerStorage
	chunkIndex       map[[12]byte]uint32
	chunkIndexRWLock *sync.RWMutex
	randGenerator    *rand.Rand

	currentContainerId     uint32
	currentContainerChunks [][12]byte
	freeSpace              uint32
	sliceAllocSize         int // small opt. to not allocate too much memory

	Statistics           SamplingContainerCachingStatistics
	chunkIndexStatistics common.ChunkIndexStatistics
	chunkHashBuf         [12]byte // buffer for all chunk hashes. Used to make the Digest useable in maps.

	traceFileCount int
	StatsList      []SamplingContainerCachingStatistics
	outputFilename string
	traceChan      chan<- string
	streamId       string
}

func NewSamplingContainerCaching(config SamplingContainerCachingConfig,
	avgChunkSize int,
	containerStorage *ContainerStorage,
	containerCache *ContainerCache,
	currContCache *CurrentContainerCache,
	chunkIndex map[[12]byte]uint32,
	chunkIndexRWLock *sync.RWMutex,
	outFilename string,
	traceChan chan<- string) *SamplingContainerCaching {

	cc := new(SamplingContainerCaching)
	cc.avgChunkSize = avgChunkSize
	cc.config = config
	cc.chunkCache = common.NewChunkCache(cc.config.ChunkCacheSize)
	cc.containerStorage = containerStorage
	cc.containerCache = containerCache
	cc.chunkIndex = chunkIndex
	cc.chunkIndexRWLock = chunkIndexRWLock
	cc.randGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))
	cc.outputFilename = outFilename
	cc.traceChan = traceChan

	cc.currentContainerId = cc.containerStorage.GetContainerId()
	if currContCache == nil {
		cc.currentContCache = NewCurrentContainerCache()
	} else {
		cc.currentContCache = currContCache
	}
	cc.currentContCache.add(cc.currentContainerId)

	return cc
}

func (cc *SamplingContainerCaching) BeginTraceFile(number int, name string) {
	log.Info("begin Tracefile ", number, ": ", name)
	cc.streamId = name
	cc.traceFileCount++
	cc.Statistics.FileNumber = cc.traceFileCount
}

func (cc *SamplingContainerCaching) EndTraceFile() {
	log.Info("end Tracefile")

	// collect statistics
	cc.Statistics.ChunkCacheStats = cc.chunkCache.Statistics
	cc.chunkIndexStatistics.ItemCount = int64(len(cc.chunkIndex))
	cc.chunkIndexStatistics.Finish()
	cc.Statistics.ChunkIndexStats = cc.chunkIndexStatistics

	cc.Statistics.ChunkCacheStats.Finish()
	cc.Statistics.ContainerCacheStats.finish()
	cc.Statistics.ContainerStorageStats.finish()

	cc.Statistics.finish()

	cc.chunkCache.Statistics.Reset()
	cc.chunkIndexStatistics.Reset()

	if encodedStats, err := json.MarshalIndent(cc.Statistics, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
	}

	cc.StatsList = append(cc.StatsList, cc.Statistics)
	cc.Statistics = SamplingContainerCachingStatistics{}
}

func (cc *SamplingContainerCaching) Quit() {
	if cc.outputFilename == "" {
		return
	}

	if encodedStats, err := json.MarshalIndent(cc.StatsList, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(cc.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}

func (cc *SamplingContainerCaching) isHook(fp *[12]byte) bool {
	for i := 0; i < cc.config.SampleFactor/8; i++ {
		if fp[i] != byte(0) {
			return false
		}
	}

	shifts := uint32(cc.config.SampleFactor % 8)
	mask := byte(^(0xFF << shifts))
	return (fp[cc.config.SampleFactor/8] & mask) == 0
}

func (cc *SamplingContainerCaching) HandleFiles(fileEntries <-chan *common.FileEntry, fileEntryReturn chan<- *common.FileEntry) {
	for fileEntry := range fileEntries {
		cc.handleChunks(fileEntry)
		fileEntryReturn <- fileEntry
	}
}

func (cc *SamplingContainerCaching) handleChunks(fileEntry *common.FileEntry) {

	for i := range fileEntry.Chunks {
		cc.Statistics.ChunkCount++
		copy(cc.chunkHashBuf[:], fileEntry.Chunks[i].Digest)

		// run algorithm

		cc.chunkIndexRWLock.RLock()
		containerId, ok := cc.chunkIndex[cc.chunkHashBuf]
		cc.chunkIndexRWLock.RUnlock()

		if containerId == cc.currentContainerId { // check current container
			cc.Statistics.RedundantChunkCount++
			cc.Statistics.CurrentContainerHits++
		} else if cc.chunkCache.Contains(cc.chunkHashBuf) { // check LRU cache
			cc.Statistics.RedundantChunkCount++
		} else if cc.currentContCache.contains(containerId) { // is in the set of currently open containers
			cc.Statistics.RedundantChunkCount++
		} else if cc.containerCache.contains(&cc.chunkHashBuf, &cc.Statistics) {
			cc.Statistics.RedundantChunkCount++
		} else { // cache miss
			// we count IO for chunk index for existing chunks and for false positives of the Bloom filter
			cc.chunkIndexStatistics.LookupCount++

			if cc.isHook(&cc.chunkHashBuf) {

				if ok { // duplicate
					cc.containerCache.update(containerId, cc.streamId, &cc.Statistics)
					cc.Statistics.ContainerCacheStats.FetchCount++
					cc.Statistics.RedundantChunkCount++
					cc.Statistics.DetectedDuplicatesCount++
				} else { // chunk is new
					cc.AddToCurrentContainer(&cc.chunkHashBuf, fileEntry.Chunks[i].Size)
					/*cc.containerCache.update(cc.currentContainerId, &cc.Statistics)*/
					/*cc.Statistics.ContainerCacheStats.FetchCount++*/
					cc.chunkIndexRWLock.Lock()
					cc.chunkIndex[cc.chunkHashBuf] = cc.currentContainerId
					cc.chunkIndexRWLock.Unlock()

					cc.Statistics.UniqueChunkCount++
					cc.Statistics.DetectedUniqueChunks++
					cc.chunkIndexStatistics.UpdateCount++
				}
			} else {
				// The chunk is no hook. That is, any sampling version of the index would certainly miss (so treat it as a miss).
				// check if the chunk _really_ is new and add it to the chunkIndex if not yet appeared

				cc.AddToCurrentContainer(&cc.chunkHashBuf, fileEntry.Chunks[i].Size)
				/*cc.containerCache.update(cc.currentContainerId, &cc.Statistics)*/
				/*cc.Statistics.ContainerCacheStats.FetchCount++*/

				if ok { // duplicate
					// actually the chunk isn't new -> count as duplicate
					cc.Statistics.UndetectedDuplicatesCount++
					cc.Statistics.RedundantChunkCount++
				} else {
					cc.Statistics.UniqueChunkCount++
					cc.chunkIndexRWLock.Lock()
					cc.chunkIndex[cc.chunkHashBuf] = cc.currentContainerId
					cc.chunkIndexRWLock.Unlock()
				}
			}
		}

		cc.chunkCache.Update(cc.chunkHashBuf)
	}
}

func (cc *SamplingContainerCaching) AddToCurrentContainer(fp *[12]byte, chunkSize uint32) {
	if cc.freeSpace < chunkSize {
		newId := cc.containerStorage.AddNewContainer(cc.currentContainerChunks, cc.currentContainerId)
		cc.currentContCache.update(cc.currentContainerId, newId)
		cc.currentContainerId = newId
		cc.Statistics.ContainerStorageStats.ContainerCount++
		if cc.sliceAllocSize < len(cc.currentContainerChunks) {
			cc.sliceAllocSize = len(cc.currentContainerChunks)
		}
		cc.currentContainerChunks = make([][12]byte, 0, cc.sliceAllocSize)
		cc.freeSpace = uint32(cc.config.ContainerSize)
	}
	cc.currentContainerChunks = append(cc.currentContainerChunks, *fp)
	cc.freeSpace -= chunkSize
}
