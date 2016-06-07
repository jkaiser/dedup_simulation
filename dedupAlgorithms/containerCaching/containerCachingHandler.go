package containerCaching

import "os"
import "bytes"
import "time"
import "sync"
import "math/rand"
import "encoding/json"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type ContainerCachingConfig struct {
	ContainerSize             int
	CacheSize                 int
	ChunkCacheSize            int
	ContainerPageSize         int
	ChunkIndexPageSize        int
	NegativeLookupProbability float32
	DataSet                   string
}

type ContainerCachingStatistics struct {
	FileNumber int

	ChunkCount          int
	UniqueChunkCount    int
	RedundantChunkCount int

	UniqueChunkRatio    float32
	RedundantChunkRatio float32

	CurrentContainerHits int

	ChunkCacheStats       common.ChunkCacheStatistics
	ChunkIndexStats       common.ChunkIndexStatistics
	ContainerCacheStats   ContainerCacheStatistics
	ContainerStorageStats ContainerStorageStatistics

	ChainRuns                 int
	FalsePositiveStorageCount int64
	StorageCount              int64
	StorageByteCount          int64
}

func (cs *ContainerCachingStatistics) add(that ContainerCachingStatistics) {
	cs.ChunkCount += that.ChunkCount
	cs.UniqueChunkCount += that.UniqueChunkCount
	cs.RedundantChunkCount += that.RedundantChunkCount
	cs.CurrentContainerHits += that.CurrentContainerHits

	cs.ChunkCacheStats.Add(that.ChunkCacheStats)
	cs.ChunkIndexStats.Add(that.ChunkIndexStats)
	cs.ContainerCacheStats.add(that.ContainerCacheStats)
	cs.ContainerStorageStats.add(that.ContainerStorageStats)

	cs.ChainRuns += that.ChainRuns
	cs.FalsePositiveStorageCount += that.FalsePositiveStorageCount
	cs.StorageCount += that.StorageCount
	cs.StorageByteCount += that.StorageByteCount
}

// computes final statistics
func (cs *ContainerCachingStatistics) finish() {

	cs.StorageCount = cs.ContainerStorageStats.StorageCount + cs.ChunkIndexStats.StorageCount
	cs.StorageByteCount = cs.ContainerStorageStats.StorageByteCount + cs.ChunkIndexStats.StorageByteCount

	if cs.ChunkCount > 0 {
		cs.UniqueChunkRatio = float32(float64(cs.UniqueChunkCount) / float64(cs.ChunkCount))
		cs.RedundantChunkRatio = float32(float64(cs.RedundantChunkCount) / float64(cs.ChunkCount))
	} else {
		cs.UniqueChunkRatio = 0.0
		cs.RedundantChunkRatio = 0.0
	}
}

type ContainerCaching struct {
	avgChunkSize int
	config       ContainerCachingConfig

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

	Statistics           ContainerCachingStatistics
	chunkIndexStatistics common.ChunkIndexStatistics
	chunkHashBuf         [12]byte // buffer for all chunk hashes. Used to make the Digest useable in maps.

	traceFileCount int
	StatsList      []ContainerCachingStatistics
	outputFilename string
	traceChan      chan<- string

	streamId string
}

func NewContainerCaching(config ContainerCachingConfig,
	avgChunkSize int,
	containerStorage *ContainerStorage,
	containerCache *ContainerCache,
	currContCache *CurrentContainerCache,
	chunkIndex map[[12]byte]uint32,
	chunkIndexRWLock *sync.RWMutex,
	outFilename string,
	traceChan chan<- string) *ContainerCaching {

	cc := new(ContainerCaching)
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
	cc.currentContainerChunks = make([][12]byte, 0, config.ContainerSize/avgChunkSize)
	cc.sliceAllocSize = config.ContainerSize / avgChunkSize
	return cc
}

func (cc *ContainerCaching) BeginTraceFile(number int, name string) {
	log.Info("begin Tracefile ", number, ": ", name)
	cc.streamId = name
	cc.traceFileCount++
	cc.Statistics.FileNumber = cc.traceFileCount
}

func (cc *ContainerCaching) EndTraceFile() {
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
	cc.Statistics = ContainerCachingStatistics{}
}

func (cc *ContainerCaching) Quit() {
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

func (cc *ContainerCaching) HandleFiles(fileEntries <-chan *common.FileEntry, fileEntryReturn chan<- *common.FileEntry) {
	for fileEntry := range fileEntries {
		cc.handleChunks(fileEntry)
		fileEntryReturn <- fileEntry
	}
}

func (cc *ContainerCaching) handleChunks(fileEntry *common.FileEntry) {
	var diceThrow float32

	for i := range fileEntry.Chunks {
		cc.Statistics.ChunkCount++
		copy(cc.chunkHashBuf[:], fileEntry.Chunks[i].Digest)

		// run algorithm
		cc.chunkIndexRWLock.RLock()
		containerId, ok := cc.chunkIndex[cc.chunkHashBuf]
		cc.chunkIndexRWLock.RUnlock()
		if !ok {
			diceThrow = cc.randGenerator.Float32()
		}

		if ok || (!ok && (diceThrow < cc.config.NegativeLookupProbability)) {
			// The simulated Bloom filter gave a positive. Note that this also might be a false positive!
			cc.Statistics.ChainRuns++

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
				cc.chunkIndexStatistics.StorageCount++
				cc.chunkIndexStatistics.StorageByteCount += int64(cc.config.ChunkIndexPageSize)

				if ok { // duplicate
					cc.containerCache.update(containerId, cc.streamId, &cc.Statistics)
					cc.Statistics.ContainerCacheStats.FetchCount++
					cc.Statistics.RedundantChunkCount++

				} else { // unique chunks -> false positive
					cc.AddToCurrentContainer(&cc.chunkHashBuf, fileEntry.Chunks[i].Size)
					/*cc.containerCache.update(cc.currentContainerId, &cc.Statistics)*/
					/*cc.Statistics.ContainerCacheStats.FetchCount++*/
					cc.chunkIndexRWLock.Lock()
					cc.chunkIndex[cc.chunkHashBuf] = cc.currentContainerId
					cc.chunkIndexRWLock.Unlock()

					cc.Statistics.UniqueChunkCount++
					cc.chunkIndexStatistics.UpdateCount++
					cc.chunkIndexStatistics.StorageCountUnique++
					cc.Statistics.FalsePositiveStorageCount++
				}
			}

		} else {
			// The simulated Bloom filter gave a negative
			cc.Statistics.UniqueChunkCount++
			cc.AddToCurrentContainer(&cc.chunkHashBuf, fileEntry.Chunks[i].Size)
			/*cc.containerCache.update(cc.currentContainerId, &cc.Statistics)*/
			/*cc.Statistics.ContainerCacheStats.FetchCount++*/

			cc.chunkIndexRWLock.Lock()
			cc.chunkIndex[cc.chunkHashBuf] = cc.currentContainerId
			cc.chunkIndexRWLock.Unlock()
			cc.chunkIndexStatistics.UpdateCount++
		}

		/*cc.chunkCacheMutex.Lock()*/
		cc.chunkCache.Update(cc.chunkHashBuf)
		/*cc.chunkCacheMutex.Unlock()*/
	}
}

func (cc *ContainerCaching) AddToCurrentContainer(fp *[12]byte, chunkSize uint32) {
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

// Cache used for holding the ids of all currently open containers. For
// a single stream this is overkill, but in the many streams case there
// is at least one open container per stream.
type CurrentContainerCache struct {
	ids []uint32
	mut sync.RWMutex
}

func NewCurrentContainerCache() *CurrentContainerCache {
	return &CurrentContainerCache{ids: make([]uint32, 0, 8)}
}
func (ccc *CurrentContainerCache) contains(cid uint32) bool {
	ccc.mut.RLock()
	defer ccc.mut.RUnlock()

	for _, id := range ccc.ids {
		if cid == id {
			return true
		}
	}
	return false
}

func (ccc *CurrentContainerCache) update(cid uint32, with uint32) {

	var found bool
	ccc.mut.Lock()
	defer ccc.mut.Unlock()
	for i, id := range ccc.ids {
		if id == cid {
			ccc.ids[i] = with
			found = true
		}
	}

	if !found {
		ccc.ids = append(ccc.ids, with)
	}
}

func (ccc *CurrentContainerCache) add(cid uint32) {

	var found bool
	ccc.mut.Lock()
	defer ccc.mut.Unlock()
	for _, id := range ccc.ids {
		if id == cid {
			found = true
		}
	}

	if !found {
		ccc.ids = append(ccc.ids, cid)
	}
}
