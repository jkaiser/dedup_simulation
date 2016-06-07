package containerCaching

import "sync"
import "sort"
import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type Uint32Slice []uint32

func (p Uint32Slice) Len() int           { return len(p) }
func (p Uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type ContainerCacheStatistics struct {
	Hits       int
	Misses     int
	FetchCount int

	HitRatio float32
}

func (cs *ContainerCacheStatistics) add(that ContainerCacheStatistics) {
	cs.Hits += that.Hits
	cs.Misses += that.Misses
	cs.FetchCount += that.FetchCount
}
func (cs *ContainerCacheStatistics) finish() {
	if (cs.Hits + cs.Misses) != 0 {
		cs.HitRatio = float32(cs.Hits) / float32(cs.Hits+cs.Misses)
	} else {
		cs.HitRatio = 0.0
	}
}

func (cs *ContainerCacheStatistics) reset() {
	cs.Hits = 0
	cs.Misses = 0
	cs.FetchCount = 0

	cs.HitRatio = 0.0
}

// A container cache simulation used to simulate the approach of DDFS
type ContainerCache struct {
	containerStorage *ContainerStorage

	containerSize int
	avgChunkSize  int
	pageSize      int

	containerCache      *common.UInt32Cache
	containerCacheMutex sync.Mutex
	chunkMap            map[[12]byte]uint32
	chunkMapRWMutex     sync.RWMutex
}

// creates a new ContainerCache. The cacheSize denotes the number of containers the cache contains
func NewContainerCache(cacheSize int, containerSize int, PageSize int, averageChunkSize int, cs *ContainerStorage) *ContainerCache {

	cc := &ContainerCache{
		containerStorage: cs,
		chunkMap:         make(map[[12]byte]uint32, cs.containerSize/cs.avgChunkSize*cacheSize),
		containerSize:    containerSize,
		avgChunkSize:     averageChunkSize,
		pageSize:         PageSize,
	}

	cc.containerCache = common.NewUInt32Cache(cacheSize, cc.containerEvict)
	return cc
}

/*func (cc *ContainerCache) contains(fp *[12]byte, stats *ContainerCachingStatistics) bool {*/
func (cc *ContainerCache) contains(fp *[12]byte, stats interface{}) bool {
	cc.chunkMapRWMutex.RLock()
	containerId, ok := cc.chunkMap[*fp]
	cc.chunkMapRWMutex.RUnlock()

	if ok {
		cc.containerCacheMutex.Lock()
		cc.containerCache.Contains(containerId) // just a touch
		cc.containerCacheMutex.Unlock()
		switch t := stats.(type) {
		case *ContainerCachingStatistics:
			t.ContainerCacheStats.Hits++
		case *SamplingContainerCachingStatistics:
			t.ContainerCacheStats.Hits++
		default:
			panic("wrong type")
		}
	} else {
		switch t := stats.(type) {
		case *ContainerCachingStatistics:
			t.ContainerCacheStats.Misses++
		case *SamplingContainerCachingStatistics:
			t.ContainerCacheStats.Misses++
		default:
			panic("wrong type")
		}
	}

	return ok
}

func (cc *ContainerCache) update(containerId uint32, streamId string, stats interface{}) {

	var cIds Uint32Slice
	if cc.pageSize == 0 {
		cIds = Uint32Slice{containerId}
	} else {
		// get all container ids in the same page
		indexPage := cc.getPageForContainer(containerId)
		cIds = make(Uint32Slice, 0, 32)
		cIds = append(cIds, containerId)
		for i := uint32(1); cc.getPageForContainer(containerId-i) == indexPage; i++ {
			cIds = append(cIds, containerId-i)
		}
		for i := uint32(1); cc.getPageForContainer(containerId+i) == indexPage; i++ {
			cIds = append(cIds, containerId+i)
		}

		sort.Sort(cIds)
	}

	// add the chunks of them to the cache
	for i, container := range cIds {
		if chunks := cc.containerStorage.ReadContainer(container, (i == 0), streamId, stats); chunks != nil { // just count the IO for the first container in a page
			cc.containerCacheMutex.Lock()
			cc.containerCache.Update(container, 0)
			cc.chunkMapRWMutex.Lock()
			for i := range chunks {
				cc.chunkMap[chunks[i]] = container
			}
			cc.chunkMapRWMutex.Unlock()
			cc.containerCacheMutex.Unlock()
		}
	}
}

func (cc *ContainerCache) containerEvict(containerId uint32) {
	if chunks := cc.containerStorage.ReadContainer(containerId, false, "", nil); chunks != nil {
		cc.chunkMapRWMutex.Lock()
		for i := range chunks {
			delete(cc.chunkMap, chunks[i])
		}
		cc.chunkMapRWMutex.Unlock()
	}
}

func (cc *ContainerCache) getPageForContainer(containerId uint32) uint32 {
	averageChunksPerContainer := (cc.containerSize / (cc.avgChunkSize))
	estimatedContainerMappingSize := averageChunksPerContainer * 24
	ContainerMappingsPerPage := cc.pageSize / estimatedContainerMappingSize
	return containerId / uint32(ContainerMappingsPerPage)
}
