package blc

import "container/list"
import "sort"

import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type BlockLocalityCacheStatistics struct {
	Hits                  int
	HitsWithoutFetch      int
	HitsWithFetch         int
	HitsAfterFetch        int
	Misses                int
	FalseBlockIndexMisses int

	ChunksInBlockChunkMapCount    int
	BlocksPerChunkInChunksMapMean float32
	BlocksPerChunkInChunksMapMax  int
	BlocksPerChunkInChunksMapMin  int
	BlocksPerChunkInChunksMap25P  int
	BlocksPerChunkInChunksMap50P  int
	BlocksPerChunkInChunksMap75P  int
	BlocksPerChunkInChunksMap90P  int

	HitRatio             float32
	HitRatioWithoutFetch float32
	HitRatioWithFetch    float32

	SkippedFetchCount int

	GenerationFetches map[string]int
}

func NewBlockLocalityCacheStatistics() BlockLocalityCacheStatistics {
	var b BlockLocalityCacheStatistics
	b.GenerationFetches = make(map[string]int)
	return b
}

func (bs *BlockLocalityCacheStatistics) add(that BlockLocalityCacheStatistics) {
	bs.Hits += that.Hits
	bs.HitsWithoutFetch += that.HitsWithoutFetch
	bs.HitsWithFetch += that.HitsWithFetch
	bs.HitsAfterFetch += that.HitsAfterFetch
	bs.Misses += that.Misses
	bs.FalseBlockIndexMisses += that.FalseBlockIndexMisses

	bs.SkippedFetchCount += that.SkippedFetchCount

	for gen, num := range that.GenerationFetches {
		bs.GenerationFetches[gen] = bs.GenerationFetches[gen] + num
	}
}
func (bs *BlockLocalityCacheStatistics) finish() {
	accesses := bs.Hits + bs.Misses
	if accesses > 0 {
		bs.HitRatio = float32(float64(bs.Hits) / float64(accesses))
		bs.HitRatioWithoutFetch = float32(float64(bs.HitsWithoutFetch) / float64(accesses))
		bs.HitRatioWithFetch = float32(float64(bs.HitsWithFetch) / float64(accesses))
	} else {
		bs.HitRatio = 0.0
		bs.HitRatioWithoutFetch = 0.0
		bs.HitRatioWithFetch = 0.0
	}
}

func (bs *BlockLocalityCacheStatistics) reset() {
	bs.Hits = 0
	bs.HitsWithoutFetch = 0
	bs.HitsWithFetch = 0
	bs.HitsAfterFetch = 0
	bs.Misses = 0
	bs.FalseBlockIndexMisses = 0

	bs.ChunksInBlockChunkMapCount = 0
	bs.BlocksPerChunkInChunksMapMean = 0
	bs.BlocksPerChunkInChunksMapMax = 0
	bs.BlocksPerChunkInChunksMapMin = 0
	bs.BlocksPerChunkInChunksMap25P = 0
	bs.BlocksPerChunkInChunksMap50P = 0
	bs.BlocksPerChunkInChunksMap75P = 0
	bs.BlocksPerChunkInChunksMap90P = 0

	bs.HitRatio = 0
	bs.HitRatioWithoutFetch = 0
	bs.HitRatioWithFetch = 0

	bs.SkippedFetchCount = 0

	bs.GenerationFetches = make(map[string]int)
}

type BlockLocalityCache struct {
	minDiffValue      int
	blockCacheSize    int
	diffCacheSize     int
	blockMappingCache *common.UInt32Cache // in a real system this would contain blockMappings. Here, we replace them by their ID since it is sufficient for the in-memory simulation.
	differenceCache   *common.UInt32Cache
	blockChunkMap     map[[12]byte]map[uint32]bool

	firstIdOfGenIndex []uint32
	/*genLookupCounter  []int*/

	existenceMapBuffer []map[uint32]bool // buffer to recycle empty entries of blockChunkMap
	diffs              []int             // used in fetchBlockMappings
	blockMappings      *list.List
	firstBlockIdInRun  uint32
	blockIndex         *BlockIndex
	Statistics         BlockLocalityCacheStatistics
}

func NewBlockLocalityCache(minDiffValue int,
	blockCacheSize int,
	diffCacheSize int,
	bIndex *BlockIndex) *BlockLocalityCache {

	blc := &BlockLocalityCache{
		minDiffValue:      minDiffValue,
		blockCacheSize:    blockCacheSize,
		diffCacheSize:     diffCacheSize,
		differenceCache:   common.NewUInt32Cache(diffCacheSize, nil),
		blockChunkMap:     make(map[[12]byte]map[uint32]bool),
		firstIdOfGenIndex: make([]uint32, 0, 12),
		/*genLookupCounter:   make([]int, 0, 12),*/
		existenceMapBuffer: make([]map[uint32]bool, 0),
		diffs:              make([]int, 0, diffCacheSize),
		blockMappings:      list.New(),
		firstBlockIdInRun:  0,
		blockIndex:         bIndex,
		Statistics:         NewBlockLocalityCacheStatistics(),
	}

	blc.blockMappingCache = common.NewUInt32Cache(blockCacheSize, blc.blockCacheEvict)
	return blc
}

func (b *BlockLocalityCache) beginNewGeneration(startBlockId uint32) {
	b.firstIdOfGenIndex = append(b.firstIdOfGenIndex, startBlockId)
	/*b.genLookupCounter = append(b.genLookupCounter, 0)*/
}

func (b *BlockLocalityCache) finish() BlockLocalityCacheStatistics {

	b.Statistics.ChunksInBlockChunkMapCount = len(b.blockChunkMap)
	numBlocks := make([]int, 0, 64)
	blockSum := 0
	for _, blockMap := range b.blockChunkMap {
		numBlocks = append(numBlocks, len(blockMap))
		blockSum += len(blockMap)
	}
	sort.Ints(numBlocks)
	if len(numBlocks) > 0 {
		b.Statistics.BlocksPerChunkInChunksMapMean = float32(blockSum) / float32(len(numBlocks))
		b.Statistics.BlocksPerChunkInChunksMapMax = numBlocks[len(numBlocks)-1]
		b.Statistics.BlocksPerChunkInChunksMapMin = numBlocks[0]
		b.Statistics.BlocksPerChunkInChunksMap25P = numBlocks[len(numBlocks)/4]
		b.Statistics.BlocksPerChunkInChunksMap50P = numBlocks[len(numBlocks)/2]
		b.Statistics.BlocksPerChunkInChunksMap75P = numBlocks[int(float32(len(numBlocks))*0.75)]
		b.Statistics.BlocksPerChunkInChunksMap90P = numBlocks[int(float32(len(numBlocks))*0.9)]
	} else {
		b.Statistics.BlocksPerChunkInChunksMapMean = 0
		b.Statistics.BlocksPerChunkInChunksMapMax = 0
		b.Statistics.BlocksPerChunkInChunksMapMin = 0
		b.Statistics.BlocksPerChunkInChunksMap25P = 0
		b.Statistics.BlocksPerChunkInChunksMap50P = 0
		b.Statistics.BlocksPerChunkInChunksMap75P = 0
		b.Statistics.BlocksPerChunkInChunksMap90P = 0
	}

	/*for i, val := range b.genLookupCounter {*/
	/*b.Statistics.GenerationFetches[strconv.Itoa(i)] = val*/
	/*b.genLookupCounter[i] = 0*/
	/*}*/

	b.firstBlockIdInRun = 0
	b.Statistics.finish()
	res := b.Statistics

	// clear blc
	b.blockChunkMap = make(map[[12]byte]map[uint32]bool, len(b.blockChunkMap))
	b.differenceCache = common.NewUInt32Cache(b.diffCacheSize, nil)
	b.blockMappingCache = common.NewUInt32Cache(b.blockCacheSize, b.blockCacheEvict)

	b.Statistics = NewBlockLocalityCacheStatistics()
	return res
}

func (b *BlockLocalityCache) update(fp *[12]byte, currentBlockId uint32, lastBlockId uint32) {
	diff := currentBlockId - lastBlockId
	if val, ok := b.differenceCache.Contains(diff); ok {
		b.differenceCache.Update(diff, val+1)
	} else {
		b.differenceCache.Update(diff, 1)
	}
}

func (b *BlockLocalityCache) blockCacheEvict(blockId uint32) {

	//b.blockIndex.Index[blockId].Foreach(func(fp *[12]byte) {
	var helper BlockMapper
	var isok bool
	b.blockIndex.IndexRWLock.RLock()
	if helper, isok = b.blockIndex.localIndex[blockId]; (!isok) && (b.blockIndex.Index != nil) {
		helper, isok = b.blockIndex.GetIndex(blockId)
	}
	b.blockIndex.IndexRWLock.RUnlock()

	if isok {
		helper.Foreach(func(fp *[12]byte) {
			if existenceMap, ok := b.blockChunkMap[*fp]; ok {
				delete(existenceMap, blockId)
				if len(existenceMap) == 0 {
					b.existenceMapBuffer = append(b.existenceMapBuffer, existenceMap)
					delete(b.blockChunkMap, *fp)
				}
			}
		})
	}
}

func (b *BlockLocalityCache) contains(fp *[12]byte, currentBlockId uint32, stats *BlockIndexStatistics, streamId string) bool {
	if b.firstBlockIdInRun == 0 {
		b.firstBlockIdInRun = currentBlockId
	}

	blockSet, ok := b.blockChunkMap[*fp]

	if ok {
		differences := make([]uint32, 0, len(blockSet))
		for blockId, _ := range blockSet {
			differences = append(differences, currentBlockId-blockId)
			b.blockMappingCache.Contains(blockId) // just a touch if it is in the cache, TODO: in Dirk's prog there is an update with an 1
		}

		if len(differences) == 1 {
			val, _ := b.differenceCache.Contains(differences[0])
			b.differenceCache.Update(differences[0], val+1)
		} else {
			for _, diff := range differences {
				if val, ok := b.differenceCache.Contains(diff); ok {
					b.differenceCache.Update(diff, val+1)
				}
			}
		}

		b.Statistics.Hits++
		b.Statistics.HitsWithoutFetch++
		return true

	} else { // not in cache -> fetch
		found := b.fetchBlockMappings(fp, currentBlockId, stats, streamId)
		if found {
			b.Statistics.Hits++
			b.Statistics.HitsWithFetch++
		} else {
			b.Statistics.Misses++
		}
		return found
	}
}

func (b *BlockLocalityCache) fetchBlockMappings(fp *[12]byte, currentBlockId uint32, stats *BlockIndexStatistics, streamId string) bool {

	b.diffs = b.diffs[:0]
	for diff := range b.differenceCache.CacheMap {
		b.diffs = append(b.diffs, int(diff))
	}

	sort.Sort(sort.Reverse(sort.IntSlice(b.diffs)))

	for _, d := range b.diffs {
		diff := uint32(d)
		num_uses, _ := b.differenceCache.Contains(diff)

		checkBlockId := currentBlockId - diff

		var blockIndexContains bool
		b.blockIndex.IndexRWLock.RLock()
		if _, blockIndexContains = b.blockIndex.localIndex[uint32(checkBlockId)]; (!blockIndexContains) && (b.blockIndex.Index != nil) {
			_, blockIndexContains = b.blockIndex.GetIndex(uint32(checkBlockId))
		}
		b.blockIndex.IndexRWLock.RUnlock()

		_, blockMappingCacheContains := b.blockMappingCache.Contains(checkBlockId)
		if blockIndexContains && !blockMappingCacheContains {
			// fetch
			if _, blockChunkMapContains := b.blockChunkMap[*fp]; blockChunkMapContains {
				b.differenceCache.Update(diff, num_uses+1)
			}

			if num_uses+1 > uint32(b.minDiffValue) { // only fetch the block if seen enough
				b.fetchSingleBlock(checkBlockId, stats, streamId)
			}

			if _, ok := b.blockChunkMap[*fp]; ok {
				b.Statistics.HitsAfterFetch++
				return true
			}

		} else {
			// simulation artifact?
			if !blockIndexContains && (checkBlockId < currentBlockId) {
				b.Statistics.FalseBlockIndexMisses++
			}
		}
	}

	return false
}

// Helperfunction that maintains the histogram about the fetches from different generations
/*func (b *BlockLocalityCache) logUsedBlock(blockId uint32) {*/
/*if len(b.genLookupCounter) == 1 {*/
/*[>b.genLookupCounter[0]++<]*/
/*} else {*/
/*var found bool = false*/
/*for i, val := range b.firstIdOfGenIndex {*/
/*if blockId < val { // found border => block comes from generation i-1*/
/*[>b.genLookupCounter[i-1]++<]*/
/*found = true*/
/*break*/
/*}*/
/*}*/

/*if !found {*/
/*b.genLookupCounter[len(b.genLookupCounter)-1]++*/
/*}*/
/*}*/
/*}*/

// In fs-ca, this function is simply called "fetchBlock".
func (b *BlockLocalityCache) fetchSingleBlock(blockId uint32, stats *BlockIndexStatistics, streamId string) {
	if _, ok := b.blockMappingCache.Contains(blockId); ok { // already in the cache?
		b.Statistics.SkippedFetchCount++
		return
	}

	/*b.logUsedBlock(blockId)*/
	b.blockIndex.fetchFullPage(blockId, true, b.blockMappings, stats, streamId)

	// for each blockMapping in the page
	for e := b.blockMappings.Front(); e != nil; e = e.Next() {
		mapPair := e.Value.(*BlockIdBlockMappingPair)

		// add blockID to blockChunkMap for each chunk
		mapPair.mapping.Foreach(func(fp *[12]byte) {
			chunkMap, ok := b.blockChunkMap[*fp]
			if !ok && (len(b.existenceMapBuffer) > 0) {
				recMap := b.existenceMapBuffer[len(b.existenceMapBuffer)-1]
				b.existenceMapBuffer = b.existenceMapBuffer[:len(b.existenceMapBuffer)-1]
				recMap[mapPair.blockId] = true
				b.blockChunkMap[*fp] = recMap
			} else if !ok {
				b.blockChunkMap[*fp] = map[uint32]bool{mapPair.blockId: true}
			} else {
				chunkMap[mapPair.blockId] = true
			}
		})

		// add BlockMapping to cache
		b.blockMappingCache.Update(mapPair.blockId, uint32(1))
	}
}
