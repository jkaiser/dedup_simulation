package blc

import "sync"
import "container/list"
import "github.com/bradfitz/gomemcache/memcache"
import "encoding/gob"
import "strconv"
import "bytes"
import "fmt"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

//import "time"

// The BlockMapper is the interface for all BlockMapping implementations.
type BlockMapper interface {
	SavedChunks() int
	Contains(fp *[12]byte) bool
	Add(fp *[12]byte)
	Size() int
	Foreach(f func(fp *[12]byte))
}

// Implementation for BlockMappings. This one uses a map as set structure. Useful for BlockMappings that are bigger than 32 chunks, on average
type BlockMappingSetMap struct {
	ActualSize int
	Set        map[[12]byte]bool
}

// Implementation for BlockMappings. This one uses a slice as set structure. Useful for BlockMappings that are smaller than or equal to 32 chunks, on average
type BlockMappingSetArray struct {
	ActualSize int
	Set        [][12]byte
}

func NewBlockMapping(avgNumChunks int) BlockMapper {

	if avgNumChunks <= 32 {
		return &BlockMappingSetArray{Set: make([][12]byte, 0, avgNumChunks)}
	} else {
		return &BlockMappingSetMap{Set: make(map[[12]byte]bool, avgNumChunks)}
	}
}

func (bm *BlockMappingSetMap) SavedChunks() int { return bm.ActualSize - bm.Size() }
func (bm *BlockMappingSetMap) Size() int        { return len(bm.Set) }
func (bm *BlockMappingSetMap) Add(fp *[12]byte) {
	bm.ActualSize++
	bm.Set[*fp] = true
}
func (bm *BlockMappingSetMap) Contains(fp *[12]byte) bool {
	return bm.Set[*fp] // default zero value of bool is false
}
func (bm *BlockMappingSetMap) Foreach(f func(*[12]byte)) {
	for fp := range bm.Set {
		f(&fp)
	}
}

func (bm *BlockMappingSetArray) SavedChunks() int { return bm.ActualSize - bm.Size() }
func (bm *BlockMappingSetArray) Size() int        { return len(bm.Set) }
func (bm *BlockMappingSetArray) Add(fp *[12]byte) {
	bm.ActualSize++
	for i := range bm.Set {
		if bm.Set[i] == *fp {
			return
		}
	}
	bm.Set = append(bm.Set, *fp)
}
func (bm *BlockMappingSetArray) Contains(fp *[12]byte) bool {
	for index, _ := range bm.Set {
		if bm.Set[index] == *fp {
			return true
		}
	}
	return false
}
func (bm *BlockMappingSetArray) Foreach(f func(*[12]byte)) {
	for i := range bm.Set {
		f(&bm.Set[i])
	}
}

// small helperstruct to simulate tuples
type BlockIdBlockMappingPair struct {
	blockId uint32
	mapping BlockMapper
}

// BlockIndexStatistics holds all statistics concerning the BlockIndex
type BlockIndexStatistics struct {
	FetchCount       int
	FetchChunkCount  int
	StorageCount     int64
	StorageByteCount int64

	MemcacheSets   int
	MemcacheGets   int
	LocalIndexGets int

	IndexEntryCount int
	AvgFpPerEntry   float32
	AvgSavedChunks  float32
}

func (bs *BlockIndexStatistics) add(that BlockIndexStatistics) {
	bs.FetchCount += that.FetchCount
	bs.FetchChunkCount += that.FetchChunkCount
	bs.StorageCount += that.StorageCount
	bs.StorageByteCount += that.StorageByteCount
}

func (bs *BlockIndexStatistics) finish() {}
func (bs *BlockIndexStatistics) reset() {
	bs.FetchCount = 0
	bs.FetchChunkCount = 0
	bs.StorageCount = 0
	bs.StorageByteCount = 0
	bs.IndexEntryCount = 0
	bs.MemcacheSets = 0
	bs.MemcacheGets = 0
	bs.LocalIndexGets = 0
	bs.AvgFpPerEntry = 0.0
	bs.AvgSavedChunks = 0.0
}

// The BlockIndex is the main data structure for rebuilding blocks. In this
// simulation context, it just holds a mapping from the block id to a set
// of chunks.
type BlockIndex struct {
	blockSize    int
	localIndex   map[uint32]BlockMapper
	Index        *memcache.Client
	IndexRWLock  *sync.RWMutex
	pageCache    *common.UInt32Cache
	avgChunkSize int
	pageSize     int
	traceChan    chan string
}

func NewBlockIndex(blockSize int, biPageSize int, averageChunkSize int, traceChan chan string) *BlockIndex {

	if (blockSize / averageChunkSize) <= 32 {
		gob.Register(&BlockMappingSetArray{})
	} else {
		gob.Register(&BlockMappingSetMap{})
	}

	return &BlockIndex{blockSize: blockSize,
		localIndex: make(map[uint32]BlockMapper, 1E7),
		//Index:        mem_client,
		IndexRWLock:  new(sync.RWMutex),
		pageCache:    common.NewUInt32Cache(16, nil),
		avgChunkSize: averageChunkSize,
		pageSize:     biPageSize,
		traceChan:    traceChan}
}

//set memclient as Index
func (b *BlockIndex) setIndex(client *memcache.Client) {
	b.Index = client
}

//add to b.Index
func (b *BlockIndex) AddIndex(idd uint32, mapping BlockMapper) bool {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if encErr := enc.Encode(&mapping); encErr != nil {
		log.Error("encode error addIndex:", encErr)
	}

	//try to add for max 10 times
	id := strconv.FormatInt(int64(idd), 10)
	done := false
	var memerr error
	for i := 0; i < 10; i++ {
		if memerr = b.Index.Set(&memcache.Item{Key: id, Value: buf.Bytes()}); memerr != nil {
			log.Debugf("write to memcacheserver addIndex: %s. Counter is at %d ", memerr, i)
		} else {
			/*b.Statistics.MemcacheSets++*/
			done = true
			break
		}
	}

	if !done {
		log.Errorf("Write to memcacheserver failed for block %s : %s", idd, memerr)
	}

	return done
}

//get from b.Index
func (b *BlockIndex) GetIndex(idd uint32) (BlockMapper, bool) {
	var blockmap BlockMapper
	var mapping *memcache.Item
	var memerr error
	id := strconv.FormatInt(int64(idd), 10)
	timeout := true
	toCounter := 0

	for timeout {
		/*b.Statistics.MemcacheGets++*/
		mapping, memerr = b.Index.Get(id)
		if memerr != nil {
			if memerr == memcache.ErrCacheMiss {
				log.Trace("get memcache cache miss: ", memerr)
				return nil, false
			} else {
				toCounter++
				log.Debugf("get memcache error in GetIndex: %s. Counter is at %d ", memerr, toCounter)
				if toCounter > 9 {
					log.Errorf("read from memcacheserver GetIndex: %s. tried %d times with id %d", memerr, toCounter, id)
					return nil, false
				}
			}
		} else {
			//b.Statistics.MemcacheGets++
			timeout = false
		}
	}
	dec := gob.NewDecoder(bytes.NewBuffer(mapping.Value))
	if decerr := dec.Decode(&blockmap); decerr != nil {
		log.Error("decode error in GetIndex: ", decerr)
		return nil, false
	}

	return blockmap, true
}

// Inserts last statistics and returns a copy. The internal statistics are reset.
/*func (b *BlockIndex) finish() BlockIndexStatistics {*/
//statistics out commented because of memcache integration
//b.Statistics.IndexEntryCount = len(b.Index)

/*var sum int64*/
/*var sumSavedChunks int64*/
/*_ = sum*/
/*_ = sumSavedChunks*/

//b.IndexRWLock.RLock()
/*for _, bm := range b.Index {
	sum += int64(bm.Size())
	sumSavedChunks += int64(bm.SavedChunks())
}*/
//b.IndexRWLock.RUnlock()

//b.Statistics.AvgFpPerEntry = float32(float64(sum) / float64(len(b.Index)))
//b.Statistics.AvgSavedChunks = float32(float64(sumSavedChunks) / float64(len(b.Index)))
//b.Statistics.finish()
/*ret := b.Statistics*/
//b.Statistics.reset()
/*return ret*/
/*return nil*/
/*}*/

// deletes all entries with the indices between low and high
// Delete locks the block index, therefore external locking is unnecessary.

//out comment because of memcache migration
/*func (b *BlockIndex) Delete(low uint32, high uint32) {
	b.IndexRWLock.Lock()
	for key := range b.Index {
		if (low <= key) && (key < high) {
			delete(b.Index, key)
		}
	}
	b.IndexRWLock.Unlock()
}*/

func (b *BlockIndex) flushFromDownto(high uint32, low uint32, doneChan chan<- bool) {
	log.Debugf("starting flushing in range [%v, %v]", low, high)
	var cnt int
	for i := high - 1; (i >= low) && (i > 0); i-- {
		b.IndexRWLock.RLock()
		bm, ok := b.localIndex[i]
		b.IndexRWLock.RUnlock()

		if ok {
			if !b.AddIndex(i, bm) {
				log.Error("Couldn't flush blockMapping with id ", i)
				doneChan <- false
				return
			} else {
				cnt++
				b.IndexRWLock.Lock()
				delete(b.localIndex, i)
				b.IndexRWLock.Unlock()
			}

		}
	}

	doneChan <- true
	log.Debugf("flushing done in range [%v, %v], succesfully flushed %v blocks", low, high, cnt)
	return

}

func (b *BlockIndex) FlushIDsFromDownto(from uint32, downto uint32) bool {

	if b.Index != nil {
		log.Infof("Flushing BlockIndex in range [%v, %v]", downto, from)
	} else {
		log.Infof("Skip flushing BlockIndex because of unconfigured memcache")
		return true
	}

	numEntriesBefore := len(b.localIndex)

	const maxCategories = 64
	boundaries := make([]uint32, 0)
	for i := downto; i < from; i += (from - downto) / maxCategories {
		boundaries = append(boundaries, i)
	}
	boundaries = append(boundaries, from)

	const maxParallel = 4
	done := make(chan bool, maxParallel)
	numRunning := 0

	for i := 0; i < len(boundaries)-1; i++ {

		if numRunning < maxParallel {
			go b.flushFromDownto(boundaries[i+1], boundaries[i], done)
			numRunning++
		} else {
			<-done
			go b.flushFromDownto(boundaries[i+1], boundaries[i], done)
		}
	}

	// wait for them
	for i := 0; i < numRunning; i++ {
		<-done
	}

	log.Infof("flushing done; num entries before: %v, after: %v", numEntriesBefore, len(b.localIndex))
	return true
}

// Adds the given block mapping under the given id to the index. WARNING: Do not
// use or modify the mapping afterwards. For performance reasons the mapping
// directly is put into the index.
// Add also locks the block index, therefore no external locking is necessary.
func (b *BlockIndex) Add(id uint32, mapping BlockMapper) {
	b.IndexRWLock.Lock()
	b.localIndex[id] = mapping
	/*b.AddIndex(id, mapping)*/
	b.IndexRWLock.Unlock()
}

// Gets the entry for the given blockId. Returns two values. The first is the BlockMapping,
// the second one a bool indicating whether the get was successfull. This follows the rules
// of map lookups and similar data structures in Go.
func (b *BlockIndex) Get(id uint32, countIO bool, stats *BlockIndexStatistics, streamId string) (BlockMapper, bool) {

	b.IndexRWLock.RLock()
	defer b.IndexRWLock.RUnlock()
	if countIO {
		indexPage := b.getPageForBlock(id)
		if _, ok := b.pageCache.Contains(indexPage); !ok { // contains also touched the block
			stats.StorageCount++
			stats.StorageByteCount += int64(b.pageSize)
			if b.traceChan != nil {
				b.traceChan <- fmt.Sprintf("%v\t%v\n", streamId, id)
			}
		}
	}

	var bm BlockMapper = nil
	var ok bool
	if bm, ok = b.localIndex[id]; ok {
		stats.LocalIndexGets++
		stats.FetchCount++
		stats.FetchChunkCount += bm.Size()

	} else if b.Index != nil {
		if bm, ok = b.GetIndex(id); ok {
			stats.FetchCount++
			stats.FetchChunkCount += bm.Size()
		}
	}

	return bm, ok
}

func (b *BlockIndex) getPageForBlock(blockId uint32) uint32 {
	averageChunksPerBlock := (b.blockSize / (b.avgChunkSize))
	estimatedBlockMappingSize := averageChunksPerBlock * 24
	blockMappingsPerPage := b.pageSize / estimatedBlockMappingSize
	return blockId / uint32(blockMappingsPerPage)
}

// fetches all BlockMappings that are in the same page as the given BlockMapping
// WARNING: The returned list is reused in future fetchFullPage-calls (because of performance reasons).
// Only work with copies of that list if you want to store it somewhere.
func (b *BlockIndex) fetchFullPage(blockId uint32, countIO bool, blockMappingList *list.List, stats *BlockIndexStatistics, streamId string) {

	indexPage := b.getPageForBlock(blockId)

	b.IndexRWLock.RLock()
	if countIO {
		if _, ok := b.pageCache.Contains(indexPage); !ok {
			stats.StorageCount++
			stats.StorageByteCount += int64(b.pageSize)
			if b.traceChan != nil {
				b.traceChan <- fmt.Sprintf("%v\t%v\n", streamId, blockId)
			}
		}
	}

	blockMappingList.Init()

	if blockMapping, ok := b.localIndex[blockId]; ok {
		blockMappingList.PushBack(&BlockIdBlockMappingPair{blockId, blockMapping})
		stats.FetchCount++
		stats.FetchChunkCount += blockMapping.Size()
	} else if b.Index != nil {
		if blockMapping, ok := b.GetIndex(blockId); ok {
			blockMappingList.PushBack(&BlockIdBlockMappingPair{blockId, blockMapping})
			stats.FetchCount++
			stats.FetchChunkCount += blockMapping.Size()
		}
	}

	// collect all blockMappings that are smaller than blockId
	for i := uint32(1); b.getPageForBlock(blockId-i) == indexPage; i++ {
		if blockMapping, ok := b.localIndex[blockId-i]; ok {
			stats.FetchCount++
			stats.FetchChunkCount += blockMapping.Size()
			blockMappingList.PushBack(&BlockIdBlockMappingPair{blockId - i, blockMapping})
		} else if b.Index != nil {
			if blockMapping, ok := b.GetIndex(blockId - i); ok {
				stats.FetchCount++
				stats.FetchChunkCount += blockMapping.Size()
				blockMappingList.PushBack(&BlockIdBlockMappingPair{blockId - i, blockMapping})
			}
		}
	}

	// collect all blockMappings that are bigger than blockId
	for i := uint32(1); b.getPageForBlock(blockId+i) == indexPage; i++ {
		if blockMapping, ok := b.localIndex[blockId+i]; ok {
			stats.FetchCount++
			stats.FetchChunkCount += blockMapping.Size()
			blockMappingList.PushBack(&BlockIdBlockMappingPair{blockId + i, blockMapping})
		} else if b.Index != nil {
			if blockMapping, ok := b.GetIndex(blockId + i); ok {
				stats.FetchCount++
				stats.FetchChunkCount += blockMapping.Size()
				blockMappingList.PushBack(&BlockIdBlockMappingPair{blockId + i, blockMapping})
			}
		}
	}

	b.IndexRWLock.RUnlock()
}
