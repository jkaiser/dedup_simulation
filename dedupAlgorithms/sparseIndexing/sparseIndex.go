package sparseIndexing

import "container/list"
import "sync"
import "sync/atomic"
import "bytes"
import "strconv"
import "fmt"
import "encoding/gob"
import "runtime"

import "github.com/bradfitz/gomemcache/memcache"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

// Implementation for an ArraySet. Useful for set sizes below 32 units
type ArraySet struct {
	set [][12]byte
}

func NewArraySet() *ArraySet {
	return &ArraySet{set: make([][12]byte, 0, 32)}
}
func (sa *ArraySet) Size() int { return len(sa.set) }
func (sa *ArraySet) Clear()    { sa.set = sa.set[:0] }
func (sa *ArraySet) Add(fp *[12]byte) {
	for i := range sa.set {
		if sa.set[i] == *fp {
			return
		}
	}
	sa.set = append(sa.set, *fp)
}
func (sa *ArraySet) Remove(fp *[12]byte) {
	for i := range sa.set {
		if sa.set[i] == *fp {
			sa.set[i], sa.set[len(sa.set)-1] = sa.set[len(sa.set)-1], sa.set[i]
			sa.set = sa.set[:len(sa.set)-1]
			break
		}
	}
}
func (sa *ArraySet) Contains(fp *[12]byte) bool {
	for index, _ := range sa.set {
		if sa.set[index] == *fp {
			return true
		}
	}
	return false
}
func (sa *ArraySet) Foreach(f func(*[12]byte)) {
	for i := range sa.set {
		f(&sa.set[i])
	}
}

// representation of a Segment
type Segment struct {
	Id     uint32
	Chunks map[[12]byte]struct{}
	Offset int64
}

func (s *Segment) onDiskSize() int {
	return 24 + (len(s.Chunks) * 20)
}

func (s *Segment) clear() {
	s.Id = 0
	s.Offset = 0
	for fp := range s.Chunks {
		delete(s.Chunks, fp)
	}
}

func NewSegment(id uint32, expectedNumChunks int) *Segment {
	return &Segment{
		Id:     id,
		Chunks: make(map[[12]byte]struct{}, expectedNumChunks),
	}
}

type SparseIndexStatistics struct {
	ItemCount        int
	SegmentCount     int
	LookupCount      int
	UpdateCount      int
	StorageCount     int64
	StorageByteCount int64

	CacheHitCount  int
	CacheMissCount int
	HitRatio       float32

	TotalUniqueChunks int

	AvgSegmentsPerHook        float32
	AvgUniqueChunksPerSegment float32
	AvgSegmentRefCount        float32
	ObsoleteSegmentsCount     int
	RecycledSegmentsCount     int

	/*GenerationFetches map[string]int*/
}

func NewSparseIndexStatistics() SparseIndexStatistics {
	/*return SparseIndexStatistics{GenerationFetches: make(map[string]int)}*/
	return SparseIndexStatistics{}
}

func (ss *SparseIndexStatistics) add(that SparseIndexStatistics) {
	ss.ItemCount += that.ItemCount
	ss.SegmentCount += that.SegmentCount
	ss.LookupCount += that.LookupCount
	ss.UpdateCount += that.UpdateCount
	ss.StorageCount += that.StorageCount
	ss.StorageByteCount += that.StorageByteCount

	ss.CacheHitCount += that.CacheHitCount
	ss.CacheMissCount += that.CacheMissCount

	ss.TotalUniqueChunks += that.TotalUniqueChunks

	ss.ObsoleteSegmentsCount += that.ObsoleteSegmentsCount
	ss.RecycledSegmentsCount += that.RecycledSegmentsCount

	/*for gen, val := range that.GenerationFetches {*/
	/*ss.GenerationFetches[gen] += val*/
	/*}*/
}

func (ss *SparseIndexStatistics) finish() {
	if (ss.CacheHitCount + ss.CacheMissCount) > 0 {
		ss.HitRatio = float32(ss.CacheHitCount) / float32(ss.CacheHitCount+ss.CacheMissCount)
	} else {
		ss.HitRatio = 0.0
	}
}

func (ss *SparseIndexStatistics) reset() {
	ss.ItemCount = 0
	ss.SegmentCount = 0

	ss.LookupCount = 0
	ss.UpdateCount = 0
	ss.StorageCount = 0
	ss.StorageByteCount = 0

	ss.CacheHitCount = 0
	ss.CacheMissCount = 0
	ss.HitRatio = 0.0

	ss.TotalUniqueChunks = 0

	ss.ObsoleteSegmentsCount = 0
	ss.RecycledSegmentsCount = 0
	ss.AvgSegmentsPerHook = 0.0
	ss.AvgUniqueChunksPerSegment = 0.0
	ss.AvgSegmentRefCount = 0.0

	/*ss.GenerationFetches = make(map[string]int)*/
}

type SparseIndex struct {
	indexPageCount     int
	avgChunkSize       int
	sampleFactor       int
	segmentSize        int
	maxHooksPerSegment int

	sparseIndex            map[[12]byte]*list.List
	sparseIndexRWLock      sync.RWMutex
	localSegmentIndex      map[uint32]*Segment
	mcIndex                *memcache.Client
	segmentIndexRWLock     sync.RWMutex
	segmentIdCounter       uint32
	smallestLocalSegmentId uint32

	segmentRWLocks []sync.RWMutex // locks for the segments themselves. Necessary as getSegments returns a list of them instead of a copy
	ioTraceChan    chan string

	currentSegmentOffset int64
	cache                *common.UInt32Cache
	cacheLock            sync.RWMutex

	dirtySegments chan *Segment
	cleanSegments chan *Segment

	firstIdOfGenIndex       []uint32
	recycledSegmentsCounter int

	flushingTokenChan chan bool
}

func NewSparseIndex(indexPageCount int,
	avgChunkSize int,
	cacheSize int,
	sampleFactor int,
	segmentSize int,
	maxHooksPerSegment int,
	ioTraceChan chan string) *SparseIndex {

	s := &SparseIndex{
		indexPageCount:     indexPageCount,
		avgChunkSize:       avgChunkSize,
		sampleFactor:       sampleFactor,
		segmentSize:        segmentSize,
		maxHooksPerSegment: maxHooksPerSegment,
		cache:              common.NewUInt32Cache(cacheSize, nil),
		sparseIndex:        make(map[[12]byte]*list.List, 5*1E6), // assuming ca 150M chunks with 1/32 sampling
		localSegmentIndex:  make(map[uint32]*Segment, 6*1E6),     // assuming ca 10 * 1TB / 2MB segments
		segmentRWLocks:     make([]sync.RWMutex, 1024),
		dirtySegments:      make(chan *Segment, 10000),
		cleanSegments:      make(chan *Segment, 10000),
		firstIdOfGenIndex:  make([]uint32, 0, 12),
		ioTraceChan:        ioTraceChan,
		flushingTokenChan:  make(chan bool, 1),
	}

	s.flushingTokenChan <- true
	go s.recycle()
	return s
}

//set memclient as Index
func (si *SparseIndex) setIndex(client *memcache.Client) {
	si.mcIndex = client
}

func (si *SparseIndex) finish(that *SparseIndexStatistics) {
	//var sum int
	si.sparseIndexRWLock.RLock()
	//for _, list := range si.sparseIndex {
	//sum += list.Len()
	//}

	that.RecycledSegmentsCount = si.recycledSegmentsCounter
	//if (sum > 0) && (len(si.sparseIndex) > 0) {
	//that.AvgSegmentsPerHook = float32(float64(sum) / float64(len(si.sparseIndex)))
	//}

	//sum = 0

	that.ItemCount = len(si.sparseIndex)
	that.SegmentCount = int(si.segmentIdCounter)
	si.sparseIndexRWLock.RUnlock()
}

func (si *SparseIndex) beginNewGeneration() {
	if (si.mcIndex != nil) && (len(si.firstIdOfGenIndex) > 1) {
		si.FlushIDsFromDownto(si.firstIdOfGenIndex[len(si.firstIdOfGenIndex)-1], 0)
	}

	si.firstIdOfGenIndex = append(si.firstIdOfGenIndex, si.segmentIdCounter)
}

func (si *SparseIndex) FlushIDsFromDownto(from uint32, downto uint32) bool {
	log.Infof("Flushing Segments in range [%v, %v]", downto, from)

	var totalNumFlushed int
	numEntriesBefore := len(si.localSegmentIndex)

	const numFlusher = 8
	segmentChan := make(chan *Segment, 100)
	doneChan := make(chan int)

	select {
	case <-si.flushingTokenChan:
	default:
		log.Info("delaying big flush because of running other flush")
		<-si.flushingTokenChan
	}
	defer func() {
		si.flushingTokenChan <- true
	}()

	for i := 0; i < numFlusher; i++ {
		go func() {
			var numFlushed int
			for seg := range segmentChan {
				if !si.AddIndex(seg) {
					log.Error("Couldn't flush segment with id ", seg.Id)
				} else {
					// mark it for recycling if possible
					si.segmentIndexRWLock.Lock()
					delete(si.localSegmentIndex, seg.Id)
					si.segmentIndexRWLock.Unlock()
					numFlushed++

					select { // recycle if possible
					case si.dirtySegments <- seg:
					default:
					}
				}
			}
			doneChan <- numFlushed
		}()
		log.Debug("Started flusher")
	}

	atomic.StoreUint32(&si.smallestLocalSegmentId, from) // theoretically there can be some below from because of errors, but they are few and I don't care

	// put to chan
	for i := from; i != downto; i-- {
		si.segmentIndexRWLock.RLock()
		segment, ok := si.localSegmentIndex[i]
		si.segmentIndexRWLock.RUnlock()

		if ok {
			segmentChan <- segment
		} else { // reached end
			break
		}
	}
	close(segmentChan)

	// join
	for i := 0; i < numFlusher; i++ {
		log.Debug("Joined flusher")
		totalNumFlushed += <-doneChan
	}

	log.Infof("Flushed %v Segments. numEntries before: %v, after: %v", totalNumFlushed, numEntriesBefore, len(si.localSegmentIndex))
	return true
}

// flushes the first p percent (==ratio) of the current local localSegmentIndex
func (si *SparseIndex) FlushLowerPart(ratio float32) {
	const numFlusher = 4

	high := atomic.LoadUint32(&si.segmentIdCounter)
	low := atomic.LoadUint32(&si.smallestLocalSegmentId)
	threshold := uint32(float32((high - low)) * ratio)
	log.Infof("Flushing %v of the segments with %v routines; range [%v, %v]", ratio, numFlusher, low, low+threshold)

	segmentChan := make(chan *Segment, 100)
	doneChan := make(chan bool)

	for i := uint32(0); i < numFlusher; i++ {
		go func() {

			for seg := range segmentChan {
				if si.AddIndex(seg) {
					si.segmentIndexRWLock.Lock()
					delete(si.localSegmentIndex, seg.Id) // no recycle here since the segment itself could be in flight from getSegments
					si.segmentIndexRWLock.Unlock()

					sm := atomic.LoadUint32(&si.smallestLocalSegmentId)
					for (sm < i) && !atomic.CompareAndSwapUint32(&si.smallestLocalSegmentId, sm, i) {
						sm = atomic.LoadUint32(&si.smallestLocalSegmentId)
					}
				} // no else. If add fails it doesn't hurt
			}

			doneChan <- true

		}()
	}

	for i := low; i < low+threshold; i++ {
		si.segmentIndexRWLock.RLock()
		seg, ok := si.localSegmentIndex[i]
		si.segmentIndexRWLock.RUnlock()

		if !ok {
			continue
		} else {
			segmentChan <- seg
		}
	}

	close(segmentChan)

	// join
	for i := 0; i < numFlusher; i++ {
		log.Debug("Joined flusher")
		<-doneChan
	}
}

// checks the current segmentIndex size/memory consumption and starst flushing if necessary
func (si *SparseIndex) flushIfNecessary() {
	const ratio = 0.5
	const localSegmentIndexThreshold = 100000
	const memoryThreshold = 70 // GB

	if si.mcIndex == nil {
		return
	}

	select {
	case <-si.flushingTokenChan:
		defer func() {
			si.flushingTokenChan <- true
		}()
	default:
		return
	}

	mStats := new(runtime.MemStats)
	runtime.ReadMemStats(mStats)
	if (len(si.localSegmentIndex) > localSegmentIndexThreshold) && (mStats.Alloc/(1024*1024*1024) > memoryThreshold) {
		log.Infof("memusage above %vGB; localSegmentIndex has more than %v entries => flush %v ", mStats.Alloc/(1024*1024*1024), len(si.localSegmentIndex), ratio)
		si.FlushLowerPart(ratio)
	}
}

func (si *SparseIndex) recycle() {

	var seg *Segment
	for {
		seg = <-si.dirtySegments
		seg.clear()
		si.cleanSegments <- seg
		si.recycledSegmentsCounter++
	}
}

// computes whether the given fp is a Hook
func (si *SparseIndex) isHook(fp *[12]byte) bool {
	for i := 0; i < si.sampleFactor/8; i++ {
		if fp[i] != byte(0) {
			return false
		}
	}

	shifts := uint32(si.sampleFactor % 8)
	mask := byte(^(0xFF << shifts))
	return (fp[si.sampleFactor/8] & mask) == 0
}

func (si *SparseIndex) getHooks(chunks [][12]byte, numChunksToConsider int) *ArraySet {
	s := NewArraySet()
	for i := 0; i < numChunksToConsider; i++ {
		if si.isHook(&chunks[i]) {
			s.Add(&chunks[i])
		}
	}
	return s
}

// Adds the segment to the SparseIndex.
func (si *SparseIndex) addSegment(chunks [][12]byte, numChunksToConsider int, stats *SparseIndexStatistics) {
	newId := atomic.AddUint32(&si.segmentIdCounter, 1)
	var segment *Segment

	select {
	case segment = <-si.cleanSegments:
		segment.Id = newId
	default:
		segment = NewSegment(newId, si.segmentSize/si.avgChunkSize)
	}

	for i := 0; i < numChunksToConsider; i++ {
		segment.Chunks[chunks[i]] = struct{}{}
	}

	si.segmentIndexRWLock.Lock()
	si.localSegmentIndex[newId] = segment
	si.segmentIndexRWLock.Unlock()

	for fp := range segment.Chunks {
		if !si.isHook(&fp) {
			continue
		}

		/*segment.refCount++*/
		si.sparseIndexRWLock.Lock()
		if seqList, ok := si.sparseIndex[fp]; ok {
			if seqList.Len() == si.maxHooksPerSegment {
				seqList.MoveToFront(seqList.Back())
				seqList.Front().Value = segment.Id // no good refcounting here since the old segment could be in memcache
			} else {
				seqList.PushFront(segment.Id)
			}

		} else { // no entry -> create new one
			newList := list.New()
			newList.PushFront(segment.Id)
			si.sparseIndex[fp] = newList
		}

		si.sparseIndexRWLock.Unlock()
	}

	if (newId % 50000) == 0 {
		go si.flushIfNecessary()
	}

	stats.UpdateCount++
}

func (si *SparseIndex) roundUpTo(v int, v2 int) int {
	d := v / v2
	r := v % v2
	if r == 0 {
		return v
	} else {
		return (d + 1) * v2
	}
}

// returns the id and the hook set of the best matching candidate
// According to the paper, the best matching is the candidate with the most matching hooks. If
// there are several candidates with the same number, the algorithm chooses the last stored one
// (== the one with the biggest segment id)
func (si *SparseIndex) getNextChampion(candidates map[uint32]*ArraySet) (uint32, *ArraySet) {
	num_hooks := 0
	var best_id uint32
	var best_set *ArraySet

	for id, hookSet := range candidates {
		if hookSet.Size() > num_hooks {
			best_id = id
			num_hooks = hookSet.Size()
			best_set = hookSet
		} else if (hookSet.Size() == num_hooks) && (id > best_id) { // TODO: das ist in Dirk's version nicht so
			best_id = id
			num_hooks = hookSet.Size()
			best_set = hookSet
		}
	}

	return best_id, best_set
}

// unlocks the segments. This is part of the locks_per_segment scheme which is an
// alternative to returning copies of segmetns in getSegments.
func (si *SparseIndex) unlockSegments(segments []*Segment) {
	for _, seg := range segments {
		si.segmentRWLocks[seg.Id%uint32(len(si.segmentRWLocks))].RUnlock()
	}
}

// Gets the segments of the hooks. Returns a list of these segments.
func (si *SparseIndex) getSegments(hooks *ArraySet, maxSegmentCount int, countIO bool, championList []*Segment, stats *SparseIndexStatistics, streamId string) []*Segment {

	candidates := make(map[uint32]*ArraySet)

	// build index segmentId -> [list of common hooks]
	si.sparseIndexRWLock.RLock()
	hooks.Foreach(func(fp *[12]byte) {
		if segmentList, ok := si.sparseIndex[*fp]; ok {
			for e := segmentList.Front(); e != nil; e = e.Next() {
				if set, ok := candidates[e.Value.(uint32)]; ok {
					set.Add(fp)
				} else {
					newSet := NewArraySet()
					newSet.Add(fp)
					candidates[e.Value.(uint32)] = newSet
				}
			}
		}
	})
	si.sparseIndexRWLock.RUnlock()

	//////////////////// NEW APPROACH  ////////////////////
	//// determine champions
	//championIDs := make([]uint32, 0, 32)
	//for (len(championIDs) < maxSegmentCount) && (len(candidates) != 0) {

	//champion, commonHooks := si.getNextChampion(candidates)
	//championIDs = append(championIDs, champion)

	//delete(candidates, champion)
	//commonHooks.Foreach(func(fp *[12]byte) {
	//for segId, set := range candidates {
	//set.Remove(fp)
	//if set.Size() == 0 {
	//delete(candidates, segId)
	//}
	//}
	//})
	//}

	//toFetchFromMC := make([]string, 0, 32)
	//remaining := make(map[uint32]struct{}, 32)
	//si.segmentIndexRWLock.RLock()
	//for _, chmpID := range championIDs {
	//si.segmentRWLocks[chmpID%uint32(len(si.segmentRWLocks))].RLock() // gets released in calling function
	//if seg, ok := si.localSegmentIndex[chmpID]; ok {
	//championList = append(championList, seg)
	//} else {
	//toFetchFromMC = append(toFetchFromMC, strconv.FormatInt(int64(chmpID), 10))
	//remaining[chmpID] = struct{}{}
	//}
	//}
	////log.Infof("%v", toFetchFromMC)
	////log.Infof("%v", championList)

	//// fetch from MC if necessary
	//if len(toFetchFromMC) > 0 {
	//segments := si.MultiGetIndex(toFetchFromMC)
	//for _, seg := range segments {
	//championList = append(championList, seg)
	//delete(remaining, seg.Id)
	//}

	//if len(remaining) != 0 { // less returned than requested
	//for chmpId := range remaining {
	//log.Error("Couldn't fetch champion ", chmpId)
	//si.segmentRWLocks[chmpId%uint32(len(si.segmentRWLocks))].RUnlock() // as it was wrongly locked before
	//}
	//}
	//}

	//si.segmentIndexRWLock.RUnlock()
	//////////////////// END NEW APPROACH  ////////////////////

	//////////////////// TRADITIONAL APPROACH  ////////////////////
	//// choose champions
	//for (len(championList) < maxSegmentCount) && (len(candidates) != 0) {
	//champion, commonHooks := si.getNextChampion(candidates)

	//si.segmentIndexRWLock.RLock()
	//var seg *Segment
	//var ok bool
	//if seg, ok = si.localSegmentIndex[champion]; !ok {
	//if seg, ok = si.GetIndex(champion); !ok {
	//log.Error("Couldn't fetch champion ", champion)

	//si.segmentIndexRWLock.RUnlock()
	//delete(candidates, champion)
	////stats.UnfetchableChampions++
	//continue
	//}
	//}
	////[>seg := si.segmentIndex[champion]<]*/
	//si.segmentRWLocks[seg.Id%uint32(len(si.segmentRWLocks))].RLock() // gets released in calling function

	//championList = append(championList, seg)
	//si.segmentIndexRWLock.RUnlock()
	//delete(candidates, champion)

	///*// remove hooks of that champion from the candidates*/
	//commonHooks.Foreach(func(fp *[12]byte) {
	//for segId, set := range candidates {
	//set.Remove(fp)
	//if set.Size() == 0 {
	//delete(candidates, segId)
	//}
	//}
	//})
	//}
	//////////////////// END TRADITIONAL APPROACH  ////////////////////

	//////////////////// MODIFIED TRADITIONAL APPROACH  ////////////////////
	championIDs := make([]uint32, 0, 32)
	for (len(championList) < maxSegmentCount) && (len(candidates) != 0) {
		champion, commonHooks := si.getNextChampion(candidates)
		delete(candidates, champion)

		/*// remove hooks of that champion from the candidates*/
		commonHooks.Foreach(func(fp *[12]byte) {
			for segId, set := range candidates {
				set.Remove(fp)
				if set.Size() == 0 {
					delete(candidates, segId)
				}
			}
		})
		championIDs = append(championIDs, champion)
	}

	toFetchFromMC := make([]string, 0)
	remaining := make(map[uint32]struct{})
	for _, champion := range championIDs {
		si.segmentIndexRWLock.RLock()
		if seg, ok := si.localSegmentIndex[champion]; ok {
			championList = append(championList, seg)
			si.segmentRWLocks[seg.Id%uint32(len(si.segmentRWLocks))].RLock() // gets released in calling function
		} else {
			toFetchFromMC = append(toFetchFromMC, strconv.FormatInt(int64(champion), 10))
			remaining[champion] = struct{}{}
		}
		si.segmentIndexRWLock.RUnlock()
	}

	// fetch from MC if necessary
	if len(toFetchFromMC) > 0 {
		//log.Infof("fetch %v champions from memcache: %v", len(toFetchFromMC), toFetchFromMC)
		si.segmentIndexRWLock.RLock()

		segments := si.MultiGetIndex(toFetchFromMC)
		for _, seg := range segments {
			//log.Infof("got segment %v", seg.Id)

			championList = append(championList, seg)
			si.segmentRWLocks[seg.Id%uint32(len(si.segmentRWLocks))].RLock() // gets released in calling function
			delete(remaining, seg.Id)
		}

		if len(remaining) != 0 { // less returned than requested
			for chmpId := range remaining {
				log.Error("Couldn't fetch champion ", chmpId)
			}
		}
		si.segmentIndexRWLock.RUnlock()
	}

	//for _, champion := range championIDs {
	//si.segmentIndexRWLock.RLock()
	//var seg *Segment
	//var ok bool
	//if seg, ok = si.localSegmentIndex[champion]; !ok {
	//if seg, ok = si.GetIndex(champion); !ok {
	//log.Error("Couldn't fetch champion ", champion)

	//si.segmentIndexRWLock.RUnlock()
	//delete(candidates, champion)
	////stats.UnfetchableChampions++
	//continue
	//}
	//}
	////[>seg := si.segmentIndex[champion]<]*/
	//si.segmentRWLocks[seg.Id%uint32(len(si.segmentRWLocks))].RLock() // gets released in calling function
	//championList = append(championList, seg)
	//si.segmentIndexRWLock.RUnlock()
	//}
	//////////////////// END MODIFIED TRADITIONAL APPROACH  ////////////////////

	stats.LookupCount += len(championList)

	// champion list complete, now count IO if wished
	if countIO {
		si.cacheLock.Lock()
		for _, champion := range championList {

			if _, ok := si.cache.Contains(champion.Id); ok {
				stats.CacheHitCount++
			} else {
				stats.CacheMissCount++
				stats.StorageCount++
				stats.StorageByteCount += int64(si.roundUpTo(champion.onDiskSize(), 4096))

				si.cache.Update(champion.Id, 1)

				if si.ioTraceChan != nil {
					si.ioTraceChan <- fmt.Sprintf("%v\t%v\n", streamId, champion.Id)
				}
			}
		}
		si.cacheLock.Unlock()
	}

	return championList
}

//add to b.Index
func (si *SparseIndex) AddIndex(segment *Segment) bool {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if encErr := enc.Encode(segment); encErr != nil {
		log.Error("encode error addIndex:", encErr)
	}

	//try to add for max 10 times
	id := strconv.FormatInt(int64(segment.Id), 10)
	done := false
	var memerr error
	for i := 0; i < 10; i++ {
		if memerr = si.mcIndex.Set(&memcache.Item{Key: id, Value: buf.Bytes()}); memerr != nil {
			log.Debugf("write to memcacheserver failed. addIndex: %s. Counter is at %d ", memerr, i)
		} else {
			done = true
			break
		}
	}

	if !done {
		log.Errorf("Write to memcacheserver failed for segment %s : %s", segment.Id, memerr)
	}

	return done
}

func (si *SparseIndex) MultiGetIndex(ids []string) []*Segment {

	buf := bytes.NewBuffer(nil)
	ret_segments := make([]*Segment, 0, len(ids))

	remaining := make(map[string]struct{})
	for i := range ids {
		remaining[ids[i]] = struct{}{}
	}

	var mapping map[string]*memcache.Item
	var memerr error
	for i := 0; i < 10 && len(remaining) > 0; i++ {

		if mapping, memerr = si.mcIndex.GetMulti(ids); memerr != nil {
			if memerr == memcache.ErrCacheMiss {
				log.Error("get memcache cache miss: ", memerr)
			} else {
				log.Debug("MultiGet memcache error: ", memerr)
			}
		}

		var segment *Segment
		if mapping != nil {
			for id, item := range mapping {
				select {
				case s := <-si.dirtySegments:
					segment = s
				default:
					segment = new(Segment)
				}

				buf.Reset()
				buf.Write(item.Value)
				dec := gob.NewDecoder(buf)
				//dec := gob.NewDecoder(bytes.NewBuffer(item.Value))
				if decerr := dec.Decode(segment); decerr != nil {
					log.Error("decode error in MultiGetIndex: ", decerr)
				} else {
					ret_segments = append(ret_segments, segment)
				}
				delete(remaining, id)
			}
		}
	}

	if len(remaining) > 0 {
		log.Errorf("Couldn't get segments %v : %v", remaining, memerr)
	}

	return ret_segments
}

//get from b.Index
func (si *SparseIndex) GetIndex(idd uint32) (*Segment, bool) {
	var segment *Segment

	select {
	case s := <-si.dirtySegments:
		segment = s
	default:
		segment = new(Segment)
	}

	var mapping *memcache.Item
	var memerr error
	id := strconv.FormatInt(int64(idd), 10)
	done := false
	toCounter := 0

	for !done {
		if mapping, memerr = si.mcIndex.Get(id); memerr != nil {
			if memerr == memcache.ErrCacheMiss {
				log.Error("get memcache cache miss: ", memerr)
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
			done = true
		}
	}
	dec := gob.NewDecoder(bytes.NewBuffer(mapping.Value))
	if decerr := dec.Decode(segment); decerr != nil {
		log.Error("decode error in GetIndex: ", decerr)
		return nil, false
	}

	return segment, true
}
