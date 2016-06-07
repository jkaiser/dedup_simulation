package sortedChunkIndexing

import "bytes"
import "sort"
import "fmt"
import "sync"
import log "github.com/cihub/seelog"

const ciEntrySize = 24

type ByFp [][12]byte

func (c ByFp) Len() int           { return len(c) }
func (c ByFp) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c ByFp) Less(i, j int) bool { return (bytes.Compare(c[i][:], c[j][:]) == -1) }

type SortedChunkIndexStatistics struct {
	ItemCount                 int64
	PageCount                 uint32
	LookupCount               int
	UpdateCount               int
	FlushCount                int
	Hits                      int
	Misses                    int
	MemIndexHits              int
	MemIndexMisses            int
	MemIndexHitRatio          float32
	DiskIndexHits             int
	DiskIndexMisses           int
	DiskIndexHitRatio         float32
	StorageCount              int64
	StorageCountDuplicates    int64
	StorageCountUniques       int64
	RelStorageCountDuplicates float32
	StorageByteCount          int64
	HitRatio                  float32
}

func (cs *SortedChunkIndexStatistics) add(that SortedChunkIndexStatistics) {
	if cs.PageCount < that.PageCount {
		cs.PageCount = that.PageCount // just set it. An addition makes no sense
	}

	cs.ItemCount += that.ItemCount
	cs.LookupCount += that.LookupCount
	cs.UpdateCount += that.UpdateCount
	cs.FlushCount += that.FlushCount
	cs.Hits += that.Hits
	cs.Misses += that.Misses
	cs.MemIndexHits += that.MemIndexHits
	cs.MemIndexMisses += that.MemIndexMisses
	cs.DiskIndexHits += that.DiskIndexHits
	cs.DiskIndexMisses += that.DiskIndexMisses
	cs.StorageCount += that.StorageCount
	cs.StorageCountDuplicates += that.StorageCountDuplicates
	cs.StorageCountUniques += that.StorageCountUniques
	cs.StorageByteCount += that.StorageByteCount
}

func (cs *SortedChunkIndexStatistics) finish() {
	if cs.Misses > 0 {
		cs.HitRatio = float32(float64(cs.Hits) / float64(cs.Hits+cs.Misses))
	} else {
		cs.HitRatio = 0.0
	}

	if cs.MemIndexMisses > 0 {
		cs.MemIndexHitRatio = float32(float64(cs.MemIndexHits) / float64(cs.MemIndexHits+cs.MemIndexMisses))
	} else {
		cs.MemIndexHitRatio = 0.0
	}

	if cs.DiskIndexMisses > 0 {
		cs.DiskIndexHitRatio = float32(float64(cs.DiskIndexHits) / float64(cs.DiskIndexHits+cs.DiskIndexMisses))
	} else {
		cs.DiskIndexHitRatio = 0.0
	}

	if (cs.StorageCountUniques + cs.StorageCountDuplicates) > 0 {
		cs.RelStorageCountDuplicates = float32(float64(cs.StorageCountDuplicates) / float64(cs.StorageCountDuplicates+cs.StorageCountUniques))
	}
}

func (cs *SortedChunkIndexStatistics) reset() {
	cs.ItemCount = 0
	cs.PageCount = 0
	cs.LookupCount = 0
	cs.UpdateCount = 0
	cs.FlushCount = 0
	cs.Hits = 0
	cs.Misses = 0
	cs.MemIndexHits = 0
	cs.MemIndexMisses = 0
	cs.MemIndexHitRatio = 0
	cs.DiskIndexHits = 0
	cs.DiskIndexMisses = 0
	cs.DiskIndexHitRatio = 0
	cs.StorageCount = 0
	cs.StorageCountDuplicates = 0
	cs.StorageCountUniques = 0
	cs.StorageByteCount = 0
	cs.HitRatio = 0
}

type SortedChunkIndex struct {
	diskIndex        map[[12]byte]uint32
	memIndex         map[[12]byte]struct{}
	memList          ByFp
	chunkIndexRWLock *sync.RWMutex
	Statistics       SortedChunkIndexStatistics
	pageSize         uint32
	currentPage      uint32
	memLimit         int64

	allFp          ByFp
	firstPageIndex map[[12]byte]struct{}
	traceChan      chan string
}

func NewSortedChunkIndex(pageSize int, chunkIndexMemoryLimit int64, traceChan chan string) *SortedChunkIndex {
	if chunkIndexMemoryLimit == 0 {
		panic("invalid memory size")
	}
	return &SortedChunkIndex{
		memIndex:         make(map[[12]byte]struct{}, chunkIndexMemoryLimit/ciEntrySize),
		memList:          make([][12]byte, 0, chunkIndexMemoryLimit/ciEntrySize),
		diskIndex:        make(map[[12]byte]uint32, 1e7),
		chunkIndexRWLock: new(sync.RWMutex),
		pageSize:         uint32(pageSize),
		memLimit:         chunkIndexMemoryLimit,
		allFp:            make([][12]byte, 0),
		firstPageIndex:   make(map[[12]byte]struct{}, pageSize/ciEntrySize),
		traceChan:        traceChan,
	}
}

func (ci *SortedChunkIndex) SetPageSize(newPageSize uint32) {
	ci.pageSize = newPageSize
}

func (ci *SortedChunkIndex) Add(fp *[12]byte, stats *SortedChunkIndexStatistics) bool {
	var isRedundant bool

	ci.chunkIndexRWLock.Lock()
	defer ci.chunkIndexRWLock.Unlock()

	if _, isRedundant = ci.diskIndex[*fp]; !isRedundant {
		_, isRedundant = ci.memIndex[*fp]
	}

	if isRedundant {
		return false
	}
	ci.memIndex[*fp] = struct{}{}
	ci.memList = append(ci.memList, *fp)
	if stats != nil {
		stats.UpdateCount++
	}

	if (uint64(len(ci.memIndex)) * uint64(ciEntrySize)) >= uint64(ci.memLimit) {
		log.Infof("memIndex has %v elements (%v bytes), which is more than %v bytes => flush", len(ci.memIndex), uint64(len(ci.memIndex))*uint64(ciEntrySize), uint64(ci.memLimit))
		ci.Flush(stats)
	}
	return true
}

// checks for the given chunk fingerprint, but does not change the internal state
func (ci *SortedChunkIndex) CheckWithoutChange(fp *[12]byte) (ok bool) {

	ci.chunkIndexRWLock.RLock()

	if _, ok = ci.diskIndex[*fp]; !ok {
		_, ok = ci.memIndex[*fp]
	}
	ci.chunkIndexRWLock.RUnlock()
	return ok
}

// Checks the index for the fingerprints. It returns two values: a boolean that indicates the chunk existence in the index and a position. If the chunk resides on disk, the position is the id of the page that includes the chunk. Otherwise, it is the id of the page that would include the chunk if it would exist. Note that there is the possibility that the chunk exists, but isn't on disk, yet!
func (ci *SortedChunkIndex) Check(fp *[12]byte, countIO bool, currentPage uint32, stats *SortedChunkIndexStatistics, streamId string) (pos uint32, isDuplicate bool, memIndexHit bool) {
	if stats != nil {
		stats.UpdateCount++
	}

	ci.chunkIndexRWLock.RLock()
	defer ci.chunkIndexRWLock.RUnlock()

	if _, isDuplicate = ci.memIndex[*fp]; isDuplicate {
		memIndexHit = true
		if stats != nil {
			stats.MemIndexHits++
			stats.Hits++
		}

		pos = uint32(sort.Search(len(ci.allFp), func(i int) bool { return bytes.Compare(ci.allFp[i][:], fp[:]) >= 0 }))

	} else if pos, isDuplicate = ci.diskIndex[*fp]; isDuplicate {
		if stats != nil {
			stats.MemIndexMisses++
			stats.Hits++
		}

		switch {
		case pos == currentPage:
			if stats != nil {
				stats.DiskIndexHits++
			}
		case countIO && (pos != currentPage):
			if stats != nil {
				stats.DiskIndexMisses++
				stats.StorageCount++
				stats.StorageCountDuplicates++
				stats.StorageByteCount += int64(ci.pageSize)
			}
			if ci.traceChan != nil {
				ci.traceChan <- fmt.Sprintf("%v\t%v\n", streamId, pos)
			}

		case pos != currentPage:
			if stats != nil {
				stats.DiskIndexMisses++
			}
			/*ci.currentPage = pos*/
		default:
			log.Error("Unknown case in SortedChunkIndex. fp: %x, countIO: %v, pos: %v", *fp, countIO, pos)
		}

	} else { // Miss
		if stats != nil {
			stats.MemIndexMisses++
			stats.DiskIndexMisses++
			stats.Misses++
			stats.StorageCount++ // checking the index consumes IO even if we don't update its cache
			stats.StorageByteCount += int64(ci.pageSize)
			stats.StorageCountUniques++
		}

		if ci.traceChan != nil {
			i := sort.Search(len(ci.allFp), func(i int) bool { return bytes.Compare(ci.allFp[i][:], fp[:]) >= 0 })
			ci.traceChan <- fmt.Sprintf("%v\t%v\n", streamId, ci.getPageForPos(uint32(i)))
		}
	}

	return
}

type SortedIndexState struct {
	memIndex map[[12]byte]struct{}
	memList  [][12]byte
	allFp    [][12]byte
}

func (ci *SortedChunkIndex) GetState() *SortedIndexState {
	state := &SortedIndexState{
		memIndex: make(map[[12]byte]struct{}, len(ci.memIndex)),
		memList:  make([][12]byte, len(ci.memList)),
		allFp:    make([][12]byte, len(ci.allFp)),
	}

	for k, v := range ci.memIndex {
		state.memIndex[k] = v
	}

	copy(state.memList, ci.memList)
	copy(state.allFp, ci.allFp)

	return state
}

func (ci *SortedChunkIndex) SetState(newState *SortedIndexState) {

	for k := range ci.memIndex {
		delete(ci.memIndex, k)
	}
	for k, v := range newState.memIndex {
		ci.memIndex[k] = v
	}

	// rebuild memlist
	n := copy(ci.memList, newState.memList)
	if n == len(newState.memList) {
		ci.memList = ci.memList[:n]
	} else {
		ci.memList = append(ci.memList, newState.memList[n:]...)
	}

	// rebuild diskList
	n = copy(ci.allFp, newState.allFp)
	if n == len(newState.allFp) {
		ci.allFp = ci.allFp[:n]
	} else {
		ci.allFp = append(ci.allFp, newState.allFp[n:]...)
	}
}

// Flush the memory index to the disk one. This function assumes that the internal lock is held
func (ci *SortedChunkIndex) Flush2(stats *SortedChunkIndexStatistics) {

	if len(ci.memIndex) == 0 {
		return
	}

	for fp := range ci.memIndex {
		ci.allFp = append(ci.allFp, fp)
	}

	// sort
	sort.Sort(ci.allFp)

	// update
	ci.memIndex = make(map[[12]byte]struct{}, uint64(ci.memLimit)/ciEntrySize)
	ci.memList = ci.memList[:0]

	for i := range ci.allFp {
		ci.diskIndex[ci.allFp[i]] = ci.getPageForPos(uint32(i))
	}

	// rebuild first page index
	ci.firstPageIndex = make(map[[12]byte]struct{})
	var firstPage ByFp
	if len(ci.allFp) < ci.getNumChunksPerPage() {
		firstPage = ci.allFp[:len(ci.allFp)]
		log.Warnf("flush very small index! size = %v", len(ci.allFp))
	} else {
		firstPage = ci.allFp[:ci.getNumChunksPerPage()]
	}
	for i := range firstPage {
		ci.firstPageIndex[firstPage[i]] = struct{}{}
	}

	if stats != nil {
		stats.FlushCount++
	}
}

// Flush the memory index to the disk one. This function assumes that the internal lock is held
func (ci *SortedChunkIndex) Flush(stats *SortedChunkIndexStatistics) {

	if len(ci.memIndex) == 0 {
		return
	}

	// sort small list
	sort.Sort(ci.memList)

	new_allFp := make([][12]byte, len(ci.allFp)+len(ci.memList))

	var all_index int
	var mem_index int

	for i := range new_allFp {

		if (all_index < len(ci.allFp)) && (mem_index < len(ci.memList)) {
			if cmp := bytes.Compare(ci.allFp[all_index][:], ci.memList[mem_index][:]); cmp <= 0 {
				new_allFp[i] = ci.allFp[all_index]
				all_index++
			} else { // cmp==1
				new_allFp[i] = ci.memList[mem_index]
				mem_index++
			}

		} else if all_index < len(ci.allFp) {
			new_allFp[i] = ci.allFp[all_index]
			all_index++

		} else { // mem_index < len(ci.memList)
			new_allFp[i] = ci.memList[mem_index]
			mem_index++
		}
	}

	ci.allFp = new_allFp

	// update
	ci.memIndex = make(map[[12]byte]struct{}, uint64(ci.memLimit)/ciEntrySize)
	ci.memList = ci.memList[:0]

	for i := range ci.allFp {
		ci.diskIndex[ci.allFp[i]] = ci.getPageForPos(uint32(i))
	}

	// rebuild first page index
	ci.firstPageIndex = make(map[[12]byte]struct{})
	var firstPage ByFp
	if len(ci.allFp) < ci.getNumChunksPerPage() {
		firstPage = ci.allFp[:len(ci.allFp)]
		log.Warnf("flush very small index! size = %v", len(ci.allFp))
	} else {
		firstPage = ci.allFp[:ci.getNumChunksPerPage()]
	}
	for i := range firstPage {
		ci.firstPageIndex[firstPage[i]] = struct{}{}
	}

	if stats != nil {
		stats.FlushCount++
	}
}

func (ci *SortedChunkIndex) Finish(stats *SortedChunkIndexStatistics) {

	ci.chunkIndexRWLock.RLock()
	defer ci.chunkIndexRWLock.RUnlock()

	if stats == nil {
		ci.Statistics.ItemCount = int64(len(ci.memIndex) + len(ci.diskIndex))
		ci.Statistics.PageCount = ci.getNumPages()
	} else {
		stats.ItemCount = int64(len(ci.memIndex) + len(ci.diskIndex))
		stats.PageCount = ci.getNumPages()
	}
}

func (ci *SortedChunkIndex) getPageForPos(pos uint32) uint32 {
	entriesPerPage := ci.pageSize / ciEntrySize
	return pos / entriesPerPage
}

func (ci *SortedChunkIndex) getNumChunksPerPage() int {
	return int(ci.pageSize / ciEntrySize)
}

func (ci *SortedChunkIndex) getNumPages() (numPages uint32) {
	ci.chunkIndexRWLock.RLock()
	if len(ci.allFp) > 0 {
		numPages = ci.getPageForPos(uint32(len(ci.allFp)-1)) + 1 // +1 because the first page starts at index 0
	}

	ci.chunkIndexRWLock.RUnlock()
	return
}

// returns a copy of the first page.
func (ci *SortedChunkIndex) GetFirstPage() [][12]byte {
	var numChunks int
	ci.chunkIndexRWLock.RLock()
	defer ci.chunkIndexRWLock.RUnlock()

	if len(ci.allFp) < ci.getNumChunksPerPage() {
		numChunks = len(ci.allFp)
	} else {
		numChunks = ci.getNumChunksPerPage()
	}

	ret := make([][12]byte, numChunks)
	copy(ret, ci.allFp[:ci.getNumChunksPerPage()])
	return ret
}

// returns a copy of the last chunk in a page. The page is the page the given chunk resides in.
func (ci *SortedChunkIndex) GetLastChunkFpOfPage(fp [12]byte) (lastFp [12]byte, pagenum uint32) {
	ci.chunkIndexRWLock.RLock()

	// get pos and pagenum of given chunk
	pos := uint32(sort.Search(len(ci.allFp), func(i int) bool { return bytes.Compare(ci.allFp[i][:], fp[:]) >= 0 }))
	pagenum = ci.getPageForPos(pos)

	// compute the position of the last fp in the same page
	pos = (pagenum+1)*uint32(ci.getNumChunksPerPage()) - 1
	if pos >= uint32(len(ci.allFp)) { // the computation above doesn't work for last page => quick&dirty fix
		pos = uint32(len(ci.allFp) - 1)
	}
	if len(ci.allFp) == 0 {
		lastFp = [12]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
	} else {
		lastFp = ci.allFp[pos]
	}

	ci.chunkIndexRWLock.RUnlock()
	return
}

func (ci *SortedChunkIndex) predictPageUtility(with [][12]byte) float32 {

	if with == nil {
		return 0
	}

	ci.chunkIndexRWLock.RLock()
	defer ci.chunkIndexRWLock.RUnlock()

	var firstPage [][12]byte
	if len(ci.allFp) == 0 {
		return 0
	} else if len(ci.allFp) > ci.getNumChunksPerPage() {
		firstPage = ci.allFp[:ci.getNumChunksPerPage()]
	} else {
		firstPage = ci.allFp
	}

	var hits int

	for i := range with {

		if n := bytes.Compare(with[i][:], firstPage[len(firstPage)-1][:]); n == 1 {
			// this chunk is out of the first page, Since we predict only base on the first page, we abort here.
			break
		}

		if _, ok := ci.firstPageIndex[with[i]]; ok {
			hits++
		}
	}

	return float32(hits) / float32(len(firstPage))
}

// Conputes the Jacard Distance of the first chunks in the index with given ones. It will only look at the chunks in the disk index and ignore the memoryIndex.
func (ci *SortedChunkIndex) predictPageHitrate(with [][12]byte) float32 {

	if with == nil {
		return 0
	}

	ci.chunkIndexRWLock.RLock()
	defer ci.chunkIndexRWLock.RUnlock()

	var firstPage [][12]byte
	if len(ci.allFp) == 0 {
		return 0
	} else if len(ci.allFp) > ci.getNumChunksPerPage() {
		firstPage = ci.allFp[:ci.getNumChunksPerPage()]
	} else {
		firstPage = ci.allFp
	}

	var hits int
	var misses int

	for i := range with {

		if n := bytes.Compare(with[i][:], firstPage[len(firstPage)-1][:]); n == 1 {
			// this chunk is out of the first page, Since we predict only base on the first page, we abort here.
			break
		}

		if _, ok := ci.firstPageIndex[with[i]]; ok {
			hits++
		} else {
			misses++
		}
	}

	if (hits + misses) > 0 {
		return float32(hits) / float32(hits+misses)
	} else {
		return 0
	}
}
