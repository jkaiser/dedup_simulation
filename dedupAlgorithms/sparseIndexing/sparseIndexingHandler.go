package sparseIndexing

import "os"

import "bytes"
import "encoding/binary"
import "sync"
import "encoding/json"
import "hash"
import "hash/fnv"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type SparseIndexingConfig struct {
	DataSet                  string
	SegmentSize              int
	CacheSize                int
	SampleFactor             int
	MaxSegmentCount          int
	MaxHooksPerSegment       int
	EndSegmentPerFile        bool
	ZeroHookHandlingLimit    int
	CountChunkIndex          bool
	ChunkIndexPageSize       int
	NegativeLookupProbabiliy float32
	FixedSegmentation        bool
}

type SparseIndexingStatistics struct {
	FileNumber int

	ChunkCount                int
	UniqueChunkCount          int
	RedundantChunkCount       int
	DetectedDuplicatesCount   int
	UndetectedDuplicatesCount int
	DetectedUniqueChunks      int // the number of detected unique chunks (because they are hooks)

	ZeroHookCount int
	ZeroHookItems int

	SegmentCount              int
	SegmentTotalSize          int
	AvgSegmentSize            float32
	AvgHooksPerSegment        float32
	AvgNumChampionsPerSegment float32

	ChunkHits               int
	ChunkMisses             int
	ChunkMissesWithSegments int
	HitRatio                float32

	UniqueChunkRatio                         float32
	RedundantChunkRatio                      float32
	UndetectedDuplicatesRatio                float32 // the number of stored duplicates compared to the total number of chunks
	UndetectedDuplicatesToAllDuplicatesRatio float32 // the number of undetected duplicates compare to the number of duplicates

	SparseIndexStats SparseIndexStatistics
	ChunkIndexStats  common.ChunkIndexStatistics

	StorageCount     int64
	StorageByteCount int64
}

func NewSparseIndexingStatistics() SparseIndexingStatistics {
	var s SparseIndexingStatistics
	s.SparseIndexStats = NewSparseIndexStatistics()
	return s
}
func (s *SparseIndexingStatistics) add(that SparseIndexingStatistics) {
	s.ChunkCount += that.ChunkCount
	s.UniqueChunkCount += that.UniqueChunkCount
	s.RedundantChunkCount += that.RedundantChunkCount
	s.DetectedDuplicatesCount += that.DetectedDuplicatesCount
	s.UndetectedDuplicatesCount += that.UndetectedDuplicatesCount
	s.DetectedUniqueChunks += that.DetectedUniqueChunks

	s.ZeroHookCount += that.ZeroHookCount
	s.ZeroHookItems += that.ZeroHookItems

	s.SegmentCount += that.SegmentCount
	s.SegmentTotalSize += that.SegmentTotalSize

	s.ChunkHits += that.ChunkHits
	s.ChunkMisses += that.ChunkMisses
	s.ChunkMissesWithSegments += that.ChunkMissesWithSegments

	s.SparseIndexStats.add(that.SparseIndexStats)
	s.ChunkIndexStats.Add(that.ChunkIndexStats)

	s.StorageCount += that.StorageCount
	s.StorageByteCount += that.StorageByteCount
}

// computes final statistics
func (s *SparseIndexingStatistics) finish() {

	s.StorageCount = s.SparseIndexStats.StorageCount + s.ChunkIndexStats.StorageCount
	s.StorageByteCount = s.SparseIndexStats.StorageByteCount + s.ChunkIndexStats.StorageByteCount

	if s.SegmentCount > 0 {
		s.AvgSegmentSize = float32(float64(s.SegmentTotalSize) / float64(s.SegmentCount))
	} else {
		s.AvgSegmentSize = 0.0
	}

	if s.ChunkCount > 0 {
		s.UniqueChunkRatio = float32(float64(s.UniqueChunkCount) / float64(s.ChunkCount))
		s.RedundantChunkRatio = float32(float64(s.RedundantChunkCount) / float64(s.ChunkCount))
		s.UndetectedDuplicatesRatio = float32(float64(s.UndetectedDuplicatesCount) / float64(s.ChunkCount))
		s.HitRatio = float32(float64(s.ChunkHits) / float64(s.ChunkHits+s.ChunkMisses))
	} else {
		s.UniqueChunkRatio = 0.0
		s.RedundantChunkRatio = 0.0
		s.UndetectedDuplicatesRatio = 0.0
		s.HitRatio = 0.0
	}

	if s.RedundantChunkCount > 0 {
		s.UndetectedDuplicatesToAllDuplicatesRatio = float32(float64(s.UndetectedDuplicatesCount) / float64(s.RedundantChunkCount))
	} else {
		s.UndetectedDuplicatesToAllDuplicatesRatio = 0.0
	}
}

// The handler for SparseIndexing
type SparseIndexing struct {
	sparseIndex       *SparseIndex
	chunkIndexRWLocks []sync.RWMutex
	chunkIndices      []map[[12]byte]struct{}

	Statistics           SparseIndexingStatistics
	chunkIndexStatistics common.ChunkIndexStatistics
	config               SparseIndexingConfig

	chunkHashBuf             [12]byte // buffer for all chunk hashes. Used to make the Digest useable in maps.
	traceFileCount           int
	currentSegment           [][12]byte
	championList             []*Segment
	currentSegmentIndex      int
	currentSegmentSize       uint32
	segmentBreakmark         int
	averageChunkSize         int
	hashBuf                  bytes.Buffer
	expectedChunksPerSegment uint32

	numHooks      int
	numChampions  int64
	segmentHasher hash.Hash

	outputFilename string
	streamId       string
	StatsList      []SparseIndexingStatistics
}

func NewSparseIndexing(config SparseIndexingConfig,
	avgChunkSize int,
	chunkIndices []map[[12]byte]struct{},
	chunkIndexLocks []sync.RWMutex,
	sIndex *SparseIndex,
	outFilename string) *SparseIndexing {

	si := &SparseIndexing{
		sparseIndex:       sIndex,
		chunkIndices:      chunkIndices,
		chunkIndexRWLocks: chunkIndexLocks,
		averageChunkSize:  avgChunkSize,

		config:         config,
		outputFilename: outFilename,
		segmentHasher:  fnv.New32(),
		currentSegment: make([][12]byte, 0, config.SegmentSize/avgChunkSize*2),
		championList:   make([]*Segment, 0),
	}
	si.Statistics = NewSparseIndexingStatistics()
	si.setSegmentBreakmark()
	return si
}

func (si *SparseIndexing) BeginTraceFile(number int, name string) {
	log.Info("begin Tracefile ", number, ": ", name)
	si.streamId = name
	si.Statistics.FileNumber = number
}

func (si *SparseIndexing) EndTraceFile() {
	log.Info("end Tracefile ")

	// finish open segments
	si.finishSegment()
	// collect statistics
	si.sparseIndex.finish(&si.Statistics.SparseIndexStats)
	si.chunkIndexStatistics.ItemCount = 0
	for _, cindex := range si.chunkIndices {
		si.chunkIndexStatistics.ItemCount += int64(len(cindex))
	}
	si.chunkIndexStatistics.Finish()
	si.Statistics.ChunkIndexStats = si.chunkIndexStatistics
	si.Statistics.SparseIndexStats.finish()
	if si.Statistics.SegmentCount > 0 {
		si.Statistics.AvgHooksPerSegment = float32(float64(si.numHooks) / float64(si.Statistics.SegmentCount))
		si.Statistics.AvgNumChampionsPerSegment = float32(float64(si.numChampions) / float64(si.Statistics.SegmentCount))
	} else {
		si.Statistics.AvgHooksPerSegment = 0.0
		si.Statistics.AvgNumChampionsPerSegment = 0.0
	}
	si.Statistics.finish()

	si.StatsList = append(si.StatsList, si.Statistics)

	if encodedStats, err := json.MarshalIndent(si.Statistics, "", "    "); err != nil {
		log.Errorf("Couldn't marshal statistics: %v\nStatistic struct is: %v", err, si.Statistics)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
	}

	// cleanup
	si.numHooks = 0
	si.numChampions = 0
	si.chunkIndexStatistics.Reset()
	si.Statistics = NewSparseIndexingStatistics()
}

func (si *SparseIndexing) Quit() {
	if si.outputFilename == "" {
		return
	}

	if encodedStats, err := json.MarshalIndent(si.StatsList, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(si.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}

func (si *SparseIndexing) HandleFiles(fileEntries <-chan *common.FileEntry, fileEntryReturn chan<- *common.FileEntry) {

	for fileEntry := range fileEntries {
		if len(fileEntry.Chunks) > 0 {
			si.handleChunks(fileEntry)
		}
		fileEntryReturn <- fileEntry
	}
}

func (si *SparseIndexing) handleChunks(fileEntry *common.FileEntry) {
	for i := range fileEntry.Chunks {
		si.Statistics.ChunkCount++
		copy(si.chunkHashBuf[:], fileEntry.Chunks[i].Digest)

		if si.config.FixedSegmentation {
			if si.currentSegmentSize+fileEntry.Chunks[i].Size >= uint32(si.config.SegmentSize) {
				si.finishSegment()
			}
			if si.currentSegmentIndex == len(si.currentSegment) {
				si.currentSegment = append(si.currentSegment, si.chunkHashBuf)
				si.currentSegmentIndex++
			} else {
				si.currentSegment[si.currentSegmentIndex] = si.chunkHashBuf
				si.currentSegmentIndex++
			}
			si.currentSegmentSize += fileEntry.Chunks[i].Size
		} else if si.isSegmentLandmark(&si.chunkHashBuf) {

			if si.currentSegmentIndex == len(si.currentSegment) {
				si.currentSegment = append(si.currentSegment, si.chunkHashBuf)
				si.currentSegmentIndex++
			} else {
				si.currentSegment[si.currentSegmentIndex] = si.chunkHashBuf
				si.currentSegmentIndex++
			}

			si.currentSegmentSize += fileEntry.Chunks[i].Size
			si.finishSegment()
		} else {
			if si.currentSegmentIndex == len(si.currentSegment) {
				si.currentSegment = append(si.currentSegment, si.chunkHashBuf)
				si.currentSegmentIndex++
			} else {
				si.currentSegment[si.currentSegmentIndex] = si.chunkHashBuf
				si.currentSegmentIndex++
			}

			si.currentSegmentSize += fileEntry.Chunks[i].Size
		}
	}
}

func (si *SparseIndexing) setSegmentBreakmark() {

	si.expectedChunksPerSegment = uint32(si.config.SegmentSize / si.averageChunkSize)
	//bitlen := float64(big.NewInt(int64(si.config.SegmentSize) / 8192).BitLen())
	//si.segmentBreakmark = int(math.Pow(2, bitlen-1) - 1)
}

func (si *SparseIndexing) isSegmentLandmark(fp *[12]byte) bool {

	if si.config.FixedSegmentation {
		return false
	} else {
		var truncatedHash uint32
		si.hashBuf.Reset()
		si.hashBuf.Write(fp[:])
		if err := binary.Read(&si.hashBuf, binary.LittleEndian, &truncatedHash); err != nil {
			log.Errorf("Couldn't decode int for landmark detection: %v", err)
			return false
		}

		return (truncatedHash % si.expectedChunksPerSegment) == 0

		/* Some number of different hash methods
		   adler: avgSegmentSize : 92MB
		   fnv avgSegmentSize : 42MB
		   fingperprint itself : 39MB
		   murmur32 : 39MB
		*/

		//si.segmentHasher.Reset()
		//si.segmentHasher.Write(fp[:])
		//si.bigInt.SetBytes(si.segmentHasher.Sum(nil))

		//return (int(si.bigInt.Int64()) & si.segmentBreakmark) == si.segmentBreakmark
	}
}

func (si *SparseIndexing) finishSegment() {

	var segmentHitCount int
	var segmentMissCount int
	var segmentFalseMissCount int

	if len(si.currentSegment) == 0 {
		return
	}

	hooks := si.sparseIndex.getHooks(si.currentSegment, si.currentSegmentIndex)
	si.championList = si.sparseIndex.getSegments(hooks, si.config.MaxSegmentCount, true, si.championList[:0], &si.Statistics.SparseIndexStats, si.streamId)
	si.numChampions += int64(len(si.championList))

	si.Statistics.SegmentCount++
	/*si.Statistics.SegmentTotalSize += len(si.currentSegment)*/
	si.Statistics.SegmentTotalSize += si.currentSegmentIndex

	for i := 0; i < si.currentSegmentIndex; i++ {

		// check si.championList
		found := false
		for _, champion := range si.championList {
			if _, ok := champion.Chunks[si.currentSegment[i]]; ok {
				found = true
				break
			}
		}

		// do statistics
		if found {
			si.Statistics.ChunkHits++
			si.Statistics.RedundantChunkCount++
			si.Statistics.DetectedDuplicatesCount++

			segmentHitCount++

		} else {
			if len(si.championList) != 0 {
				si.Statistics.ChunkMissesWithSegments++
			}
			si.Statistics.ChunkMisses++
			segmentMissCount++

			si.chunkIndexStatistics.LookupCount++
			indexPos := si.currentSegment[i][0]
			si.chunkIndexRWLocks[indexPos].RLock()
			_, chunkExists := si.chunkIndices[indexPos][si.currentSegment[i]]
			si.chunkIndexRWLocks[indexPos].RUnlock()
			if chunkExists {
				si.Statistics.RedundantChunkCount++
				si.Statistics.UndetectedDuplicatesCount++

				segmentFalseMissCount++
				if si.config.CountChunkIndex { // each lookup generates an IO if counted
					si.chunkIndexStatistics.StorageCount++
					si.chunkIndexStatistics.StorageCountDuplicates++
					si.chunkIndexStatistics.StorageByteCount += int64(si.config.ChunkIndexPageSize)
				}

			} else {
				si.Statistics.UniqueChunkCount++

				if si.config.CountChunkIndex { // each lookup generates an IO if counted
					si.chunkIndexStatistics.StorageCount++
					si.chunkIndexStatistics.StorageCountUnique++
					si.chunkIndexStatistics.StorageByteCount += int64(si.config.ChunkIndexPageSize)
				}

				// update chunk index. This is considered free since it can happen out of the
				// critical path in an efficient manner
				si.chunkIndexStatistics.UpdateCount++
				si.chunkIndexRWLocks[indexPos].Lock()
				si.chunkIndices[indexPos][si.currentSegment[i]] = struct{}{}
				si.chunkIndexRWLocks[indexPos].Unlock()
			}
		}
	}

	si.sparseIndex.unlockSegments(si.championList)

	// end segment
	if hooks.Size() == 0 {
		si.Statistics.ZeroHookCount++
		/*si.Statistics.ZeroHookItems += len(si.currentSegment)*/
		si.Statistics.ZeroHookItems += si.currentSegmentIndex
	} else { // if there are no hooks, the segment has no further effect
		si.sparseIndex.addSegment(si.currentSegment, si.currentSegmentIndex, &si.Statistics.SparseIndexStats)
		si.numHooks += hooks.Size()
	}
	si.currentSegment = si.currentSegment[:0]
	si.currentSegmentIndex = 0
	si.currentSegmentSize = 0
}
