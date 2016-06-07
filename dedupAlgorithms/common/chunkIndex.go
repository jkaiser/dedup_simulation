package common

import "time"
import "math/rand"

type ChunkMapping struct {
	ContainerId int
	LastBlockId int
}

type ChunkIndexStatistics struct {
	ItemCount              int64
	LookupCount            int
	UpdateCount            int
	StorageCount           int64
	StorageCountDuplicates int64
	StorageCountUnique     int64
	StorageByteCount       int64
}

func (cs *ChunkIndexStatistics) Add(that ChunkIndexStatistics) {
	cs.ItemCount += that.ItemCount
	cs.LookupCount += that.LookupCount
	cs.UpdateCount += that.UpdateCount
	cs.StorageCount += that.StorageCount
	cs.StorageCountDuplicates += that.StorageCountDuplicates
	cs.StorageCountUnique += that.StorageCountUnique
	cs.StorageByteCount += that.StorageByteCount
}
func (cs *ChunkIndexStatistics) Finish() {}
func (cs *ChunkIndexStatistics) Reset() {
	cs.ItemCount = 0
	cs.LookupCount = 0
	cs.UpdateCount = 0
	cs.StorageCount = 0
	cs.StorageCountDuplicates = 0
	cs.StorageCountUnique = 0
	cs.StorageByteCount = 0
}

type ChunkIndex struct {
	Index                     map[[12]byte]*ChunkMapping
	Statistics                *ChunkIndexStatistics
	negativeLookupProbability float32
	pageSize                  int
	randGenerator             *rand.Rand
	indexPageCount            int
}

func NewChunkIndex(indexPageCount int, negLookupProb float32, chunkIndexPageSize int) *ChunkIndex {
	return &ChunkIndex{
		Index:                     make(map[[12]byte]*ChunkMapping, 1E7),
		Statistics:                new(ChunkIndexStatistics),
		negativeLookupProbability: negLookupProb,
		pageSize:                  chunkIndexPageSize,
		indexPageCount:            indexPageCount,
		randGenerator:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (ci *ChunkIndex) Add(fp *[12]byte, cm *ChunkMapping) {
	ci.Statistics.UpdateCount++
	ci.Index[*fp] = cm
}

func (ci *ChunkIndex) Get(fp *[12]byte, countIO bool) (*ChunkMapping, bool) {
	chunkMapping, ok := ci.Index[*fp]

	if countIO {
		if !ok && (ci.randGenerator.Float32() < ci.negativeLookupProbability) {
			ci.Statistics.StorageCount++
			ci.Statistics.StorageByteCount += int64(ci.pageSize)
		}
		ci.Statistics.LookupCount++
	}

	return chunkMapping, ok
}

func (ci *ChunkIndex) Finish() {
	ci.Statistics.ItemCount = int64(len(ci.Index))
}
