package common

import "testing"

var chunkIndex *ChunkIndex

func InitChunkIndex() {
	chunkIndex = NewChunkIndex(16*1024*1024, 1.0, 2048)
}

func TestChunkIndexAddGet(t *testing.T) {
	InitChunkIndex()

	var fp [12]byte
	cm := &ChunkMapping{17, 42}
	chunkIndex.Add(&fp, cm)

	chunkMapping, ok := chunkIndex.Get(&fp, true)
	if !ok {
		t.Fatalf("index didn't store fp %x", fp)
	}
	if (chunkMapping.ContainerId != 17) || (chunkMapping.LastBlockId != 42) {
		t.Fatalf("index returned wrong chunkMapping. got: %v, want: %v", chunkMapping, cm)
	}

	fp[0] = byte(1)
	chunkMapping, ok = chunkIndex.Get(&fp, true)
	if ok {
		t.Fatalf("index returned not stored fp %x", fp)
	}

	if chunkIndex.Statistics.StorageCount == 0 {
		t.Fatalf("index didn't count IO")
	}

}
