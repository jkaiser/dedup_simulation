package blc

import "testing"
import "container/list"
import "encoding/binary"
import "bytes"

var blockIndex *BlockIndex

func Init() {
	blockIndex = NewBlockIndex(262144, 32768, 8*1024, nil)
}

func TestBlockMappingSetArray(t *testing.T) {

	bm := BlockMappingSetArray{Set: make([][12]byte, 0, 32)}
	var fp [12]byte
	fp[0] = 1

	bm.Add(&fp)
	bm.Add(&fp)

	if bm.Size() != 1 { // 1 because it is a _set_
		t.Fatalf("wrong size: %v", bm.Size())
	}

	if !bm.Contains(&fp) {
		t.Fatalf("Didn't store fingerprint")
	}
}

func TestBlockMappingSetMap(t *testing.T) {

	bm := BlockMappingSetMap{Set: make(map[[12]byte]bool, 64)}
	var fp [12]byte
	fp[0] = 1

	bm.Add(&fp)
	bm.Add(&fp)

	if bm.Size() != 1 {
		t.Fatalf("wrong size: %v", bm.Size())
	}

	if !bm.Contains(&fp) {
		t.Fatalf("Didn't store fingerprint")
	}
}

func TestGetPageForBlock(t *testing.T) {
	Init()
	if blockIndex.getPageForBlock(0) != 0 {
		t.Fatalf("wrong page computed")
	}

	pageNum := blockIndex.getPageForBlock(0)
	for i := uint32(1E5); i < 1E7; i += 1E5 {
		tmpPageNum := blockIndex.getPageForBlock(i)
		if tmpPageNum == pageNum {
			t.Fatalf("wrong page computed. Page of %v must not be equal to page of 0", i)
		}
	}
}

func TestGet(t *testing.T) {
	Init()
	for i := 0; i < 1000; i++ {
		mapping := NewBlockMapping(32)
		for j := 0; j < 32; j++ {
			var fp [12]byte
			fp[j%12] = byte(i % 256)
			mapping.Add(&fp)
		}

		blockIndex.Add(uint32(i), mapping)
	}

	// test
	for i := 0; i < 1000; i++ {
		bm, ok := blockIndex.Get(uint32(i), false, new(BlockIndexStatistics), "test")
		if !ok {
			t.Fatalf("mapping %v does not exist in index", i)
		}

		for j := 0; j < 32; j++ {
			var fp [12]byte
			fp[j%12] = byte(i % 256)
			if !bm.Contains(&fp) {
				t.Fatalf("mapping %v does not have fingerprint", i)
			}
		}
	}
}

func TestFetchFullPage(t *testing.T) {

	Init()

	buffer := new(bytes.Buffer)
	for i := 0; i < 1000; i++ {
		mapping := NewBlockMapping(32)
		for j := 0; j < 32; j++ {
			var fp [12]byte
			if err := binary.Write(buffer, binary.LittleEndian, int64(j)); err != nil {
				t.Fatalf("writing binary representaion failed: %v", err)
			}
			copy(fp[:], buffer.Bytes())
			mapping.Add(&fp)
			buffer.Reset()
		}

		blockIndex.Add(uint32(i), mapping)
	}

	// test whether the blocks only appear in one page and have the correct size
	blockMappingMap := make(map[uint32]uint32, 1000)
	for i := uint32(0); i < 1000; i++ {
		list := list.New()
		blockIndex.fetchFullPage(i, true, list, new(BlockIndexStatistics), "")

		for e := list.Front(); e != nil; e = e.Next() {
			mappingPair := e.Value.(*BlockIdBlockMappingPair)

			if mappingPair.mapping.Size() != 32 {
				t.Fatalf("mapping %v has wrong size; got %v, expected %v", mappingPair.blockId, mappingPair.mapping.Size, 32)
			}

			page := blockIndex.getPageForBlock(i)
			oldPage, ok := blockMappingMap[i]
			if ok && (oldPage != page) {
				t.Fatalf("Found same block in two pages: block %v, page1: %v, page2: %v", i, oldPage, page)
			}
			if !ok {
				blockMappingMap[i] = page
			}
		}
	}
}

func benchmarkFetchFullPage(numFetches int, b *testing.B) {

	b.StopTimer()
	b.ReportAllocs()
	Init()
	for i := 0; i < 1E5; i++ {
		mapping := NewBlockMapping(32)
		for j := 0; j < 32; j++ {
			var fp [12]byte
			fp[j%12] = byte(i % 256)
			mapping.Add(&fp)
		}

		blockIndex.Add(uint32(i), mapping)
	}
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		for i := 0; i < numFetches; i++ {
			blockIndex.fetchFullPage(uint32(i), true, nil, new(BlockIndexStatistics), "")
		}
	}
}

func BenchmarkFetchFullPage10(b *testing.B)  { benchmarkFetchFullPage(10, b) }
func BenchmarkFetchFullPage100(b *testing.B) { benchmarkFetchFullPage(100, b) }
func BenchmarkFetchFullPage1K(b *testing.B)  { benchmarkFetchFullPage(1E3, b) }
func BenchmarkFetchFullPage10K(b *testing.B) { benchmarkFetchFullPage(1E4, b) }
