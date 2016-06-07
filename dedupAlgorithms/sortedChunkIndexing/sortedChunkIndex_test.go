package sortedChunkIndexing

import "testing"
import "fmt"
import "bytes"
import "encoding/binary"

var sortedCI *SortedChunkIndex

func InitSortedChunkIndex() {
	sortedCI = NewSortedChunkIndex(1024, 1024*1024, nil)
}

func TestSortedChunkIndexGetSetStateFlush(t *testing.T) {
	sortedCI = NewSortedChunkIndex(48, 1024*1024, nil)
	var fp [12]byte

	sortedCI.Add(&fp, nil)
	fp[0] = 3
	sortedCI.Add(&fp, nil)

	// now with a flush
	sortedCI.Flush(nil)
	fp[0] = 7
	sortedCI.Add(&fp, nil)
	s := sortedCI.GetState()
	if len(s.memList) != 1 {
		t.Fatal(len(s.memList))
	} else if len(s.memIndex) != 1 {
		t.Fatal(len(s.memIndex))
	} else if len(s.allFp) != 2 {
		t.Fatal(len(s.allFp))
	}

	sortedCI.SetState(s)
	if len(sortedCI.memList) != 1 {
		t.Fatal(len(sortedCI.memList))
	} else if len(sortedCI.memIndex) != 1 {
		t.Fatal(len(sortedCI.memIndex))
	} else if len(sortedCI.diskIndex) != 2 {
		t.Fatal(len(sortedCI.diskIndex))
	} else if len(sortedCI.allFp) != 2 {
		t.Fatal(len(sortedCI.allFp))
	}
}

func TestSortedChunkIndexGetSetState(t *testing.T) {
	sortedCI = NewSortedChunkIndex(48, 1024*1024, nil)
	var fp [12]byte
	sortedCI.Add(&fp, nil)

	if len(sortedCI.memList) != 1 {
		t.Fatal(len(sortedCI.memList))
	}

	sortedCI.Flush(nil)
	s := sortedCI.GetState()
	if len(s.memList) != 0 {
		t.Fatal(len(s.memList))
	} else if len(s.memIndex) != 0 {
		t.Fatal(len(s.memIndex))
	} else if len(s.allFp) != 1 {
		t.Fatal(len(s.allFp))
	}

	fp[0] = 42
	if !sortedCI.Add(&fp, nil) {
		t.Fatal("couldn't add new chunk")
	}

	if !sortedCI.CheckWithoutChange(&fp) {
		t.Fatalf("couldn't save fingerprint %x", fp)
	}
	sortedCI.SetState(s)
	if sortedCI.CheckWithoutChange(&fp) {
		t.Fatalf("Still have fingerprint %x after setState", fp)
	}
	if len(sortedCI.memList) != 0 {
		t.Fatal(len(sortedCI.memList))
	} else if len(sortedCI.memIndex) != 0 {
		t.Fatal(len(sortedCI.memIndex))
	} else if len(s.allFp) != 1 {
		t.Fatal(len(s.allFp))
	}

	fp[0] = 0
	if !sortedCI.CheckWithoutChange(&fp) {
		t.Fatalf("Forgot fingerprint %x after setState", fp)
	}

	fp[0] = 17
	sortedCI.Add(&fp, nil)
	if !sortedCI.CheckWithoutChange(&fp) {
		t.Fatalf("couldn't save fingerprint %x", fp)
	}

	sortedCI.SetState(s)
	if sortedCI.CheckWithoutChange(&fp) {
		t.Fatalf("Still have fingerprint %x after setState", fp)
	}

}

func TestSortedChunkIndexGetNumPages(t *testing.T) {

	sortedCI = NewSortedChunkIndex(48, 1024*1024, nil)

	var fp [12]byte

	// fil CI
	for i := 0; i < 100; i++ {
		fp[0] = byte(i)
		sortedCI.Add(&fp, nil)
	}

	sortedCI.Flush(nil)

	sortedCI.SetPageSize(ciEntrySize)
	if np := sortedCI.getNumPages(); np != 100 {
		t.Fatalf("expected: %v, got: %v", 100, np)
	}

	sortedCI.SetPageSize(ciEntrySize * 2)
	if np := sortedCI.getNumPages(); np != 50 {
		t.Fatalf("expected: %v, got: %v", 50, np)
	}

	sortedCI.SetPageSize(ciEntrySize*2 + 1)
	if np := sortedCI.getNumPages(); np != 50 {
		t.Fatalf("expected: %v, got: %v", 50, np)
	}

	sortedCI.SetPageSize(ciEntrySize * 1000)
	if np := sortedCI.getNumPages(); np != 1 {
		t.Fatalf("expected: %v, got: %v", 1, np)
	}
}

func TestSortedChunkIndexGetLastChunkFpOfPage(t *testing.T) {

	sortedCI = NewSortedChunkIndex(48, 1024*1024, nil)

	var fp [12]byte
	var expFP [12]byte

	// fil CI
	for i := 0; i < 100; i++ {
		fp[0] = byte(i)
		sortedCI.Add(&fp, nil)
	}

	sortedCI.Flush(nil)

	sortedCI.SetPageSize(2 * ciEntrySize)
	fp[0] = 0
	expFP[0] = 1
	if lastFP, pnum := sortedCI.GetLastChunkFpOfPage(fp); pnum != 0 {
		t.Fatal(pnum)
	} else if !bytes.Equal(expFP[:], lastFP[:]) {
		t.Fatalf("expected: %x, got: %x", expFP, lastFP)
	}

	fp[0] = 3
	expFP = fp
	if lastFP, pnum := sortedCI.GetLastChunkFpOfPage(fp); pnum != 1 {
		t.Fatal(pnum)
	} else if !bytes.Equal(expFP[:], lastFP[:]) {
		t.Fatalf("expected: %x, got: %x", expFP, lastFP)
	}

	sortedCI.SetPageSize(2*ciEntrySize + ciEntrySize/2)
	fp[0] = 0
	expFP[0] = 1
	if lastFP, pnum := sortedCI.GetLastChunkFpOfPage(fp); pnum != 0 {
		t.Fatal(pnum)
	} else if !bytes.Equal(expFP[:], lastFP[:]) {
		t.Fatalf("expected: %x, got: %x", expFP, lastFP)
	}

	fp[0] = 3
	expFP = fp
	if lastFP, pnum := sortedCI.GetLastChunkFpOfPage(fp); pnum != 1 {
		t.Fatal(pnum)
	} else if !bytes.Equal(expFP[:], lastFP[:]) {
		t.Fatalf("expected: %x, got: %x", expFP, lastFP)
	}
	fp[0] = 4
	expFP[0] = 5
	if lastFP, pnum := sortedCI.GetLastChunkFpOfPage(fp); pnum != 2 {
		t.Fatal(pnum)
	} else if !bytes.Equal(expFP[:], lastFP[:]) {
		t.Fatalf("expected: %x, got: %x", expFP, lastFP)
	}

}

func TestSortedChunkIndexGetPage(t *testing.T) {
	sortedCI = NewSortedChunkIndex(48, 1024*1024, nil)

	if sortedCI.getPageForPos(0) != 0 {
		t.Fatalf("wrong position, got %v, expected 0", sortedCI.getPageForPos(0))
	}
	if sortedCI.getPageForPos(2) != 1 {
		t.Fatalf("wrong position, got %v, expected 1", sortedCI.getPageForPos(2))
	}

	if sortedCI.getPageForPos(1) != sortedCI.getPageForPos(1) {
		t.Fatalf("position mismatch")
	}

	if sortedCI.getPageForPos(0) != sortedCI.getPageForPos(1) {
		t.Fatalf("position mismatch. Expected in both: %v, but got %v", sortedCI.getPageForPos(0), sortedCI.getPageForPos(1))
	}

	if sortedCI.getPageForPos(0) == sortedCI.getPageForPos(2) {
		t.Fatalf("position match where it shouldn't. Expected in %v and %v, but got the same", sortedCI.getPageForPos(0), sortedCI.getPageForPos(0)+1)
	}
}

func TestSortedChunkIndexAddCheck(t *testing.T) {

	InitSortedChunkIndex()

	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.LittleEndian, int64(42)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}

	var fp [12]byte
	copy(fp[:], buffer.Bytes())

	sortedCI.Add(&fp, nil)

	if len(sortedCI.diskIndex) > 0 {
		t.Fatalf("disk index was touched!")
	} else if len(sortedCI.memIndex) != 1 {
		t.Fatalf("unexpected mem index size. expected %v, got %v", 1, len(sortedCI.memIndex))
	} else if _, ok, _ := sortedCI.Check(&fp, false, 0, nil, ""); !ok {
		t.Fatalf("index doesn't contain just inserted fp %x ", fp)
	}

	// two time the same should not change anything
	sortedCI.Add(&fp, nil)

	if len(sortedCI.diskIndex) > 0 {
		t.Fatalf("disk index was touched!")
	} else if len(sortedCI.memIndex) != 1 {
		t.Fatalf("unexpected mem index size. expected %v, got %v", 1, len(sortedCI.memIndex))
	} else if _, ok, _ := sortedCI.Check(&fp, false, 0, nil, ""); !ok {
		t.Fatalf("index doesn't contain just inserted fp %x ", fp)
	}

	buffer.Reset()
	InitSortedChunkIndex()

	for i := 0; i < 100; i++ {
		if err := binary.Write(buffer, binary.LittleEndian, int64(i)); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}
		copy(fp[:], buffer.Bytes())
		sortedCI.Add(&fp, nil)
		buffer.Reset()
	}

	for i := 0; i < 100; i++ {
		if err := binary.Write(buffer, binary.LittleEndian, int64(i)); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}
		copy(fp[:], buffer.Bytes())
		if _, ok, _ := sortedCI.Check(&fp, false, 0, nil, ""); !ok {
			t.Fatalf("index doesn't contain inserted fp %x ", fp)
		}
		buffer.Reset()
	}
}

func TestSortedChunkIndexSingleFlush(t *testing.T) {

	InitSortedChunkIndex()

	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.LittleEndian, int64(42)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}

	var fp [12]byte
	copy(fp[:], buffer.Bytes())

	sortedCI.Add(&fp, nil)

	sortedCI.Flush(nil)
	if len(sortedCI.diskIndex) != 1 {
		t.Fatalf("unexpected disk index size. expected %v, got %v", 1, len(sortedCI.diskIndex))
	} else if len(sortedCI.memIndex) != 0 {
		t.Fatalf("unexpected mem index size. expected %v, got %v", 0, len(sortedCI.memIndex))
	} else if _, ok, _ := sortedCI.Check(&fp, false, 0, nil, ""); !ok {
		t.Fatalf("index doesn't contain just inserted fp %x ", fp)
	}

	if pos, ok := sortedCI.diskIndex[fp]; !ok {
		t.Fatalf("index doesn't hold position for fp")
	} else if pos != 0 {
		t.Fatalf("index holds wrong position for fp. expected: 0, got %v ", pos)
	}
}

func TestSortedChunkIndexManyFlushes(t *testing.T) {

	sortedCI = NewSortedChunkIndex(1024, 24, nil)

	var fp [12]byte
	buffer := new(bytes.Buffer)
	const numChunks = 100
	for i := 0; i < numChunks; i++ {
		if err := binary.Write(buffer, binary.LittleEndian, int64(i)); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}
		copy(fp[:], buffer.Bytes())
		sortedCI.Add(&fp, nil)
		t.Log("added %x", fp)
		buffer.Reset()
	}

	for i := 0; i < numChunks; i++ {
		if err := binary.Write(buffer, binary.LittleEndian, int64(i)); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}
		copy(fp[:], buffer.Bytes())
		if _, ok, _ := sortedCI.Check(&fp, false, 0, nil, ""); !ok {
			t.Fatalf("index doesn't contain inserted fp %x ", fp)
		}
		buffer.Reset()
	}

	sortedCI.Flush(nil)
	if pos := sortedCI.diskIndex[fp]; pos != sortedCI.getPageForPos(numChunks-1) {
		t.Fatalf("index holds wrong position for fp %x. expected: %v, got %v ", fp, sortedCI.getPageForPos(numChunks-1), pos)
	}
}

func TestSortedChunkIndexTraceGeneration(t *testing.T) {

	streamId := "fooId"
	iochan := make(chan string, 10)
	sortedCI = NewSortedChunkIndex(24, 24, iochan)

	var fp [12]byte
	buffer := new(bytes.Buffer)
	for i := 0; i < 5; i++ {
		if i == 2 {
			continue
		}

		if err := binary.Write(buffer, binary.LittleEndian, int64(i)); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}
		copy(fp[:], buffer.Bytes())
		sortedCI.Add(&fp, nil)
		t.Log("added %x", fp)
		buffer.Reset()
	}

	// check for fp 2
	if err := binary.Write(buffer, binary.LittleEndian, int64(2)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}
	copy(fp[:], buffer.Bytes())

	if _, ok, _ := sortedCI.Check(&fp, false, 0, nil, streamId); ok {
		t.Fatalf("found unadded fp: %x", fp)
	}

	close(iochan)
	if s, ok := <-iochan; !ok {
		t.Fatalf("new fp didn't cause IO: %x", fp)
	} else if s != fmt.Sprintf("%v\t%v\n", streamId, 2) {
		t.Fatalf("new fp check caused wrong IO. expected %v, got '%v'", fmt.Sprintf("%v\t%v\n", streamId, 2), s)
	}
}

func benchmarkSortedChunkIndexFlush(numFlushes int, b *testing.B) {
	b.StopTimer()
	b.ReportAllocs()

	sortedCI = NewSortedChunkIndex(1024, 24*100, nil)

	var fp [12]byte
	buffer := new(bytes.Buffer)

	prefilled := 1000
	for i := 0; i < prefilled; i++ {
		if err := binary.Write(buffer, binary.LittleEndian, int64(i)); err != nil {
			b.Fatalf("writing binary representaion failed: %v", err)
		}
		copy(fp[:], buffer.Bytes())
		sortedCI.Add(&fp, nil)
		buffer.Reset()
	}
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		for i := prefilled; i < prefilled+numFlushes; i++ {

			if err := binary.Write(buffer, binary.LittleEndian, int64(i)); err != nil {
				b.Fatalf("writing binary representaion failed: %v", err)
			}
			copy(fp[:], buffer.Bytes())
			sortedCI.Add(&fp, nil)
			buffer.Reset()
		}
	}
}

func BenchmarkSortedCIFlush10(b *testing.B)    { benchmarkSortedChunkIndexFlush(1e1, b) }
func BenchmarkSortedCIFlush100(b *testing.B)   { benchmarkSortedChunkIndexFlush(1e2, b) }
func BenchmarkSortedCIFlush1000(b *testing.B)  { benchmarkSortedChunkIndexFlush(1e3, b) }
func BenchmarkSortedCIFlush10000(b *testing.B) { benchmarkSortedChunkIndexFlush(1e4, b) }
