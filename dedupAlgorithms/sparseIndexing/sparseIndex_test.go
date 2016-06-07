package sparseIndexing

import "testing"

import "fmt"
import "bytes"
import "encoding/binary"
import log "github.com/cihub/seelog"

var sparseIndex *SparseIndex

func InitSparseIndex() *SparseIndex {
	sparseIndex := NewSparseIndex(16*1024*1024,
		8*1024,
		16,           // cacheSize
		5,            // sampleFactor
		20*1024*1024, // segmentSize
		5,            //maxHooksPerSegment
		nil)

	testConfig := `
<seelog type="sync">
    <outputs formatid="main">
        <filter levels="info">
            <console/>
        </filter>
    </outputs>
    <formats>
        <format id="main" format="%Date %Time [%Level] %Msg%n"/>
    </formats>
</seelog>`

	if logger, err := log.LoggerFromConfigAsBytes([]byte(testConfig)); err != nil {
		fmt.Println(err)
	} else {
		if loggerErr := log.ReplaceLogger(logger); loggerErr != nil {
			fmt.Println(loggerErr)
		}
	}

	return sparseIndex
}

func TestArraySet(t *testing.T) {
	set := NewArraySet()

	var fp [12]byte
	fp[0] = byte(1)

	set.Add(&fp)
	if set.Size() != 1 {
		t.Fatalf("Wrong set size")
	}

	if !set.Contains(&fp) {
		t.Fatalf("Could not detect written fp")
	}

	set.Remove(&fp)

	if set.Size() != 0 {
		t.Fatalf("Wrong set size after delete, expected %v, got %v", 0, set.Size())
	}

	if set.Contains(&fp) {
		t.Fatalf("Detected deleted fp")
	}
}

func TestAddSegment(t *testing.T) {

	si := InitSparseIndex()
	numChunks := 1024
	segment := make([][12]byte, numChunks)

	buffer := new(bytes.Buffer)
	for i := uint64(0); i < uint64(numChunks); i++ {

		if err := binary.Write(buffer, binary.LittleEndian, i); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}

		copy(segment[i][:], buffer.Bytes())
		/*t.Logf("Adding chunk fp %x:", chunks[i])*/
		buffer.Reset()
	}

	hooks := si.getHooks(segment, 100)
	if hooks.Size() == 0 {
		t.Fatalf("no hooks to test with")
	}

	// add it
	var expectedId uint32 = 1
	si.addSegment(segment, 100, new(SparseIndexStatistics))

	hooks.Foreach(func(fp *[12]byte) {
		if list, ok := si.sparseIndex[*fp]; !ok {
			t.Fatalf("hook %x was not added to sparseIndex", *fp)
		} else if list.Len() != 1 {
			t.Fatalf("list for hook %x has wrong size. expected: %v, got: %v", *fp, 1, list.Len())
		} else if list.Front().Value.(*Segment).Id != expectedId {
			t.Fatalf("segment for hook %x got wrong id. expected: %v, got: %v", *fp, expectedId, list.Front().Value.(*Segment).Id)
		}
	})

	// check internal segmentIndex
	if uint32(len(si.localSegmentIndex)) != expectedId {
		t.Fatalf("wront segmentIndex size. expected: %v, got: %v", expectedId, len(si.localSegmentIndex))
	}

	// add second one and check ordering
	si.addSegment(segment, 100, new(SparseIndexStatistics))
	expectedId++
	hooks.Foreach(func(fp *[12]byte) {
		if list, ok := si.sparseIndex[*fp]; !ok {
			t.Fatalf("hook %x was not added to sparseIndex", *fp)
		} else if list.Len() != 2 {
			t.Fatalf("list for hook %x has wrong size. expected: %v, got: %v", *fp, 2, list.Len())
		} else if list.Front().Value.(*Segment).Id != expectedId {
			t.Fatalf("segment for hook %x got wrong id. expected: %v, got: %v", *fp, expectedId, list.Front().Value.(*Segment).Id)
		}
	})

	// add more and check max list size
	for i := expectedId; i < uint32(2*si.maxHooksPerSegment); i++ {
		si.addSegment(segment, 100, new(SparseIndexStatistics))
		expectedId++
	}
	hooks.Foreach(func(fp *[12]byte) {
		if list, ok := si.sparseIndex[*fp]; !ok {
			t.Fatalf("hook %x was not added to sparseIndex", *fp)
		} else if list.Len() != si.maxHooksPerSegment {
			t.Fatalf("list for hook %x has wrong size. expected: %v, got: %v", *fp, si.maxHooksPerSegment, list.Len())
		} else if list.Front().Value.(*Segment).Id != expectedId {
			t.Fatalf("segment for hook %x got wrong id. expected: %v, got: %v", *fp, expectedId, list.Front().Value.(*Segment).Id)
		}
	})
}

func TestGetNextChampion(t *testing.T) {

	si := InitSparseIndex()
	numChunks := 32

	set1 := NewArraySet()
	set2 := NewArraySet()
	set3 := NewArraySet()
	buffer := new(bytes.Buffer)
	for i := uint64(0); i < uint64(numChunks); i++ {

		if err := binary.Write(buffer, binary.LittleEndian, i); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}

		var newFp [12]byte
		copy(newFp[:], buffer.Bytes())

		if i%2 == 0 {
			set1.Add(&newFp)
		}
		set2.Add(&newFp)
		set3.Add(&newFp)
		buffer.Reset()
	}

	// test whether the function returns the biggest set
	m := make(map[uint32]*ArraySet)
	m[1] = set1
	m[2] = set2
	m[3] = set1
	m[4] = set1

	if id, retSet := si.getNextChampion(m); id != 2 {
		t.Fatalf("got wrong set id back. expected set: %v, got: %v", 2, id)
	} else if retSet == nil {
		t.Fatalf("got nil set")
	}

	// if two or more equal sized candidates are present, the function must return the one with the bigger id
	m = make(map[uint32]*ArraySet)
	m[1] = set1
	m[2] = set2
	m[3] = set3
	m[4] = set1
	m[5] = set2 // this should be returned
	m[6] = set1

	if id, retSet := si.getNextChampion(m); id != 5 {
		t.Fatalf("got wrong set id back. expected set: %v, got: %v", 5, id)
	} else if retSet == nil {
		t.Fatalf("got nil set")
	} else if retSet != set2 {
		t.Fatalf("got wrong set back")
	}
}

func TestGetSegments(t *testing.T) {

	si := InitSparseIndex()
	maxSegmentCount := 4

	// empty hooks cause empty slice
	championList := make([]*Segment, 0)
	if segList := si.getSegments(NewArraySet(), maxSegmentCount, true, championList, new(SparseIndexStatistics), ""); len(segList) != 0 {
		t.Fatalf("got nonEmpty slice back")
	}

	numChunks := 1024
	segment := make([][12]byte, numChunks)

	buffer := new(bytes.Buffer)
	for i := uint64(0); i < uint64(numChunks); i++ {

		if err := binary.Write(buffer, binary.LittleEndian, i); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}

		copy(segment[i][:], buffer.Bytes())
		/*t.Logf("Adding chunk fp %x:", chunks[i])*/
		buffer.Reset()
	}

	hooks := si.getHooks(segment, 100)
	if hooks.Size() == 0 {
		t.Fatalf("no hooks to test with")
	}

	// check whether the function returns the correct amount of segments
	numSegments := 2
	for i := uint32(0); i < uint32(numSegments); i++ {
		si.addSegment(segment, 100, new(SparseIndexStatistics))
	}

	// should be 1 because all segments are equal up to now
	championList = championList[:0]
	if segList := si.getSegments(hooks, 2*numSegments, true, championList, new(SparseIndexStatistics), ""); len(segList) != 1 {
		t.Fatalf("got back wrong number of segments. expected: %v, got: %v", 1, len(segList))
	}

	// test with more segments
	si = InitSparseIndex()
	maxSegmentCount = 2
	numSegments = 0
	numHooks := 4
	segment = make([][12]byte, 2)

	hooks = NewArraySet()
	hooksList := make([][12]byte, 0)
	for i := uint64(0); len(hooksList) < numHooks; i++ {
		if err := binary.Write(buffer, binary.LittleEndian, i); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}

		var fp [12]byte
		copy(fp[:], buffer.Bytes())

		if si.isHook(&fp) {
			hooksList = append(hooksList, fp)
			hooks.Add(&fp)
		}
		buffer.Reset()
	}

	t.Logf("add segment with unique hook %x", hooksList[0:1][0])
	segment = hooksList[0:1]
	si.addSegment(segment, 100, new(SparseIndexStatistics))
	numSegments++

	t.Logf("add segment with unique hook %x", hooksList[1:2][0])
	segment = hooksList[1:2]
	si.addSegment(segment, 100, new(SparseIndexStatistics))
	numSegments++

	t.Logf("add segment with unique hook %x", hooksList[2:3][0])
	segment = hooksList[2:3]
	si.addSegment(segment, 100, new(SparseIndexStatistics))
	numSegments++

	t.Logf("add segment with unique hook %x", hooksList[3:4][0])
	segment = hooksList[3:4]
	si.addSegment(segment, 100, new(SparseIndexStatistics))
	numSegments++
	si.addSegment(segment, 100, new(SparseIndexStatistics))
	numSegments++

	hooks.Foreach(func(fp *[12]byte) {
		if list, ok := si.sparseIndex[*fp]; !ok {
			t.Fatalf("hook %x is not in the index", *fp)
		} else if list.Len() == 0 {
			t.Fatalf("list hook %x is empty", *fp)
		} else {
			t.Logf("list length of hook %x : %v", *fp, list.Len())
		}
	})

	if len(si.sparseIndex) != hooks.Size() {
		t.Fatalf("unexpected internal sparseIndex size. expected: %v, got: %v", hooks.Size(), len(si.sparseIndex))
	}

	championList = championList[:0]
	if segList := si.getSegments(hooks, 2*numSegments, true, championList, new(SparseIndexStatistics), ""); len(segList) != numSegments-1 {
		t.Logf("%v", segList[0].Id)
		t.Fatalf("got back wrong number of segments. expected: %v, got: %v", numSegments-1, len(segList))
	}

	championList = championList[:0]
	if segList := si.getSegments(hooks, maxSegmentCount, true, championList, new(SparseIndexStatistics), ""); len(segList) != maxSegmentCount {
		t.Fatalf("got back wrong number of segments. expected: %v, got: %v", maxSegmentCount, len(segList))
	}
}
