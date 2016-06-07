package sortedChunkIndexing

import "testing"
import "bytes"
import "encoding/binary"

import "github.com/jkaiser/dedup_simulations/common"

func TestComputePageUtility(t *testing.T) {

	/*sortedCI = NewSortedChunkIndex(48, 1024*1024, nil)*/
	entriesPerPage := 100

	chunksA := make([]common.Chunk, 0)
	chunksB := make([]common.Chunk, 0)

	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.LittleEndian, int64(42)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}

	c := common.Chunk{Digest: make([]byte, 12)}
	copy(c.Digest, buffer.Bytes())
	chunksA = append(chunksA, c)

	// everything is total similar to itself -> utility is 1 for identital lists
	if utilA, utilB := computePageUtility(chunksA, chunksA, entriesPerPage); utilA != 1 {
		t.Fatalf("got wrong util val. expected %v, got %v", 1, utilA)
	} else if utilB != 1 {
		t.Fatalf("got wrong util val. expected %v, got %v", 1, utilB)
	}

	buffer.Reset()
	if err := binary.Write(buffer, binary.LittleEndian, int64(17)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}

	c = common.Chunk{Digest: make([]byte, 12)}
	copy(c.Digest, buffer.Bytes())
	chunksB = append(chunksB, c)

	// both fully fit in entriylimit -> util mus be 0.5
	if utilA, utilB := computePageUtility(chunksA, chunksB, entriesPerPage); utilA != 0.5 {
		t.Fatalf("got wrong util val. expected %v, got %v", 0.5, utilA)
	} else if utilB != 0.5 {
		t.Fatalf("got wrong util val. expected %v, got %v", 0.5, utilB)
	}

	// only B's chunk is inside
	if utilA, utilB := computePageUtility(chunksA, chunksB, 1); utilA != 0 {
		t.Fatalf("got wrong util val. expected %v, got %v", 0, utilA)
	} else if utilB != 1 {
		t.Fatalf("got wrong util val. expected %v, got %v", 1, utilB)
	}

	// fill a little more....
	buffer.Reset()
	if err := binary.Write(buffer, binary.LittleEndian, int64(68)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}

	t.Logf("Add %x ", buffer.Bytes())
	c = common.Chunk{Digest: make([]byte, 12)}
	copy(c.Digest, buffer.Bytes())
	chunksA = append(chunksA, c)

	buffer.Reset()
	if err := binary.Write(buffer, binary.LittleEndian, int64(70)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}

	c = common.Chunk{Digest: make([]byte, 12)}
	copy(c.Digest, buffer.Bytes())
	chunksA = append(chunksA, c)

	if utilA, utilB := computePageUtility(chunksA, chunksB, entriesPerPage); utilA != 0.75 {
		t.Fatalf("got wrong util val. expected %v, got %v", 0.75, utilA)
	} else if utilB != 0.25 {
		t.Fatalf("got wrong util val. expected %v, got %v", 0.25, utilB)
	}
}

func TestMergeMatrixEntriy(t *testing.T) {

	// init
	var matrixA, matrixB matrixEntry
	matrixA.firstNChunks = make([]common.Chunk, 0)
	matrixB.firstNChunks = make([]common.Chunk, 0)
	matrixA.traces = make([]*traceTuple, 0)
	matrixB.traces = make([]*traceTuple, 0)

	matrixA.traces = append(matrixA.traces, new(traceTuple))
	matrixB.traces = append(matrixB.traces, new(traceTuple))
	matrixB.traces = append(matrixB.traces, new(traceTuple))

	matrixA.traces[0].hostname = "firstHost"
	matrixB.traces[0].hostname = "secondHost"

	// test for empty matrix entries
	if nM := mergeMatrixEntries(matrixA, matrixB, 0); len(nM.firstNChunks) != 0 {
		t.Fatalf("list should be empty, but has %v elements.", len(nM.firstNChunks))
	}

	if nM := mergeMatrixEntries(matrixA, matrixB, 42); len(nM.firstNChunks) != 0 {
		t.Fatalf("list should be empty, but has %v elements.", len(nM.firstNChunks))
	}

	// tests with chunks
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.LittleEndian, int64(42)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}
	c := common.Chunk{Digest: make([]byte, 12)}
	copy(c.Digest, buffer.Bytes())
	matrixA.firstNChunks = append(matrixA.firstNChunks, c)

	if err := binary.Write(buffer, binary.LittleEndian, int64(17)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}
	c = common.Chunk{Digest: make([]byte, 12)}
	copy(c.Digest, buffer.Bytes())
	matrixB.firstNChunks = append(matrixB.firstNChunks, c)

	if nM := mergeMatrixEntries(matrixA, matrixB, 42); len(nM.firstNChunks) != 2 { // normal merge
		t.Fatalf("wrong list length, expected %v elements, got %v.", 2, len(nM.firstNChunks))
	} else if len(nM.traces) != 3 {
		t.Fatalf("wrong list length, expected %v elements, got %v.", 3, len(nM.traces))
	}

	if nM := mergeMatrixEntries(matrixA, matrixB, 1); len(nM.firstNChunks) != 1 { // merge with cutting
		t.Fatalf("wrong list length, expected %v elements, got %v.", 1, len(nM.firstNChunks))
	} else if len(nM.traces) != 3 {
		t.Fatalf("wrong list length, expected %v elements, got %v.", 3, len(nM.traces))
	}

	// no duplicates?
	if nM := mergeMatrixEntries(matrixA, matrixA, 42); len(nM.firstNChunks) != len(matrixA.firstNChunks) {
		t.Fatalf("wrong list length, expected %v elements, got %v.", len(matrixA.firstNChunks), len(nM.firstNChunks))
	} else if len(nM.traces) != 2*len(matrixA.traces) {
		t.Fatalf("wrong list length, expected %v elements, got %v.", 2*len(matrixA.traces), len(nM.traces))
	}
}

// Tests the computeNewTraceGroups function. Mosts of the tests assume that the function merges two
// traceTuples if the threshold is greater or equal of the _average_ utilities.
func TestComputeTraceGroups(t *testing.T) {

	var tt1, tt2, tt3 traceTuple
	tt1.hostname = "hostA"
	tt2.hostname = "hostB"
	tt3.hostname = "hostC"
	tt1.firstUniques = make([]common.Chunk, 0)
	tt2.firstUniques = make([]common.Chunk, 0)
	tt3.firstUniques = make([]common.Chunk, 0)

	tuples := []*traceTuple{&tt1, &tt2}

	// with theshold 1 nothing should be merged (only if both have chunks and are equal)
	if res := computeNewTraceGroups(tuples, 1, 42); len(res) != 2 {
		t.Fatalf("wrong number of groups. expected: %v, got: %v", 2, len(res))
	} else if len(res[0]) != 1 {
		t.Fatalf("wrong size of first group. expected: %v, got: %v", 1, len(res[0]))
	} else if len(res[1]) != 1 {
		t.Fatalf("wrong size of second group. expected: %v, got: %v", 1, len(res[1]))
	}

	// with threshold 0 everything should be merged together
	if res := computeNewTraceGroups(tuples, 0, 42); len(res) != 1 {
		t.Fatalf("wrong number of groups. expected: %v, got: %v", 1, len(res))
	} else if len(res[0]) != 2 {
		t.Fatalf("wrong size of first group. expected: %v, got: %v", 2, len(res[0]))
	}

	// test with chunks
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.LittleEndian, int64(42)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}
	c := common.Chunk{Digest: make([]byte, 12)}
	copy(c.Digest, buffer.Bytes())
	tt1.firstUniques = append(tt1.firstUniques, c)

	buffer.Reset()
	if err := binary.Write(buffer, binary.LittleEndian, int64(17)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}
	c = common.Chunk{Digest: make([]byte, 12)}
	copy(c.Digest, buffer.Bytes())
	tt2.firstUniques = append(tt2.firstUniques, c)

	// both should be merged since they fit into 1 page
	if res := computeNewTraceGroups(tuples, 0.5, 42); len(res) != 1 {
		t.Fatalf("wrong number of groups. expected: %v, got: %v", 1, len(res))
	} else if len(res[0]) != 2 {
		t.Fatalf("wrong size of first group. expected: %v, got: %v", 2, len(res[0]))
	}

	// no merge because the average utilazation is 0.5
	if res := computeNewTraceGroups(tuples, 1, 1); len(res) != 2 {
		t.Fatalf("wrong number of groups. expected: %v, got: %v", 2, len(res))
	} else if len(res[0]) != 1 {
		t.Fatalf("wrong size of first group. expected: %v, got: %v", 1, len(res[0]))
	} else if len(res[1]) != 1 {
		t.Fatalf("wrong size of second group. expected: %v, got: %v", 2, len(res[1]))
	}

	// both should be merged now
	if res := computeNewTraceGroups(tuples, 0.5, 1); len(res) != 1 {
		t.Fatalf("wrong number of groups. expected: %v, got: %v", 1, len(res))
	} else if len(res[0]) != 2 {
		t.Fatalf("wrong size of first group. expected: %v, got: %v", 2, len(res[0]))
	}

	// test with 3 traces
	tuples = []*traceTuple{&tt1, &tt2, &tt3}
	buffer.Reset()
	if err := binary.Write(buffer, binary.LittleEndian, int64(42)); err != nil {
		t.Fatalf("writing binary representaion failed: %v", err)
	}
	c = common.Chunk{Digest: make([]byte, 12)}
	copy(c.Digest, buffer.Bytes())
	tt3.firstUniques = append(tt3.firstUniques, c)

	// all should be merged since they fit into 1 page
	if res := computeNewTraceGroups(tuples, 0.2, 42); len(res) != 1 {
		t.Fatalf("wrong number of groups. expected: %v, got: %v", 1, len(res))
	} else if len(res[0]) != 3 {
		t.Fatalf("wrong size of first group. expected: %v, got: %v", 3, len(res[0]))
	}

	// only 1 merge because the max util after the first is only 0.5
	if res := computeNewTraceGroups(tuples, 0.6, 42); len(res) != 2 {
		t.Fatalf("wrong number of groups. expected: %v, got: %v", 2, len(res))
	} else if len(res[0]) != 2 {
		t.Fatalf("wrong size of first group. expected: %v, got: %v", 2, len(res[0]))
	} else if len(res[1]) != 1 {
		t.Fatalf("wrong size of first group. expected: %v, got: %v", 1, len(res[1]))
	}
}
