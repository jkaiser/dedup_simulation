package blc

import "testing"
import "encoding/binary"
import "bytes"
import "sync"
import "fmt"
import log "github.com/cihub/seelog"
import "github.com/jkaiser/dedup_simulations/common"

var samplingBLC *SamplingBlockIndexFilter

func InitSamplingBLC() {
	config := SamplingBlockIndexFilterConfig{
		BlockSize:                        262144,
		BlockIndexPageSize:               32768,
		ChunkIndexPageSize:               4096,
		DiffCacheSize:                    4,
		BlockCacheSize:                   2048,
		ChunkCacheSize:                   1024,
		MinDiffValue:                     0,
		SampleFactor:                     5,
		MaxBackupGenerationsInBlockIndex: 8,
	}
	chunkIndexLock := new(sync.RWMutex)
	// TODO: create block index here and give it to the constructor
	samplingBLC = NewSamplingBlockIndexFilter(config, 8*1024, make(map[[12]byte]uint32), chunkIndexLock, nil, "")

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
}

func InitSamplingBLCNoChunkCache() {
	config := SamplingBlockIndexFilterConfig{
		BlockSize:                        262144,
		BlockIndexPageSize:               32768,
		ChunkIndexPageSize:               4096,
		DiffCacheSize:                    4,
		BlockCacheSize:                   2048,
		ChunkCacheSize:                   0,
		MinDiffValue:                     0,
		SampleFactor:                     5,
		MaxBackupGenerationsInBlockIndex: 8,
	}

	chunkIndexLock := new(sync.RWMutex)
	// TODO: create block index here and give it to the constructor
	samplingBLC = NewSamplingBlockIndexFilter(config, 8*1024, make(map[[12]byte]uint32), chunkIndexLock, nil, "")

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
}

func InitSamplingBLCHighSampling() {
	config := SamplingBlockIndexFilterConfig{
		BlockSize:                        262144,
		BlockIndexPageSize:               32768,
		ChunkIndexPageSize:               4096,
		DiffCacheSize:                    4,
		BlockCacheSize:                   2048,
		ChunkCacheSize:                   1024,
		MinDiffValue:                     0,
		SampleFactor:                     10,
		MaxBackupGenerationsInBlockIndex: 8,
	}

	chunkIndexLock := new(sync.RWMutex)
	// TODO: create block index here and give it to the constructor
	samplingBLC = NewSamplingBlockIndexFilter(config, 8*1024, make(map[[12]byte]uint32), chunkIndexLock, nil, "")
}

func TestIsHook(t *testing.T) {

	if samplingBLC == nil {
		InitSamplingBLC()
	}
	t.Logf("testing isHook with SampleFactors %v", samplingBLC.config.SampleFactor)
	var fp [12]byte
	fp[11] = byte(1)

	if !samplingBLC.isHook(&fp) {
		t.Fatalf("%x was not detected as hook. checked from wrong side?", fp)
	}

	fp[11] = byte(0)
	for i := byte(0); i < 255; i++ {
		fp[0] = i
		t.Logf("testing fp: %x", fp)
		if (i%(1<<uint32(samplingBLC.config.SampleFactor%8)) == 0) && !samplingBLC.isHook(&fp) {
			t.Fatalf("%v was not detected as hook", i)
		} else if ((i % (1 << uint32(samplingBLC.config.SampleFactor%8))) != 0) && samplingBLC.isHook(&fp) {
			t.Fatalf("%v was detected as hook", i)
		}
	}

	InitSamplingBLCHighSampling()
	fp[0] = 0
	for i := byte(0); i < 255; i++ {
		fp[1] = i
		if (i%(1<<uint32(samplingBLC.config.SampleFactor%8)) == 0) && !samplingBLC.isHook(&fp) {
			t.Fatalf("%v was not detected as hook", i)
		} else if (i%(1<<uint32(samplingBLC.config.SampleFactor%8)) != 0) && samplingBLC.isHook(&fp) {
			t.Fatalf("%v was detected as hook", i)
		}
	}
}

var res bool

func benchmarkIsHook(numTests int, b *testing.B) {
	b.StopTimer()
	InitSamplingBLC()
	var fp [12]byte
	b.StartTimer()

	for i := 0; i < numTests; i++ {
		res = samplingBLC.isHook(&fp)
	}

}

func BenchmarkIsHook1K(b *testing.B)   { benchmarkIsHook(1E3, b) }
func BenchmarkIsHook10K(b *testing.B)  { benchmarkIsHook(1E4, b) }
func BenchmarkIsHook100K(b *testing.B) { benchmarkIsHook(1E5, b) }
func BenchmarkIsHook1M(b *testing.B)   { benchmarkIsHook(1E6, b) }

func TestStartEndTraceFile(t *testing.T) {

	if samplingBLC == nil {
		InitSamplingBLC()
	}

	samplingBLC.BeginTraceFile(0, "")
	samplingBLC.EndTraceFile()

	if len(samplingBLC.StatsList) != 1 {
		t.Fatalf("BLC didn't store statistics")
	}
}

func TestHandleChunks(t *testing.T) {

	InitSamplingBLC()

	fileEntry := new(FileEntry)

	// fill entry with chunks
	numChunks := 1000
	chunks := make([]common.Chunk, numChunks)
	buffer := new(bytes.Buffer)
	for i := uint64(0); i < uint64(numChunks); i++ {

		if err := binary.Write(buffer, binary.LittleEndian, i); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}

		newFp := make([]byte, 12)
		copy(newFp, buffer.Bytes())
		t.Logf("Adding chunk fp %x with length:", newFp, len(newFp))
		chunks[i] = common.Chunk{Size: 12, Digest: newFp, ChunkHash: int64(i)}
		buffer.Reset()
	}

	fileEntry.Chunks = chunks

	// test
	samplingBLC.handleChunks(fileEntry)

	if len(samplingBLC.chunkIndex) != numChunks {
		t.Fatalf("size of internal chunk index incorrect. got: %v, expected: %v", len(samplingBLC.chunkIndex), numChunks)
	}

	// check statistics
	if samplingBLC.Statistics.ChunkCount != numChunks {
		t.Fatalf("number of chunks incorrect. got: %v, expected: %v", samplingBLC.Statistics.ChunkCount, numChunks)
	}
	if samplingBLC.Statistics.UniqueChunkCount != numChunks {
		t.Fatalf("number of unique chunks incorrect. got: %v, expected: %v", samplingBLC.Statistics.UniqueChunkCount, numChunks)
	}
	if samplingBLC.Statistics.RedundantChunkCount != 0 {
		t.Fatalf("number of redundant chunks incorrect. got: %v, expected: %v", samplingBLC.Statistics.RedundantChunkCount, 0)
	}
	if samplingBLC.Statistics.UndetectedDuplicatesCount != 0 {
		t.Fatalf("number of duplicated chunks incorrect. got: %v, expected: %v", samplingBLC.Statistics.UndetectedDuplicatesCount, 0)
	}
	if samplingBLC.Statistics.WriteCacheHits != 0 {
		t.Fatalf("number of writecachehits incorrect. got: %v, expected: %v", samplingBLC.Statistics.WriteCacheHits, 0)
	}
}

// tests whether the WriteCacheHits actually are counted
func TestHandleChunksBlockMappingHits(t *testing.T) {

	InitSamplingBLCNoChunkCache()

	fileEntry := new(FileEntry)

	// fill entry with chunks
	numChunks := 10
	chunks := make([]common.Chunk, numChunks)
	buffer := new(bytes.Buffer)
	for i := uint64(0); i < uint64(numChunks); i++ {

		if err := binary.Write(buffer, binary.LittleEndian, int64(1)); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}

		newFp := make([]byte, 12)
		copy(newFp, buffer.Bytes())
		t.Logf("Adding chunk fp %x with length: %v", newFp, len(newFp))
		chunks[i] = common.Chunk{Size: 20, Digest: newFp, ChunkHash: int64(1)}
		buffer.Reset()
	}

	fileEntry.Chunks = chunks

	// test
	samplingBLC.handleChunks(fileEntry)

	if len(samplingBLC.chunkIndex) != 1 {
		t.Fatalf("size of internal chunk index incorrect. got: %v, expected: %v", len(samplingBLC.chunkIndex), numChunks)
	}

	// check statistics
	if samplingBLC.Statistics.ChunkCount != numChunks {
		t.Fatalf("number of chunks incorrect. got: %v, expected: %v", samplingBLC.Statistics.ChunkCount, numChunks)
	}
	if samplingBLC.Statistics.UniqueChunkCount != 1 {
		t.Fatalf("number of unique chunks incorrect. got: %v, expected: %v", samplingBLC.Statistics.UniqueChunkCount, numChunks)
	}
	if samplingBLC.Statistics.RedundantChunkCount != numChunks-1 {
		t.Fatalf("number of redundant chunks incorrect. got: %v, expected: %v", samplingBLC.Statistics.RedundantChunkCount, 0)
	}
	if samplingBLC.Statistics.WriteCacheHits != numChunks-1 {
		t.Fatalf("number of writecachehits incorrect. got: %v, expected: %v", samplingBLC.Statistics.WriteCacheHits, numChunks-1)
	}
}

func TestHandleChunksWithRedundancy(t *testing.T) {

	InitSamplingBLC()

	fileEntry := new(FileEntry)

	// fill entry with chunks
	numChunks := 1000
	chunks := make([]common.Chunk, 0, numChunks*2)
	var fp [12]byte
	buffer := new(bytes.Buffer)
	hookCounter := 0
	for i := uint64(0); i < uint64(numChunks); i++ {

		if err := binary.Write(buffer, binary.LittleEndian, i); err != nil {
			t.Fatalf("writing binary representaion failed: %v", err)
		}

		copy(fp[:], buffer.Bytes())
		if samplingBLC.isHook(&fp) {
			hookCounter++
		}

		newFp := make([]byte, 12)
		copy(newFp, buffer.Bytes())
		t.Logf("adding chunk fp %x with length: %v", newFp, len(newFp))
		chunks = append(chunks, common.Chunk{Size: 12, Digest: newFp, ChunkHash: int64(i)})
		buffer.Reset()
	}

	// add same amount again for redundancy
	chunks = append(chunks, chunks...)
	fileEntry.Chunks = chunks

	// test
	samplingBLC.handleChunks(fileEntry)

	if len(samplingBLC.chunkIndex) != numChunks {
		t.Fatalf("size of internal chunk index incorrect. got: %v, expected: %v", len(samplingBLC.chunkIndex), numChunks)
	}

	// check Statistics
	if samplingBLC.Statistics.ChunkCount != len(chunks) {
		t.Fatalf("number of chunks incorrect. got: %v, expected: %v", samplingBLC.Statistics.ChunkCount, len(chunks))
	}
	if samplingBLC.Statistics.UniqueChunkCount != numChunks {
		t.Fatalf("number of unique chunks incorrect. got: %v, expected: %v", samplingBLC.Statistics.UniqueChunkCount, numChunks)
	}
	if samplingBLC.Statistics.RedundantChunkCount != numChunks {
		t.Fatalf("number of redundant chunks incorrect. got: %v, expected: %v", samplingBLC.Statistics.RedundantChunkCount, numChunks/2)
	}
}
