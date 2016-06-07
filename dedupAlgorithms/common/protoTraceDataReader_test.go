package common

import "fmt"
import "testing"
import "bytes"
import "bufio"
import dedupProto "github.com/jkaiser/dedup_simulations/de_pc2_dedup_fschunk"

/*import "code.google.com/p/goprotobuf/proto"*/
import "github.com/gogo/protobuf/proto"
import log "github.com/cihub/seelog"

var testConfig string = `
<seelog type="sync">
    <outputs formatid="main">
        <console />
    </outputs>
    <formats>
        <format id="main" format="%Date %Time [%Level] %Func: %Msg%n"/>
    </formats>
</seelog>`

func TestCodeDecode(t *testing.T) {

	num := uint64(42)
	buf := proto.EncodeVarint(num)
	x, n := proto.DecodeVarint(buf)
	if x != num {
		t.Fatalf("got %v, want %v", x, num)
	}
	if n != len(buf) {
		t.Fatalf("consumed %v, want %v", n, len(buf))
	}
	x, n = 0, 0

	num2 := uint64(31)
	buf2 := proto.EncodeVarint(num2)
	lenBuf1 := len(buf)
	buf = append(buf, buf2...)
	x, n = proto.DecodeVarint(buf)
	if x != num {
		t.Fatalf("error for consecutive varints: got %v, want %v", x, num)
	}
	if n != lenBuf1 {
		t.Fatalf("error for consecutive varints: consumed %v, want %v", n, len(buf))
	}
	x, n = proto.DecodeVarint(buf[n:])
	if x != num2 {
		t.Fatalf("error for consecutive varints: got %v, want %v", x, num)
	}
	if n != len(buf2) {
		t.Fatalf("error for consecutive varints: consumed %v, want %v", n, len(buf))
	}
}

func TestGetNextMessageSize(t *testing.T) {
	if logger, err := log.LoggerFromConfigAsBytes([]byte(testConfig)); err != nil {
		fmt.Println(err)
	} else {
		if loggerErr := log.ReplaceLogger(logger); loggerErr != nil {
			fmt.Println(loggerErr)
		}
	}

	numTests := uint64(10000)
	var buffer bytes.Buffer
	lengths := make([]uint64, numTests)

	for i := uint64(0); i < numTests; i++ {
		b := proto.EncodeVarint(i)
		if n, err := buffer.Write(b); (err != nil) || (n != len(b)) {
			t.Fatalf("Couldn't write to buffer in testsetup")
		}
		lengths[i] = uint64(len(b))
	}

	readerBuffer := bytes.NewReader(buffer.Bytes())
	tmp := bufio.NewReader(readerBuffer)
	/*tReader := ProtoTraceDataReader{0, "", tmp, make([]byte, 10), make([]byte, 1024), nil, proto.NewBuffer(nil)}*/
	tReader := ProtoTraceDataReader{
		numReadChunks: 0,
		/*numExistingFileEntries int*/
		filePath:        "",
		fileReader:      tmp,
		msgSizeBuf:      make([]byte, 10), // small optimization to prevent many allocations. Used for buffering raw protobuf msgs
		msgBuf:          make([]byte, 1024),
		FileEntryReturn: nil,
	}

	// test
	for i := uint64(0); i < numTests; i++ {
		num := tReader.getNextMessageSize()
		if num != i {
			t.Fatalf("for number %v: got %v, want %v", i, num, i)
		}
	}

	// half empty
	buffer.Reset()
	b := proto.EncodeVarint(99999999)
	if n, err := buffer.Write(b); (err != nil) || (n != len(b)) {
		t.Fatalf("Couldn't write to buffer in testsetup")
	}

	readerBuffer = bytes.NewReader(buffer.Bytes()[:1])
	tmp = bufio.NewReader(readerBuffer)
	if num := tReader.getNextMessageSize(); num != 0 {
		t.Fatalf("Misread invalid varint: got %v, want 0", num)
	}

	// empty
	buffer.Reset()
	readerBuffer = bytes.NewReader(buffer.Bytes())
	tmp = bufio.NewReader(readerBuffer)
	if num := tReader.getNextMessageSize(); num != 0 {
		t.Fatalf("Misread invalid varint: got %v, want 0", num)
	}
}

func TestReadNextFile(t *testing.T) {
	if logger, err := log.LoggerFromConfigAsBytes([]byte(testConfig)); err != nil {
		fmt.Println(err)
	} else {
		if loggerErr := log.ReplaceLogger(logger); loggerErr != nil {
			fmt.Println(loggerErr)
		}
	}

	// prepare
	var protoFile dedupProto.File
	numTests := uint64(1000)
	var buffer bytes.Buffer

	sizes := make([]int, numTests)

	for i := uint64(0); i < numTests; i++ {
		protoFile.Reset()
		protoFile.Filename = proto.String("FooName")
		protoFile.Fsize = proto.Uint64(uint64(i))
		protoFile.Label = proto.String("wadda")
		protoFile.ChunkCount = proto.Uint32(uint32(i) + uint32(3))
		protoFile.Type = proto.String("fooType")

		if data, err := proto.Marshal(&protoFile); err != nil {
			t.Fatalf("Couldn't marshal protoFile %v", i)
		} else {
			varintBuf := proto.EncodeVarint(uint64(len(data)))
			buffer.Write(varintBuf)
			buffer.Write(data)
			sizes[i] = len(data) + len(varintBuf)
		}
	}

	tmp := bufio.NewReader(bytes.NewReader(buffer.Bytes()))
	/*tReader := ProtoTraceDataReader{0, "", tmp, make([]byte, 10), make([]byte, 1024), nil, proto.NewBuffer(nil)}*/
	tReader := ProtoTraceDataReader{
		numReadChunks:   0,
		filePath:        "",
		fileReader:      tmp,
		msgSizeBuf:      make([]byte, 10), // small optimization to prevent many allocations. Used for buffering raw protobuf msgs
		msgBuf:          make([]byte, 1024),
		FileEntryReturn: nil,
	}

	// test
	sum := uint64(0)
	for i := uint64(0); i < numTests; i++ {
		/*log.Info("will read msg ", i, " size of varint+msg = ", sizes[i], "; consumed ", sum , " bytes up to now")*/
		protoFile.Reset()
		size := tReader.readNextFile(&protoFile)
		if size == 0 {
			t.Fatalf("Could not read message %v", i)
		}
		sum += size

		if name := protoFile.GetFilename(); name != "FooName" {
			t.Fatalf("Read wrong filename in msg %v. got %v, want %v", i, name, "FooName")
		}
		if size := protoFile.GetFsize(); size != i {
			t.Fatalf("Read wrong size in msg %v. got %v, want %v", i, size, i)
		}
		if label := protoFile.GetLabel(); label != "wadda" {
			t.Fatalf("Read wrong label in msg %v. got %v, want %v", i, label, "wadda")
		}
		if chunkCount := protoFile.GetChunkCount(); chunkCount != uint32(i)+3 {
			t.Fatalf("Read wrong size in msg %v. got %v, want %v", i, size, i)
		}
		if tp := protoFile.GetType(); tp != "fooType" {
			t.Fatalf("Read wrong type in msg %v. got %v, want %v", i, tp, "fooType")
		}
	}
}

func TestReadNextChunk(t *testing.T) {
	if logger, err := log.LoggerFromConfigAsBytes([]byte(testConfig)); err != nil {
		fmt.Println(err)
	} else {
		if loggerErr := log.ReplaceLogger(logger); loggerErr != nil {
			fmt.Println(loggerErr)
		}
	}

	// prepare
	var protoChunk dedupProto.Chunk
	numTests := uint64(1000)
	var buffer bytes.Buffer
	var size int

	var digest = []byte{'w', 'u', 's', 'a', 'w', 'i', 'g'}
	for i := uint64(0); i < numTests; i++ {
		protoChunk.Reset()
		protoChunk.Csize = proto.Uint32(uint32(i))
		protoChunk.Fp = digest

		if data, err := proto.Marshal(&protoChunk); err != nil {
			t.Fatalf("Couldn't marshal protoChunk %v", i)
		} else {
			size += len(data) + len(proto.EncodeVarint(uint64(len(data))))
			buffer.Write(proto.EncodeVarint(uint64(len(data))))
			buffer.Write(data)
		}
	}

	tmp := bufio.NewReader(bytes.NewReader(buffer.Bytes()))
	/*tReader := ProtoTraceDataReader{0, "", tmp, make([]byte, 10), make([]byte, 1024), nil, proto.NewBuffer(nil)}*/
	tReader := ProtoTraceDataReader{
		numReadChunks:   0,
		filePath:        "",
		fileReader:      tmp,
		msgSizeBuf:      make([]byte, 10), // small optimization to prevent many allocations. Used for buffering raw protobuf msgs
		msgBuf:          make([]byte, 1024),
		FileEntryReturn: nil,
	}

	// test
	for i := uint64(0); i < numTests; i++ {
		protoChunk.Reset()
		tReader.readNextChunk(&protoChunk)

		if size := protoChunk.GetCsize(); size != uint32(i) {
			t.Fatalf("Read wrong size in msg %v. got %v, want %v", i, size, i)
		}
		if !bytes.Equal(protoChunk.GetFp(), digest) {
			t.Fatalf("Read wrong fp in msg %v. got %v, want %v", i, protoChunk.GetFp(), digest)
		}
	}
}

func TestEmptyTracefile(t *testing.T) {
	if logger, err := log.LoggerFromConfigAsBytes([]byte(testConfig)); err != nil {
		fmt.Println(err)
	} else {
		if loggerErr := log.ReplaceLogger(logger); loggerErr != nil {
			fmt.Println(loggerErr)
		}
	}

	var buffer bytes.Buffer
	tmp := bufio.NewReader(bytes.NewReader(buffer.Bytes()))
	tReader := ProtoTraceDataReader{
		numReadChunks:   0,
		filePath:        "",
		fileReader:      tmp,
		msgSizeBuf:      make([]byte, 10), // small optimization to prevent many allocations. Used for buffering raw protobuf msgs
		msgBuf:          make([]byte, 1024),
		FileEntryReturn: nil,
	}

	// test
	outputChan := make(chan *FileEntry, 100)
	tReader.FeedAlgorithm(outputChan)

	if m, ok := <-outputChan; ok {
		t.Fatalf("Reader returned a FileEntry from an empty file: %v", m)
	}
}

var num uint64

// performs decoding of numVarInts varints and returns the number of decoded bytes
func getNextMessageSize(numVarInts int, b *testing.B) {

	b.StopTimer()
	// create
	var byteCounter int64 = 0
	numTests := uint64(numVarInts)
	var buffer bytes.Buffer

	for i := uint64(0); i < numTests; i++ {
		buf := proto.EncodeVarint(i)
		buffer.Write(buf)
		byteCounter += int64(len(buf))
	}

	readerBuffer := bytes.NewReader(buffer.Bytes())
	tmp := bufio.NewReader(readerBuffer)
	/*tReader := ProtoTraceDataReader{0, "", tmp, make([]byte, 10), make([]byte, 1024), nil, proto.NewBuffer(nil)}*/
	tReader := ProtoTraceDataReader{
		numReadChunks:   0,
		filePath:        "",
		fileReader:      tmp,
		msgSizeBuf:      make([]byte, 10), // small optimization to prevent many allocations. Used for buffering raw protobuf msgs
		msgBuf:          make([]byte, 1024),
		FileEntryReturn: nil,
	}

	// test
	b.StartTimer()
	for i := uint64(0); i < numTests; i++ {
		num = tReader.getNextMessageSize()
	}

	b.SetBytes(byteCounter)
}

func benchmarkGetNextMessageSize(numVarInts int, b *testing.B) {

	for n := 0; n < b.N; n++ {
		getNextMessageSize(numVarInts, b)
	}
}

func BenchmarkGetNextMessageSize10(b *testing.B)  { benchmarkGetNextMessageSize(10, b) }
func BenchmarkGetNextMessageSize100(b *testing.B) { benchmarkGetNextMessageSize(100, b) }
func BenchmarkGetNextMessageSize1K(b *testing.B)  { benchmarkGetNextMessageSize(1000, b) }
func BenchmarkGetNextMessageSize10K(b *testing.B) { benchmarkGetNextMessageSize(10000, b) }

func readNextFile(numMessages int, b *testing.B) {

	b.StopTimer()
	// prepare
	var protoFile dedupProto.File
	numTests := uint64(numMessages)
	var buffer bytes.Buffer
	var byteCounter int64 = 0

	for i := uint64(0); i < numTests; i++ {
		protoFile.Reset()
		protoFile.Filename = proto.String("FooName")
		protoFile.Fsize = proto.Uint64(uint64(i))
		protoFile.Label = proto.String("wadda")
		protoFile.ChunkCount = proto.Uint32(uint32(i) + uint32(3))
		protoFile.Type = proto.String("fooType")

		if data, err := proto.Marshal(&protoFile); err != nil {
			b.Fatalf("Couldn't marshal protoFile %v", i)
		} else {
			varIntBuf := proto.EncodeVarint(uint64(len(data)))
			buffer.Write(varIntBuf)
			buffer.Write(data)
			byteCounter += int64(len(varIntBuf) + len(data))
		}
	}

	tmp := bufio.NewReader(bytes.NewReader(buffer.Bytes()))
	/*tReader := ProtoTraceDataReader{0, "", tmp, make([]byte, 10), make([]byte, 1024), nil, proto.NewBuffer(nil)}*/
	tReader := ProtoTraceDataReader{
		numReadChunks:   0,
		filePath:        "",
		fileReader:      tmp,
		msgSizeBuf:      make([]byte, 10), // small optimization to prevent many allocations. Used for buffering raw protobuf msgs
		msgBuf:          make([]byte, 1024),
		FileEntryReturn: nil,
	}

	b.StartTimer()

	// test
	for i := uint64(0); i < numTests; i++ {
		num = tReader.readNextFile(&protoFile)
	}

	b.SetBytes(byteCounter)
}

func benchmarkReadNextFile(numVarInts int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		readNextFile(numVarInts, b)
	}
}

func BenchmarkReadNextFile10(b *testing.B)  { benchmarkReadNextFile(10, b) }
func BenchmarkReadNextFile100(b *testing.B) { benchmarkReadNextFile(100, b) }
func BenchmarkReadNextFile1K(b *testing.B)  { benchmarkReadNextFile(1000, b) }
func BenchmarkReadNextFile10K(b *testing.B) { benchmarkReadNextFile(10000, b) }

func readNextChunk(numMessages int, b *testing.B) {
	b.StopTimer()
	// prepare
	var protoChunk dedupProto.Chunk
	numTests := uint64(numMessages)
	var buffer bytes.Buffer
	var byteCounter int64 = 0

	var digest = []byte{'w', 'u', 's', 'a', 'w', 'i', 'g'}
	for i := uint64(0); i < numTests; i++ {
		protoChunk.Reset()
		protoChunk.Csize = proto.Uint32(uint32(i))
		protoChunk.Fp = digest

		if data, err := proto.Marshal(&protoChunk); err != nil {
			b.Fatalf("Couldn't marshal protoChunk %v", i)
		} else {
			varIntBuf := proto.EncodeVarint(uint64(len(data)))
			buffer.Write(varIntBuf)
			buffer.Write(data)
			byteCounter += int64(len(varIntBuf) + len(data))
		}
	}

	b.StartTimer()

	tmp := bufio.NewReader(bytes.NewReader(buffer.Bytes()))
	/*tReader := ProtoTraceDataReader{0, "", tmp, make([]byte, 10), make([]byte, 1024), nil, proto.NewBuffer(nil)}*/
	tReader := ProtoTraceDataReader{
		numReadChunks:   0,
		filePath:        "",
		fileReader:      tmp,
		msgSizeBuf:      make([]byte, 10), // small optimization to prevent many allocations. Used for buffering raw protobuf msgs
		msgBuf:          make([]byte, 1024),
		FileEntryReturn: nil,
	}

	// test
	for i := uint64(0); i < numTests; i++ {
		tReader.readNextChunk(&protoChunk)
	}

	b.SetBytes(byteCounter)
}

func benchmarkReadNextChunk(numVarInts int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		readNextChunk(numVarInts, b)
	}
}

func BenchmarkReadNextChunk10(b *testing.B)  { benchmarkReadNextChunk(10, b) }
func BenchmarkReadNextChunk100(b *testing.B) { benchmarkReadNextChunk(100, b) }
func BenchmarkReadNextChunk1K(b *testing.B)  { benchmarkReadNextChunk(1000, b) }
func BenchmarkReadNextChunk10K(b *testing.B) { benchmarkReadNextChunk(10000, b) }
