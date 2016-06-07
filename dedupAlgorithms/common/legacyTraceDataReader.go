package common

import "os"
import "fmt"
import "io"
import "bufio"
import "strings"
import "strconv"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/common"
import dedupProto "github.com/jkaiser/dedup_simulations/de_pc2_dedup_fschunk"
import "github.com/gogo/protobuf/proto"

//const ConstMaxFileEntries = 60

type LegacyTraceDataReader struct {
	numReadChunks          int64
	numExistingFileEntries int
	filePath               string
	fileReader             *bufio.Reader
	msgSizeBuf             []byte // small optimization to prevent many allocations. Used for buffering raw protobuf msgs
	msgBuf                 []byte
	FileEntryReturn        chan *FileEntry
	bigChunkLists          [][]common.Chunk
	smallChunkLists        [][]common.Chunk
	bigFilesMap            map[string][]common.Chunk
}

func NewLegacyTraceDataReader(path string) *LegacyTraceDataReader {

	file, err := os.Open(path)
	if err != nil {
		log.Critical(fmt.Sprintf("could not read directory %v : %v", path, err))
		panic(fmt.Sprintf("could not read directory: %v", err))
	}

	/*return &LegacyTraceDataReader{path, bufio.NewReaderSize(file, 4*1024*1024), make([]byte, 10), make([]byte, 1024), make(chan *common.Chunk, 1000), make(chan *FileEntry, 1000)} // 4MB buffer size for file*/
	return &LegacyTraceDataReader{numExistingFileEntries: 0,
		filePath:        path,
		fileReader:      bufio.NewReaderSize(file, 1*1024*1024), // 1MB buffer size for file
		msgSizeBuf:      make([]byte, 10),
		msgBuf:          make([]byte, 1024),
		FileEntryReturn: make(chan *FileEntry, ConstMaxFileEntries),
		bigChunkLists:   make([][]common.Chunk, 0),
		smallChunkLists: make([][]common.Chunk, 0),
		bigFilesMap:     make(map[string][]common.Chunk),
	}
}

// Reads the buffer for the next message size and advances
// the buffer by that amount.
func (r *LegacyTraceDataReader) getNextMessageSize() uint64 {
	if buf, err := r.fileReader.Peek(len(r.msgSizeBuf)); (len(buf) == 0) || ((err != nil) && (err != io.EOF)) {
		if (err != nil) && (err == io.EOF) {
			log.Debug("EOF for ", r.filePath)
			return 0
		} else {
			log.Error(fmt.Sprintf("could not read next msg size: %v", err))
			return 0
		}
	} else {
		if msgSize, n := proto.DecodeVarint(buf); n == 0 {
			log.Critical(fmt.Sprintf("could not decode next msg size: %v", err))
			panic(fmt.Sprintf("could not read next msg size: %v", err))
		} else {

			num, err := r.fileReader.Read(r.msgSizeBuf[0:n])
			if err != nil {
				log.Error("Could not read from buffer")
			} else if num != n {
				log.Error("Read only ", num, " bytes instead of ", n)
			} else if msgSize == 0 {
				log.Error("file: ", r.filePath, " : determined next msgSize as 0, but there is no EOF")
			}
			return msgSize
		}
	}
}

// reads the next message size and returns its size
func (r *LegacyTraceDataReader) readNextFile(protoFile *dedupProto.File) uint64 {
	// get next size
	msgSize := r.getNextMessageSize()
	if msgSize == 0 { // EOF?
		if _, err := r.fileReader.ReadByte(); err == io.EOF {
			return msgSize
		} else {
			log.Critical("msg size is zero, but not EOF")
			panic(err)
		}
	}

	// read next File information
	if n, err := r.fileReader.Read(r.msgBuf[0:msgSize]); err != nil {
		log.Critical(err)
		panic(err)
	} else if uint64(n) != msgSize {
		if m, err := r.fileReader.Read(r.msgBuf[n:msgSize]); err != nil {
			log.Critical(err)
			panic(err)
		} else if uint64(n+m) != msgSize {
			log.Critical("Only read ", n+m, " bytes instead of", msgSize, " in second try")
			panic("Read too few bytes")
		}
	}
	if err := protoFile.Unmarshal(r.msgBuf[0:msgSize]); err != nil {
		log.Criticalf("unmarshaling error for file %v, msgSz was %v, error: %v", r.filePath, msgSize, err)
		panic(err)
	}
	return msgSize
}

func (r *LegacyTraceDataReader) readNextChunk(protoChunk *dedupProto.Chunk) {
	msgSize := r.getNextMessageSize()
	if msgSize == 0 { // EOF?
		log.Critical("Could not read size of next chunk")
		panic("Could not read size of next chunk")
	}

	// read next Chunk information
	if n, err := r.fileReader.Read(r.msgBuf[0:msgSize]); err != nil {
		log.Critical(err)
		panic(err)
	} else if uint64(n) != msgSize {
		if m, err := r.fileReader.Read(r.msgBuf[n:msgSize]); err != nil {
			log.Critical(err)
			panic(err)
		} else if uint64(n+m) != msgSize {
			log.Critical("Only read ", n+m, " bytes instead of", msgSize, " in second try")
			panic("Read too few bytes")
		}
	}

	if err := protoChunk.Unmarshal(r.msgBuf[0:msgSize]); err != nil {
		log.Critical("unmarshaling error: ", err)
		panic(err)
	}
}

// small helper function for FeedAlgorithm. It _appends_ chunks to the given slice sliceToFill.
func (r *LegacyTraceDataReader) readChunksIntoSlice(num uint32, protoChunk *dedupProto.Chunk, normalChunk *common.Chunk, sliceToFill *[]common.Chunk) {
	for j := uint32(0); j < num; j++ {
		r.readNextChunk(protoChunk)
		normalChunk.Size = protoChunk.GetCsize()
		normalChunk.Digest = protoChunk.GetFp()
		*sliceToFill = append(*sliceToFill, *normalChunk)
	}
}

func (r *LegacyTraceDataReader) Close() {
	log.Debugf("Shutdown LegacyTraceDataReader for %v", r.filePath)
	for _, ok := <-r.FileEntryReturn; ok; _, ok = <-r.FileEntryReturn {
	}
	r.FileEntryReturn = nil
	r.bigChunkLists = nil
	r.smallChunkLists = nil
}

// Resets the LegacyTraceDataReader. After this call, it is ready for a new
func (r *LegacyTraceDataReader) Reset(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Critical(fmt.Sprintf("could not read directory %v : %v", path, err))
		panic(fmt.Sprintf("could not read directory: %v", err))
	}

	r.filePath = path
	r.fileReader = bufio.NewReaderSize(file, 1*1024*1024) // 1MB buffer size for file

	// cleanup fEntries
	done := false
	for !done {
		select {
		case m := <-r.FileEntryReturn:
			m.Chunks = m.Chunks[:0]
			if m.isPartial {
				r.bigChunkLists = append(r.bigChunkLists, m.Chunks)
			} else {
				r.smallChunkLists = append(r.smallChunkLists, m.Chunks)
			}
		default:
			done = true
		}
	}

	r.numExistingFileEntries = 0

}

func (r *LegacyTraceDataReader) helperChunkSize(buffer []byte, offset int) int64 {

	result := int64(0)
	i := int64(buffer[offset+3])
	result = 256*result + i
	i = int64(buffer[offset+2])
	result = 256*result + i
	i = int64(buffer[offset+1])
	result = 256*result + i
	i = int64(buffer[offset+0])
	result = 256*result + i

	return result
}

func (r *LegacyTraceDataReader) parseChunks(normalChunk *common.Chunk, sliceToFill *[]common.Chunk) (bool, bool) {
	rS, size, _ := r.fileReader.ReadRune()
	_ = size
	recordSize := int32(rS)
	if recordSize == -1 {
		log.Info("Finished chunks. recordsize: ", recordSize)
		return false, false
	}

	if recordSize == 0 {

		r.fileReader.ReadString('\n')

		return false, false
	}

	if recordSize != 24 {

		log.Error("recordsize in parseChunks() != 24. recordsize == ", recordSize)

		return false, true
	}
	buffer := make([]byte, 4)
	k, err := r.fileReader.Read(buffer)
	if err != nil {
		log.Error("Read(buffer) in parseChunks: ", err)
	}
	if k < 4 {
		for dif := 4 - k; dif > 0; dif = 4 - k {
			m, _ := r.fileReader.Read(buffer[k:])
			k = k + m
		}

	}
	chunksize := r.helperChunkSize(buffer, 0)
	if chunksize >= 64*1024 || chunksize < 0 {
		log.Error("Illegal chunksize: ", chunksize)
		return false, false
	} else {
		fp := make([]byte, 20)
		n, err := r.fileReader.Read(fp)
		if err != nil {
			log.Error("Read(buffer) in parseChunks: ", err)
		}
		if n < 20 {
			for dif := 20 - n; dif > 0; dif = 20 - n {
				m, _ := r.fileReader.Read(fp[n:])
				n = n + m
			}
		}
		normalChunk.Digest = make([]byte, 20)
		copy(normalChunk.Digest, fp)
	}
	normalChunk.Size = uint32(chunksize)
	*sliceToFill = append(*sliceToFill, *normalChunk)

	return true, false
}

// Reads from the file, parses all entries and fills the
// given channel with the parsed FileEntries.
func (r *LegacyTraceDataReader) FeedAlgorithm(outputChan chan<- *FileEntry) {
	defer close(outputChan)

	var fEntry *FileEntry
	protoFile := new(dedupProto.File)
	normalChunk := new(common.Chunk)

	parse := true

	log.Info("start feeding")

loop:
	for parse {
		line, err := r.fileReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				parse = false
				break loop
			} else {
				log.Error("parseFileEntry() p.ReadLine() error: ", err)
				parse = false
				break loop
			}
		}
		line = strings.Trim(line, "\n")
		elements := strings.Split(line, "\t")

		if len(elements) != 2 && len(elements) != 3 {
			log.Error("wrong number of elements: ", len(elements))
			parse = false
			break loop
		}

		filename := elements[0]
		filesize, _ := strconv.ParseInt(elements[1], 10, 64)

		var filetype string
		if len(elements) >= 3 {
			filetype = elements[2]
		} else {
			filetype = ""
		}
		protoFile.Filename = proto.String(filename)
		protoFile.Fsize = proto.Uint64(uint64(filesize))
		protoFile.Type = proto.String(filetype)

		var ok bool
		if r.numExistingFileEntries < ConstMaxFileEntries {
			if len(r.smallChunkLists) != 0 {
				fEntry = &FileEntry{Chunks: r.smallChunkLists[len(r.smallChunkLists)-1]}
				r.smallChunkLists = r.smallChunkLists[:len(r.smallChunkLists)-1]
			} else {
				fEntry = &FileEntry{Chunks: make([]common.Chunk, 0, protoFile.GetChunkCount())}
			}
			r.numExistingFileEntries++

		} else {
			fEntry, ok = <-r.FileEntryReturn
			if !ok {
				log.Error("could not receive from FileEntry return channel")
			}
			fEntry.Chunks = fEntry.Chunks[:0]
		}

		chunkcount := 0
		rsf := false
		for ok, _ := r.parseChunks(normalChunk, &fEntry.Chunks); ok; ok, rsf = r.parseChunks(normalChunk, &fEntry.Chunks) {
			chunkcount++
		}

		if rsf {
			log.Infof("Filename: %s, chunkcount: %d, filetype: %s", filename, chunkcount, filetype)
		}

		protoFile.ChunkCount = proto.Uint32(uint32(chunkcount))

		fEntry.TracedFile.Filename = protoFile.GetFilename()
		fEntry.TracedFile.FileSize = protoFile.GetFsize()
		fEntry.TracedFile.FileType = protoFile.GetType()
		fEntry.TracedFile.ChunkCount = protoFile.GetChunkCount()

		outputChan <- fEntry

		fEntry = nil

	}

}

func (r *LegacyTraceDataReader) GetFileEntryReturn() chan *FileEntry {
	return r.FileEntryReturn

}
