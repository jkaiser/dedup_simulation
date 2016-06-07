package common

import "os"
import "fmt"
import "io"
import "bufio"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/common"
import "github.com/jkaiser/dedup_simulations/de_pc2_dedup_fschunk"
import "github.com/gogo/protobuf/proto"

type ProtoTraceDataReader struct {
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

func NewProtoTraceDataReader(path string) *ProtoTraceDataReader {

	file, err := os.Open(path)
	if err != nil {
		log.Critical(fmt.Sprintf("could not read directory '%v' : %v", path, err))
		panic(fmt.Sprintf("could not read directory '%v' : %v", path, err))
	}

	/*return &ProtoTraceDataReader{path, bufio.NewReaderSize(file, 4*1024*1024), make([]byte, 10), make([]byte, 1024), make(chan *common.Chunk, 1000), make(chan *FileEntry, 1000)} // 4MB buffer size for file*/
	return &ProtoTraceDataReader{numExistingFileEntries: 0,
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
func (r *ProtoTraceDataReader) getNextMessageSize() uint64 {

	if buf, err := r.fileReader.Peek(len(r.msgSizeBuf)); (len(buf) == 0) || ((err != nil) && (err != io.EOF)) {
		if (err != nil) && (err == io.EOF) {
			log.Debug("EOF for ", r.filePath)
			return 0
		} else {
			log.Errorf(fmt.Sprintf("could not read next msg size: %v", err))
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
func (r *ProtoTraceDataReader) readNextFile(protoFile *de_pc2_dedup_fschunk.File) uint64 {

	protoFile.Reset()

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

func (r *ProtoTraceDataReader) readNextChunk(protoChunk *de_pc2_dedup_fschunk.Chunk) {

	protoChunk.Reset()

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
func (r *ProtoTraceDataReader) readChunksIntoSlice(num uint32, protoChunk *de_pc2_dedup_fschunk.Chunk, normalChunk *common.Chunk, sliceToFill *[]common.Chunk) {
	for j := uint32(0); j < num; j++ {
		r.readNextChunk(protoChunk)
		normalChunk.Size = protoChunk.GetCsize()
		normalChunk.Digest = protoChunk.GetFp()
		*sliceToFill = append(*sliceToFill, *normalChunk)
	}
}

func (r *ProtoTraceDataReader) Close() {
	log.Debugf("Shutdown ProtoTraceDataReader for %v", r.filePath)
	for _, ok := <-r.FileEntryReturn; ok; _, ok = <-r.FileEntryReturn {
	}
	r.FileEntryReturn = nil
	r.bigChunkLists = nil
	r.smallChunkLists = nil
}

// Resets the ProtoTraceDataReader. After this call, it is ready for a new
func (r *ProtoTraceDataReader) Reset(path string) {
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

// Reads from the file, parses all entries and fills the
// given channel with the parsed FileEntries.
func (r *ProtoTraceDataReader) FeedAlgorithm(outputChan chan<- *FileEntry) {
	defer close(outputChan)

	var msgSize uint64
	var fEntry *FileEntry
	protoFile := new(de_pc2_dedup_fschunk.File)
	protoChunk := new(de_pc2_dedup_fschunk.Chunk)
	normalChunk := new(common.Chunk)

	log.Debug("start feeding")
	for {
		if msgSize = r.readNextFile(protoFile); msgSize == 0 {
			break
		}

		var ok bool
		if protoFile.GetPartial() {
			if slice, ok := r.bigFilesMap[protoFile.GetFilename()]; ok {
				r.readChunksIntoSlice(protoFile.GetChunkCount(), protoChunk, normalChunk, &slice)
				r.bigFilesMap[protoFile.GetFilename()] = slice
			} else {
				var s []common.Chunk
				if len(r.bigChunkLists) != 0 {
					s = r.bigChunkLists[len(r.bigChunkLists)-1]
					r.bigChunkLists = r.bigChunkLists[:len(r.bigChunkLists)-1]
				} else {
					s = make([]common.Chunk, 0, protoFile.GetChunkCount()*2)
				}
				r.readChunksIntoSlice(protoFile.GetChunkCount(), protoChunk, normalChunk, &s)
				r.bigFilesMap[protoFile.GetFilename()] = s
			}
		} else {
			// get next FileEntry
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

			switch s, ok := r.bigFilesMap[protoFile.GetFilename()]; {
			case !fEntry.isPartial && !ok:
				// no need to switch lists
				r.readChunksIntoSlice(protoFile.GetChunkCount(), protoChunk, normalChunk, &fEntry.Chunks)

			case !fEntry.isPartial && ok:
				// list in fEntry is small, but big needed -> switch to big
				fEntry.isPartial = true
				r.smallChunkLists = append(r.smallChunkLists, fEntry.Chunks[:0])
				fEntry.Chunks = s
				r.readChunksIntoSlice(protoFile.GetChunkCount(), protoChunk, normalChunk, &fEntry.Chunks)

			case fEntry.isPartial && !ok:
				// list in fEntry is big, but small needed -> switch to small
				fEntry.isPartial = false
				r.bigChunkLists = append(r.bigChunkLists, fEntry.Chunks[:0])
				if len(r.smallChunkLists) == 0 {
					fEntry.Chunks = make([]common.Chunk, 0, protoFile.GetChunkCount())
				} else {
					fEntry.Chunks = r.smallChunkLists[len(r.smallChunkLists)-1]
					r.smallChunkLists = r.smallChunkLists[:len(r.smallChunkLists)-1]
				}
				r.readChunksIntoSlice(protoFile.GetChunkCount(), protoChunk, normalChunk, &fEntry.Chunks)

			case fEntry.isPartial && ok:
				// list in fEntry is big and big is needed, -> use already filled one and store other big
				r.bigChunkLists = append(r.bigChunkLists, fEntry.Chunks[:0])
				fEntry.Chunks = s
				r.readChunksIntoSlice(protoFile.GetChunkCount(), protoChunk, normalChunk, &fEntry.Chunks)
			}

			/*// check whether it is the last part of a partial file*/
			/*if s, ok := r.bigFilesMap[protoFile.GetFilename()]; ok {*/
			/*r.readChunksIntoSlice(protoFile.GetChunkCount(), protoChunk, normalChunk, &s)*/
			/*r.bigFilesMap[protoFile.GetFilename()] = s*/
			/*} else {*/
			/*r.readChunksIntoSlice(protoFile.GetChunkCount(), protoChunk, normalChunk, &fEntry.Chunks)*/
			/*}*/

			fEntry.TracedFile.Filename = protoFile.GetFilename()
			fEntry.TracedFile.FileSize = protoFile.GetFsize()
			fEntry.TracedFile.FileType = protoFile.GetType()
			fEntry.TracedFile.FileLabel = protoFile.GetLabel()
			fEntry.TracedFile.ChunkCount = protoFile.GetChunkCount()

		}

		switch _, ok := r.bigFilesMap[protoFile.GetFilename()]; {
		case !protoFile.GetPartial() && !ok:
			outputChan <- fEntry
		case !protoFile.GetPartial() && ok:
			outputChan <- fEntry
			delete(r.bigFilesMap, protoFile.GetFilename())
		}

		fEntry = nil
	}

}

func (r *ProtoTraceDataReader) GetFileEntryReturn() chan *FileEntry {
	return r.FileEntryReturn

}
