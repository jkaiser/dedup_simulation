package analysis

import "os"
import "fmt"
import "bytes"
import "bufio"

/*import "strconv"*/
import "encoding/json"
import log "github.com/cihub/seelog"

import "github.com/jkaiser/dedup_simulations/common"
import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type StreamSimilarityConfig struct {
	DataSet    string
	WindowSize int
}

type TraceSimilarityStatistics struct {
	GenNumber      int
	WindowSize     int
	StreamFinishes []int
	AvgIndexSize   float32
	MaxIndexSize   int
}

// Algorithm for full file duplicates. This represents a naive incrementel backup.
type StreamSimilarityTraceHandler struct {
	chunkIndex map[[12]byte]int

	windowSize          int
	traceOutFilename    string
	statsOutputFilename string
	outFile             *bufio.Writer

	Statistics TraceSimilarityStatistics
}

func NewStreamSimilarityTraceHandler(config *StreamSimilarityConfig, traceOutFilename, statsOutFilename string) *StreamSimilarityTraceHandler {

	if _, err := os.Stat(traceOutFilename); !os.IsNotExist(err) {
		if err = os.Remove(traceOutFilename); err != nil {
			log.Errorf("Couldn't remove result file %v: %v", traceOutFilename, err)
			return nil
		}
	}

	newFile, err := os.Create(traceOutFilename)
	if err != nil {
		log.Criticalf("could not open output file: %v : %v", traceOutFilename, err)
		panic(fmt.Sprintf("could not open output file: %v", err))
	} else {
		log.Infof("opened %v", traceOutFilename)
	}

	if config.WindowSize == 0 {
		log.Error("Invalid configuration: The window size musn't be 0!")
		return nil
	}
	ssh := new(StreamSimilarityTraceHandler)
	ssh.chunkIndex = make(map[[12]byte]int, 1024)
	ssh.windowSize = config.WindowSize
	ssh.statsOutputFilename = statsOutFilename
	ssh.traceOutFilename = traceOutFilename
	ssh.outFile = bufio.NewWriterSize(newFile, 4*1024*1024)
	ssh.Statistics.WindowSize = ssh.windowSize

	return ssh
}

func (ssh *StreamSimilarityTraceHandler) BeginTraceFile(number int) {
	log.Info("begin Generation ", number)
	ssh.Statistics.GenNumber = number
}

func (ssh *StreamSimilarityTraceHandler) EndTraceFile(number int) {
	log.Info("end Generation ", number)

	if encodedStats, err := json.MarshalIndent(ssh.Statistics, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
	}

	// cleanup
	ssh.Statistics = TraceSimilarityStatistics{}
}

func (ssh *StreamSimilarityTraceHandler) Quit() {
	if ssh.statsOutputFilename == "" {
		return
	}

	if encodedStats, err := json.MarshalIndent(ssh.Statistics, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(ssh.statsOutputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Infof("writing to file %v", ssh.statsOutputFilename)
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}

	ssh.outFile.Flush()
}

// takes a range of FileEntry streams and generates chunk streams out of them. There will be one goroutine feeding each
// channel.
func (ssh *StreamSimilarityTraceHandler) generateChunkStreams(fileEntries []<-chan *algocommon.FileEntry, fileEntryReturn []chan<- *algocommon.FileEntry) []chan common.Chunk {

	outStreams := make([]chan common.Chunk, len(fileEntries))

	for i := range fileEntries {
		c := make(chan common.Chunk, 1024)
		outStreams[i] = c

		go func(fileIn <-chan *algocommon.FileEntry, fileOut chan<- *algocommon.FileEntry, chunkOut chan<- common.Chunk) {
			for fileEntry := range fileIn {
				for i := range fileEntry.Chunks {
					chunkOut <- fileEntry.Chunks[i]
				}
				fileOut <- fileEntry
			}
			close(chunkOut)
		}(fileEntries[i], fileEntryReturn[i], c)

		outStreams[i] = c
	}
	return outStreams
}

func (ssh *StreamSimilarityTraceHandler) HandleFiles(fileEntries []<-chan *algocommon.FileEntry, fileEntryReturn []chan<- *algocommon.FileEntry) {

	ssh.Statistics.StreamFinishes = make([]int, len(fileEntries))
	var chunkHashBuf [12]byte // buffer for all chunk hashes. Used to make the Digest useable in maps.
	chunkStreams := ssh.generateChunkStreams(fileEntries, fileEntryReturn)

	if len(chunkStreams) == 0 {
		log.Warn("Got 0 streams as input!")
		return
	} else if len(chunkStreams) != len(fileEntries) {
		log.Errorf("invalid num of chStreams. got: %v, want: %v ", len(chunkStreams), len(fileEntries))
	}

	log.Infof("window size: %v", ssh.windowSize)
	log.Infof("num streams: %v", len(chunkStreams))

	chunkGenerations := make([][][12]byte, ssh.windowSize)
	for i := range chunkGenerations {
		chunkGenerations[i] = make([][12]byte, len(fileEntries))
	}

	var max int
	var accIndexSize int64
	var round int
	var allClosed bool

	for !allClosed {
		allClosed = true

		// remove old generation
		oldGen := chunkGenerations[round%len(chunkGenerations)]
		if round >= ssh.windowSize {
			for _, c := range oldGen {
				if numOccurrences, ok := ssh.chunkIndex[c]; !ok {
					// should never happen
					log.Errorf("Found chunk %x that wasn't in the index", c)
				} else if numOccurrences == 1 {
					delete(ssh.chunkIndex, c)
				} else {
					ssh.chunkIndex[c] = numOccurrences - 1
				}
			}
		}

		// update
		oldGen = oldGen[:0]
		for i := range chunkStreams {
			if chunk, ok := <-chunkStreams[i]; ok {
				allClosed = false

				copy(chunkHashBuf[:], chunk.Digest)
				oldGen = append(oldGen, chunkHashBuf)

				// update index
				numOccurences := ssh.chunkIndex[chunkHashBuf] // num... is 0 if there is no such entry
				ssh.chunkIndex[chunkHashBuf] = numOccurences + 1

			} else if ssh.Statistics.StreamFinishes[i] == 0 { // stream/channel just got empty
				log.Infof("stream %v finished at round %v", i, round)
				ssh.Statistics.StreamFinishes[i] = round
			}
		}
		chunkGenerations[round%len(chunkGenerations)] = oldGen

		/*log.Info(fmt.Sprintln(len(ssh.chunkIndex)))*/
		if _, err := ssh.outFile.WriteString(fmt.Sprintln(len(ssh.chunkIndex))); err != nil {
			log.Errorf("error while writing trace: %v", err)
			return
		}
		/*ssh.outFile.WriteString(fmt.Sprintln(len(ssh.chunkIndex)))*/
		// collect statistics
		accIndexSize += int64(len(ssh.chunkIndex))
		if len(ssh.chunkIndex) > (ssh.windowSize * len(fileEntries)) {
			log.Criticalf("detected more chunks than possible in round %v with %v streams! have: %v, theoretical max: %v", round, len(chunkStreams), len(ssh.chunkIndex), ssh.windowSize*len(fileEntries))
		}
		if len(ssh.chunkIndex) > max {
			max = len(ssh.chunkIndex)
		}
		round++
	}

	ssh.Statistics.MaxIndexSize = max
	ssh.Statistics.AvgIndexSize = float32(float64(accIndexSize) / float64(round))
}
