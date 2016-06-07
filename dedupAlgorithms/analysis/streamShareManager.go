package analysis

import "os"
import "sync"
import "bytes"
import "strings"
import "encoding/binary"
import log "github.com/cihub/seelog"
import "encoding/json"

import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type SetArray struct {
	Set []string
}

func NewSetArray(avgNumStreams int) *SetArray {
	return &SetArray{Set: make([]string, 0, avgNumStreams)}
}
func (bm *SetArray) Size() int { return len(bm.Set) }
func (bm *SetArray) Add(id string) {
	for i := range bm.Set {
		if bm.Set[i] == id {
			return
		}
	}
	bm.Set = append(bm.Set, id)
}
func (bm *SetArray) Contains(id string) bool {
	for index, _ := range bm.Set {
		if bm.Set[index] == id {
			return true
		}
	}
	return false
}

type StreamShareStatistics struct {
	NumStreamsGen      int
	NumStreamsTotal    int
	ChunkUsageCDFCount []int64
	ChunkUsageCDFRel   []float64
}

// Kind of father struct that manages the execution of several StreamSimilarity instances
type StreamShareManager struct {
	config            StreamSimilarityConfig
	finalStatsOut     string
	seenStreams       map[string]struct{}
	maxIndices        int64
	chunkIndices      []map[[12]byte]*SetArray
	chunkIndexRWLocks []sync.RWMutex
	StatsList         []*StreamShareStatistics // holds the statistics of all _used_ blcs after each gen.
}

func NewStreamShareManager(config StreamSimilarityConfig, finalStatsOut string) *StreamShareManager {
	s := new(StreamShareManager)
	s.config = config
	s.finalStatsOut = finalStatsOut

	s.maxIndices = 1024
	s.chunkIndices = make([]map[[12]byte]*SetArray, s.maxIndices)
	for i := range s.chunkIndices {
		s.chunkIndices[i] = make(map[[12]byte]*SetArray, 1E5)
	}
	s.chunkIndexRWLocks = make([]sync.RWMutex, s.maxIndices)

	s.seenStreams = make(map[string]struct{})
	s.StatsList = make([]*StreamShareStatistics, 0)
	return s
}

func (cm *StreamShareManager) RunGeneration(genNum int, fileInfoList []algocommon.TraceFileInfo) {
	wg := new(sync.WaitGroup)
	wg.Add(len(fileInfoList))

	for _, fileInfo := range fileInfoList {

		var hostname string

		// start new generation and create data structures if necesary
		if strings.Contains(cm.config.DataSet, "multi") { // if not parallel run
			var splits []string = strings.Split(fileInfo.Name, "_")
			hostname = strings.Split(splits[len(splits)-1], "-")[0]
		} else {
			hostname = "host0"
		}
		log.Info("gen ", genNum, ": processing host: ", hostname)

		cm.seenStreams[hostname] = struct{}{}

		go func(gen int, tracePath string, host string) {
			defer wg.Done()
			var chunkHashBuf [12]byte
			var zeroChunk [12]byte // per default filled with zeros
			var indexPos uint64
			fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)

			log.Info("gen ", gen, ": generate TraceDataReader for trace: ", tracePath)
			tReader := algocommon.NewTraceDataReader(tracePath)
			go tReader.FeedAlgorithm(fileEntryChan)

			b := bytes.NewBuffer(nil)
			for fileEntry := range fileEntryChan {
				if len(fileEntry.Chunks) > 0 {

					for i := range fileEntry.Chunks {
						copy(chunkHashBuf[:], fileEntry.Chunks[i].Digest)

						// skip zero chunk. Actually I should also count this one, but since it is one of millions, it
						// does not significanly change the result, but speeds up the first phase if ordered streams are
						// the input.
						if bytes.Equal(zeroChunk[:], chunkHashBuf[:]) {
							cm.chunkIndexRWLocks[indexPos].Unlock()
							continue
						}

						b.Reset()
						b.Write(chunkHashBuf[:])
						if err := binary.Read(b, binary.LittleEndian, &indexPos); err != nil {
							log.Errorf("Couldn't read %v bytes (%x) into integer: %v", b.Len(), chunkHashBuf, err)
						}
						indexPos = indexPos % uint64(cm.maxIndices)

						cm.chunkIndexRWLocks[indexPos].Lock()
						set, chunkExists := cm.chunkIndices[indexPos][chunkHashBuf]
						switch {
						case chunkExists && (set != nil):
							if !set.Contains(host) {
								set.Add(host)
							}

						case chunkExists && (set == nil):
							s := NewSetArray(2)
							s.Add(host)
							cm.chunkIndices[indexPos][chunkHashBuf] = s

						case !chunkExists:
							s := NewSetArray(2)
							s.Add(host)
							cm.chunkIndices[indexPos][chunkHashBuf] = s

						default:
							log.Errorf("Unkown case: chunkExists=%v, set=%v", chunkExists, set)
						}
						cm.chunkIndexRWLocks[indexPos].Unlock()
					} // end chunk for loop
				}
				tReader.GetFileEntryReturn() <- fileEntry
			}

		}(genNum, fileInfo.Path, hostname)
	}
	wg.Wait()

	// collect stats
	log.Info("Collecting statistics")
	ccStats := cm.collectStatistic()
	ccStats.NumStreamsGen = len(fileInfoList)
	ccStats.NumStreamsTotal = len(cm.seenStreams)
	cm.StatsList = append(cm.StatsList, ccStats)

	/*// print statistics*/
	if encodedStats, err := json.MarshalIndent(ccStats, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info("Generation ", genNum, ": Stats of this gen:")
		log.Info(bytes.NewBuffer(encodedStats).String())
	}
}

func (cm *StreamShareManager) collectStatistic() *StreamShareStatistics {

	usageCDF := make([]int64, len(cm.seenStreams)+1)
	for _, ci := range cm.chunkIndices {
		for _, set := range ci {
			usageCDF[set.Size()]++
		}
	}

	var sum int64
	for i := range usageCDF {
		sum += usageCDF[i]
	}

	usageCDFRel := make([]float64, len(usageCDF))
	for i := range usageCDF {
		usageCDFRel[i] = float64(usageCDF[i]) / float64(sum)
	}

	stats := new(StreamShareStatistics)
	stats.ChunkUsageCDFCount = usageCDF
	stats.ChunkUsageCDFRel = usageCDFRel

	log.Infof("usage CDF: %v, rel: %v", usageCDF, usageCDFRel)
	return stats
}

// Writes the final statistics and cleans up if necesary
func (ssm *StreamShareManager) Quit() {

	if ssm.finalStatsOut == "" {
		return
	}

	if encodedStats, err := json.MarshalIndent(ssm.StatsList, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(ssm.finalStatsOut); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}
