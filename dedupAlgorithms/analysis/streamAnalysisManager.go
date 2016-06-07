package analysis

import "os"
import "sync"
import "bytes"
import "strings"
import "strconv"
import "encoding/json"

import log "github.com/cihub/seelog"
import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

type StreamStatistics struct {
	RealUniquesCount                    int64
	SharedUniquesCount                  int64
	RelRealUniquesToGlobalUniques       float32
	RelRealUniquesToStreamUniques       float32
	RelRealUniquesVolumeToStreamUniques float32
}

type StreamSimilarityStatistics struct {
	AllUniquesCount int

	// number of chunks that are 1) unique in their stream and 2) appear in _all_ streams. That is, these
	//chunks appear in all data streams.
	FullSharedUniquesCount       int
	RelFullSharedToGlobalUniques float32

	// number of chunks that are 1) unique in their stream and 2) appear in at least one other stream.
	// That is, these chunks are shared by at least two streams.
	AnySharedUniquesCount       int64
	RelAnySharedToGlobalUniques float32

	RelAnySharedVolumeToGlobalUniques float32
	StreamStats                       map[string]StreamStatistics
	ChunkUsageCDF                     []uint32
}

func NewStreamSimilarityStatistics() *StreamSimilarityStatistics {
	return &StreamSimilarityStatistics{StreamStats: make(map[string]StreamStatistics)}
}

// Kind of father struct which for creating and managing StreamAnalysisHandler instances.
type StreamAnalysisManager struct {
	chunkIndexRWLock *sync.RWMutex

	config         StreamAnalysisConfig
	outputFilename string

	/*traceReaderMap map[string]*TraceDataReader*/

	SimStatsList   []StreamSimilarityStatistics // holds the similarity statistics for each gen
	TotalStatsList []StreamAnalysisStatistics
	StatsList      []map[string]StreamAnalysisStatistics // holds the statistics of all _used_ sas after each gen.
}

func NewStreamAnalysisManager(config StreamAnalysisConfig, outFilename string) *StreamAnalysisManager {
	sm := new(StreamAnalysisManager)
	sm.config = config
	sm.outputFilename = outFilename
	/*sm.traceReaderMap = make(map[string]*TraceDataReader)*/
	return sm
}

// Creates a new StreamAnalysisHandler instance. All instances creates by this function share the
// same ChunkIndex and BlockIndex.
func (sm *StreamAnalysisManager) CreateBLockIndexFilter() *StreamAnalysisHandler {
	sa := NewStreamAnalysisHandler(sm.outputFilename)
	return sa
}

func (sm *StreamAnalysisManager) RunGeneration(genNum int, fileInfoList []common.TraceFileInfo) {
	wg := new(sync.WaitGroup)
	wg.Add(len(fileInfoList))

	usedSAs := make(map[string]*StreamAnalysisHandler, 0)

	for _, fileInfo := range fileInfoList {

		var hostname string

		// start new generation and create/prepare data structures if necesary
		if strings.Contains(sm.config.DataSet, "multi") { // if not parallel run
			var splits []string = strings.Split(fileInfo.Name, "_")
			hostname = strings.Split(splits[len(splits)-1], "-")[0]
		} else {
			hostname = "host0"
		}
		log.Info("gen ", genNum, ": processing host: ", hostname)

		var sa *StreamAnalysisHandler = sm.CreateBLockIndexFilter()
		usedSAs[hostname] = sa
		sa.BeginTraceFile(genNum, fileInfo.Name)

		var traceReader common.TraceDataReader = common.NewTraceDataReader(fileInfo.Path)
		/*sm.traceReaderMap[hostname] = traceReader*/

		// sa instance is ready
		go func(gen int, tReader common.TraceDataReader) {
			defer wg.Done()
			fileEntryChan := make(chan *common.FileEntry, common.ConstMaxFileEntries)
			go tReader.FeedAlgorithm(fileEntryChan)
			sa.HandleFiles(fileEntryChan, tReader.GetFileEntryReturn())
			sa.EndTraceFile()
		}(genNum, traceReader)
	}
	wg.Wait()

	simStats, totalStats, saStats := sm.collectStatistic(usedSAs)
	sm.SimStatsList = append(sm.SimStatsList, *simStats)
	sm.TotalStatsList = append(sm.TotalStatsList, *totalStats)
	sm.StatsList = append(sm.StatsList, saStats)

	// print statistics
	if encodedStats, err := json.MarshalIndent(saStats, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info("Generation ", genNum, ": Stats of all used sas:")
		log.Info(bytes.NewBuffer(encodedStats).String())
	}
	if encodedStats, err := json.MarshalIndent(totalStats, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info("Generation ", genNum, ": Summarized statistics:")
		log.Info(bytes.NewBuffer(encodedStats).String())
	}
	if encodedStats, err := json.MarshalIndent(simStats, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info("Generation ", genNum, ": Similarity statistics:")
		log.Info(bytes.NewBuffer(encodedStats).String())
	}
}

// Writes the final statistics and cleans up if necesary
func (sm *StreamAnalysisManager) Quit() {

	if sm.outputFilename == "" {
		return
	}

	jsonMap := make(map[string]interface{})
	for i := range sm.StatsList {

		/*jsonMap["generation "+strconv.Itoa(i)] = generationStats*/
		jsonMap["generation "+strconv.Itoa(i)] = map[string]interface{}{"similarity": sm.SimStatsList[i], "streams": sm.StatsList[i], "total": sm.TotalStatsList[i]}
	}

	if encodedStats, err := json.MarshalIndent(jsonMap, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(sm.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}

// collects the statistics of all sa instances and returns a summary as well as a list of all statistics.
func (sm *StreamAnalysisManager) collectStatistic(sas map[string]*StreamAnalysisHandler) (*StreamSimilarityStatistics, *StreamAnalysisStatistics, map[string]StreamAnalysisStatistics) {

	stats := make(map[string]StreamAnalysisStatistics)
	for host, sa := range sas {
		stats[host] = sa.StatsList[len(sa.StatsList)-1]
	}

	totalStats := new(StreamAnalysisStatistics)

	for _, s := range stats {
		totalStats.add(s)
	}
	totalStats.finish()

	// similarity
	cindices := make([]map[[12]byte]uint32, 0)
	for _, h := range sas {
		cindices = append(cindices, h.chunkIndex)
	}

	// build stats based on intersection of all streams
	intersection := sm.computeIntersection(cindices)

	simStats := NewStreamSimilarityStatistics()
	simStats.FullSharedUniquesCount = len(intersection)
	for k := range intersection {
		delete(intersection, k)
	}
	intersection = nil // give the gc the chance to clean up

	// build stats based on union of all streams
	union := sm.computeUnion(cindices)
	simStats.AllUniquesCount = len(union)
	simStats.RelFullSharedToGlobalUniques = float32(float64(simStats.FullSharedUniquesCount) / float64(len(union)))
	totalStats.UniqueChunkCount = len(union)
	totalStats.RedundantChunkCount = totalStats.ChunkCount - totalStats.UniqueChunkCount
	totalStats.RedundantChunksRatio = float32(float64(totalStats.RedundantChunkCount) / float64(totalStats.ChunkCount))

	var allChunksVol uint32
	for k, numOccurrences := range union {
		allChunksVol += numOccurrences
		delete(union, k)
	}
	union = nil

	simStats.ChunkUsageCDF = sm.computeChunkUsage(cindices)

	// build stats of real uniques per streams (= uniques that only exist in the particular stream)
	uniqueStats := sm.computeUniqueSets(sas)
	simStats.AnySharedUniquesCount = 0

	var anySharedUniquesOccurrences int64
	for stream, stats := range uniqueStats {
		simStats.AnySharedUniquesCount += stats["sharedUniquesSetSize"]
		anySharedUniquesOccurrences += stats["sharedUniquesOccurrencesCount"]

		simStats.StreamStats[stream] = StreamStatistics{
			RealUniquesCount:                    stats["uniquesSetSize"],
			SharedUniquesCount:                  stats["sharedUniquesSetSize"],
			RelRealUniquesToStreamUniques:       float32(float64(stats["uniquesSetSize"]) / float64(len(sas[stream].chunkIndex))),
			RelRealUniquesToGlobalUniques:       float32(float64(stats["uniquesSetSize"]) / float64(simStats.AllUniquesCount)),
			RelRealUniquesVolumeToStreamUniques: float32(float64(stats["uniquesOccurrencesCount"]) / float64(stats["chunksOccurrencesCount"])),
		}
	}

	simStats.RelAnySharedToGlobalUniques = float32(float64(simStats.AnySharedUniquesCount) / float64(simStats.AllUniquesCount))
	simStats.RelAnySharedVolumeToGlobalUniques = float32(float64(anySharedUniquesOccurrences) / float64(allChunksVol))

	return simStats, totalStats, stats
}

func (sm *StreamAnalysisManager) computeIntersection(sets []map[[12]byte]uint32) map[[12]byte]uint32 {
	log.Infof("compute intersection of %v sets", len(sets))
	if len(sets) == 0 {
		return make(map[[12]byte]uint32)
	}

	intersec := make(map[[12]byte]uint32, len(sets[0]))
	for chunk, numOccurrences := range sets[0] {
		intersec[chunk] = numOccurrences
	}
	sets = sets[1:]

	for len(sets) > 0 {
		s := sets[0]
		for ch, numOccurences := range intersec {
			if cnt, ok := s[ch]; ok {
				intersec[ch] = numOccurences + cnt
			} else {
				delete(intersec, ch)
			}
		}

		sets = sets[1:]
	}

	return intersec
}

func (sm *StreamAnalysisManager) computeChunkUsage(sets []map[[12]byte]uint32) []uint32 {
	log.Infof("compute chunk usage of %v sets", len(sets))
	if len(sets) == 0 {
		return make([]uint32, 0)
	}

	var max byte = 1
	union := make(map[[12]byte]byte, len(sets[0]))
	for len(sets) > 0 {
		s := sets[0]
		for ch, _ := range s {
			if numOccurrences, ok := union[ch]; ok {
				union[ch] = byte(numOccurrences) + 1
				if numOccurrences+byte(1) > max {
					max = numOccurrences + 1
				}
			} else {
				union[ch] = 1
			}
		}

		sets = sets[1:]
	}

	cusage := make([]uint32, max)
	for _, numOccurrences := range union {
		cusage[numOccurrences-1]++
	}

	return cusage
}

func (sm *StreamAnalysisManager) computeUnion(sets []map[[12]byte]uint32) map[[12]byte]uint32 {
	log.Infof("compute union of %v sets", len(sets))
	if len(sets) == 0 {
		return make(map[[12]byte]uint32)
	}

	union := make(map[[12]byte]uint32, len(sets[0]))
	for len(sets) > 0 {
		s := sets[0]
		for ch, numOccurences := range s {
			if cnt, ok := union[ch]; ok {
				union[ch] = numOccurences + cnt
			} else {
				union[ch] = numOccurences
			}
		}

		sets = sets[1:]
	}

	return union
}

func (sm *StreamAnalysisManager) computeUniqueSets(saMap map[string]*StreamAnalysisHandler) map[string]map[string]int64 {
	log.Infof("compute uniques of %v sets", len(saMap))
	if len(saMap) == 0 {
		return nil
	}

	streamStatistics := make(map[string]map[string]int64)
	/*uniqueSetSizes := make(map[string]int)*/

	for stream, handler := range saMap {
		streamStats := make(map[string]int64)
		var sharedUniquesCount int64 = 0
		var sharedUniquesOccurrences int64 = 0
		var allOccurrences int64 = 0

		// compute the set of real uniques in the current stream
		uniques := make(map[[12]byte]int, len(handler.chunkIndex))
		for chunk, numOccurrences := range handler.chunkIndex {
			uniques[chunk] = int(numOccurrences)
			allOccurrences += int64(numOccurrences)
		}

		for s, h := range saMap {
			if stream == s {
				continue
			}

			for fp := range h.chunkIndex {
				if occurrences, ok := uniques[fp]; ok {
					delete(uniques, fp)
					sharedUniquesCount++
					sharedUniquesOccurrences += int64(occurrences)
				}
			}
		}

		streamStats["uniquesSetSize"] = int64(len(uniques))
		streamStats["sharedUniquesSetSize"] = sharedUniquesCount
		streamStats["chunksOccurrencesCount"] = allOccurrences
		streamStats["uniquesOccurrencesCount"] = allOccurrences - sharedUniquesOccurrences
		streamStats["sharedUniquesOccurrencesCount"] = sharedUniquesOccurrences
		streamStatistics[stream] = streamStats
	}

	return streamStatistics
}
