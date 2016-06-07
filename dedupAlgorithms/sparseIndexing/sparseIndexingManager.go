package sparseIndexing

//import "os"
import "sync"
import "bytes"
import "strings"
import "strconv"
import "io/ioutil"
import "encoding/json"

import log "github.com/cihub/seelog"
import "github.com/bradfitz/gomemcache/memcache"
import "github.com/jkaiser/dedup_simulations/common"
import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

// Kind of father struct which for creating and managing SamplingBlockIndexFilter instances.
type SparseIndexingManager struct {
	avgChunkSize      int
	chunkIndices      []map[[12]byte]struct{}
	sparseIndex       *SparseIndex
	chunkIndexRWLocks []sync.RWMutex

	clientServers string
	mcClient      *memcache.Client

	config         SparseIndexingConfig
	outputFilename string
	ioTraceFile    string
	ioChan         chan string // channel for each handler to send ioTrace entries into
	ioCloseChan    chan bool   // channel to signal finished ioTrace goroutine

	sparseIndexingMap map[string]*SparseIndexing
	traceReaderMap    map[string]algocommon.TraceDataReader

	TotalStatsList []SparseIndexingStatistics            // holds the summarized statistics for each gen
	StatsList      []map[string]SparseIndexingStatistics // holds the statistics of all _used_ blcs after each gen.
}

func NewSparseIndexingManager(config SparseIndexingConfig, avgChunkSize int, clientServers string, outFilename string, ioTraceFile string) *SparseIndexingManager {
	sm := new(SparseIndexingManager)
	sm.config = config
	sm.avgChunkSize = avgChunkSize

	sm.chunkIndices = make([]map[[12]byte]struct{}, 256)
	for i := range sm.chunkIndices {
		sm.chunkIndices[i] = make(map[[12]byte]struct{}, 1E5)
	}

	sm.chunkIndexRWLocks = make([]sync.RWMutex, 256)
	sm.clientServers = clientServers
	sm.outputFilename = outFilename
	sm.ioTraceFile = ioTraceFile
	sm.sparseIndexingMap = make(map[string]*SparseIndexing)
	sm.traceReaderMap = make(map[string]algocommon.TraceDataReader)
	return sm
}

func (sm *SparseIndexingManager) Init() bool {

	if sm.ioTraceFile != "" {
		sm.ioCloseChan = make(chan bool)
		sm.ioChan = make(chan string, 100000)
		if sm.ioTraceFile[len(sm.ioTraceFile)-2:] != "gz" {
			go common.WriteStringsCompressed(sm.ioTraceFile+".gz", sm.ioChan, sm.ioCloseChan)
		} else {
			go common.WriteStringsCompressed(sm.ioTraceFile, sm.ioChan, sm.ioCloseChan)
		}
	}

	sm.sparseIndex = NewSparseIndex(16*1024*1024, sm.avgChunkSize, sm.config.CacheSize, sm.config.SampleFactor, sm.config.SegmentSize, sm.config.MaxHooksPerSegment, sm.ioChan)

	var ret bool = true
	if len(sm.clientServers) > 0 {
		if sm.mcClient, ret = common.MemClientInit(sm.clientServers); ret {
			sm.sparseIndex.setIndex(sm.mcClient)
		}
	}
	return ret
}

// Creates a new SamplingBlockIndexFilter instance. All instances creates by this function share the
// same ChunkIndex and BlockIndex.
func (sm *SparseIndexingManager) CreateSparseIndexing() *SparseIndexing {

	return NewSparseIndexing(sm.config,
		sm.avgChunkSize,
		sm.chunkIndices,
		sm.chunkIndexRWLocks,
		sm.sparseIndex,
		sm.outputFilename)
}

// collects the statistics of all BLC instances and returns a summary as well as a list of all statistics.
func (sm *SparseIndexingManager) collectStatistic(sparseIndices map[string]*SparseIndexing) (SparseIndexingStatistics, map[string]SparseIndexingStatistics) {

	stats := make(map[string]SparseIndexingStatistics)
	for host, si := range sparseIndices {
		stats[host] = si.StatsList[len(si.StatsList)-1]
	}

	var totalStats SparseIndexingStatistics = NewSparseIndexingStatistics()

	for _, s := range stats {
		totalStats.add(s)
	}

	totalStats.SparseIndexStats.finish()
	totalStats.ChunkIndexStats.Finish()
	totalStats.finish()

	return totalStats, stats
}

func (sm *SparseIndexingManager) RunGeneration(genNum int, fileInfoList []algocommon.TraceFileInfo) {
	wg := new(sync.WaitGroup) // to wait for all handlers to finish
	wg.Add(len(fileInfoList))

	sameMomentStartWG := new(sync.WaitGroup) // barrier to let all handlers start at the same time
	sameMomentStartWG.Add(len(fileInfoList))

	sm.sparseIndex.beginNewGeneration()
	usedSparseIndices := make(map[string]*SparseIndexing, 0)

	for _, fileInfo := range fileInfoList {

		var hostname string

		// start new generation and create data structures if necesary
		if strings.Contains(sm.config.DataSet, "multi") { // if parallel run, determine the hostname
			var splits []string = strings.Split(fileInfo.Name, "_")
			hostname = strings.Split(splits[len(splits)-1], "-")[0]
		} else {
			hostname = "host0"
		}
		log.Info("gen ", genNum, ": processing host: ", hostname)

		if _, ok := sm.sparseIndexingMap[hostname]; !ok {
			sm.sparseIndexingMap[hostname] = sm.CreateSparseIndexing()
		}

		si := sm.sparseIndexingMap[hostname]
		usedSparseIndices[hostname] = si
		si.BeginTraceFile(genNum, fileInfo.Name)

		// si instance is ready
		go func(gen int, siHandler *SparseIndexing, tracePath string) {
			defer wg.Done()
			fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)

			log.Info("gen ", gen, ": generate TraceDataReader for path: ", fileInfo.Path)
			tReader := algocommon.NewTraceDataReader(tracePath)
			go tReader.FeedAlgorithm(fileEntryChan)

			sameMomentStartWG.Done() // synchronize the start of every file backup
			sameMomentStartWG.Wait()

			siHandler.HandleFiles(fileEntryChan, tReader.GetFileEntryReturn())
			siHandler.EndTraceFile()
		}(genNum, si, fileInfo.Path)
	}
	wg.Wait()

	totalStats, siStats := sm.collectStatistic(usedSparseIndices)
	siStats["total"] = totalStats
	sm.StatsList = append(sm.StatsList, siStats)

	// print statistics
	if encodedStats, err := json.MarshalIndent(siStats, "", "    "); err != nil {
		log.Error("Couldn't marshal generation statistics: ", err)
	} else {
		log.Info("Generation ", genNum, ": Stats of all used SparseIndexes:")
		log.Info(bytes.NewBuffer(encodedStats).String())
	}
	if encodedStats, err := json.MarshalIndent(totalStats, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info("Generation ", genNum, ": Summarized statistics:")
		log.Info(bytes.NewBuffer(encodedStats).String())
	}
}

// Writes the final statistics and cleans up if necesary
func (sm *SparseIndexingManager) Quit() {

	if sm.ioCloseChan != nil {
		close(sm.ioChan)
		log.Info("wait for ioTraceRoutine")
		<-sm.ioCloseChan
	}

	if sm.outputFilename == "" {
		return
	}

	jsonMap := make(map[string]interface{})
	for i, generationStats := range sm.StatsList {
		jsonMap["generation "+strconv.Itoa(i)] = generationStats
	}

	if encodedStats, err := json.MarshalIndent(jsonMap, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
		//} else if err := ioutil.WriteFile(sm.outputFilename, encodedStats, os.O_RDWR); err != nil {
	} else if err := ioutil.WriteFile(sm.outputFilename, encodedStats, 0777); err != nil {
		log.Error("Couldn't write results file: ", err)
		//} else if f, err := os.Create(sm.outputFilename); err != nil {
		//log.Error("Couldn't create results file: ", err)
		//} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		//log.Error("Couldn't write to results file: ", err)
		//} else if err := f.Sync(); err != nil {
		//log.Error("Couldn't sync to results file: ", err)
		//} else if err := f.Close(); err != nil {
		//log.Error("Couldn't close file of results file: ", err)
		//log.Info(bytes.NewBuffer(encodedStats).String())
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
	}
}
