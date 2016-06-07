package sortedChunkIndexing

import "os"
import "sync"
import "bytes"
import "strings"
import "strconv"
import "encoding/json"

import log "github.com/cihub/seelog"
import "github.com/bradfitz/gomemcache/memcache"
import "github.com/jkaiser/dedup_simulations/common"
import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

// Kind of father struct which for creating and managing SortedChunkIndexing instances.
type SortedChunkIndexingManager struct {
	avgChunkSize int
	chunkIndex   *SortedChunkIndex

	clientServers string
	mcClient      *memcache.Client

	config         SortedChunkIndexingConfig
	outputFilename string
	ioTraceFile    string
	ioChan         chan string // channel for each handler to send ioTrace entries into
	ioCloseChan    chan bool   // channel to signal finished ioTrace goroutine

	scMap                     map[string]*SortedChunkIndexing
	traceReaderMap            map[string]algocommon.TraceDataReader
	lastContainerIdOfFirstGen uint32 // used for flushing local containers to memcache

	TotalStatsList []SortedChunkIndexingStatistics            // holds the summarized statistics for each gen
	StatsList      []map[string]SortedChunkIndexingStatistics // holds the statistics of all _used_ ccs after each gen.
}

func NewSortedChunkIndexingManager(config SortedChunkIndexingConfig, avgChunkSize int, clientServers string, outFilename string, ioTraceFile string) *SortedChunkIndexingManager {
	cm := new(SortedChunkIndexingManager)
	cm.config = config
	cm.avgChunkSize = avgChunkSize
	cm.clientServers = clientServers

	cm.lastContainerIdOfFirstGen = 0

	cm.outputFilename = outFilename
	cm.ioTraceFile = ioTraceFile
	cm.scMap = make(map[string]*SortedChunkIndexing)
	cm.traceReaderMap = make(map[string]algocommon.TraceDataReader)
	return cm
}

func (cm *SortedChunkIndexingManager) Init() bool {

	if cm.ioTraceFile != "" {
		cm.ioCloseChan = make(chan bool)
		cm.ioChan = make(chan string, 1E4)
		if cm.ioTraceFile[len(cm.ioTraceFile)-2:] != "gz" {
			go common.WriteStringsCompressed(cm.ioTraceFile+".gz", cm.ioChan, cm.ioCloseChan)
		} else {
			go common.WriteStringsCompressed(cm.ioTraceFile, cm.ioChan, cm.ioCloseChan)
		}
	}

	cm.chunkIndex = NewSortedChunkIndex(cm.config.ChunkIndexPageSize, cm.config.IndexMemoryLimit, cm.ioChan)

	return true
}

// Creates a new SortedChunkIndexing instance. All instances creates by this function share the
// same ChunkIndex and BlockIndex.
func (cm *SortedChunkIndexingManager) CreateSortedChunkIndexing() *SortedChunkIndexing {
	cc := NewSortedChunkIndexing(cm.config,
		cm.avgChunkSize,
		cm.chunkIndex,
		"",
		cm.ioChan)

	return cc
}

// collects the statistics of all handler instances and returns a summary as well as a list of all statistics.
func (cm *SortedChunkIndexingManager) collectStatistic(ccs map[string]*SortedChunkIndexing) (SortedChunkIndexingStatistics, map[string]SortedChunkIndexingStatistics) {

	stats := make(map[string]SortedChunkIndexingStatistics)
	for host, cc := range ccs {
		stats[host] = cc.StatsList[len(cc.StatsList)-1]
	}

	var totalStats SortedChunkIndexingStatistics

	var predHitrateSum float32
	var predUtilitySum float32
	for _, s := range stats {
		predHitrateSum += s.PredictedPageHitrate
		totalStats.add(s)
		totalStats.FileNumber = s.FileNumber
	}

	if len(stats) > 0 {
		totalStats.AvgPredictedPageHitrate = (predHitrateSum / float32(len(stats)))
		totalStats.PredictedPageUtility = (predUtilitySum / float32(len(stats)))
		totalStats.AvgPageUtility = (predHitrateSum / float32(len(stats)))
	}

	totalStats.ChunkIndexStats.PageCount = cm.chunkIndex.getPageForPos(uint32(len(cm.chunkIndex.diskIndex) - 1))

	if totalStats.UsedPagesCount > 0 {
		totalStats.AvgPageUtility = float32(float64(totalStats.ChunkIndexStats.DiskIndexHits) / (float64(totalStats.UsedPagesCount) * float64(cm.chunkIndex.getNumChunksPerPage())))
	}

	totalStats.ChunkIndexStats.finish()

	totalStats.finish()

	cm.chunkIndex.Statistics.reset()

	return totalStats, stats
}

func (cm *SortedChunkIndexingManager) RunGeneration(genNum int, fileInfoList []algocommon.TraceFileInfo) {
	wg := new(sync.WaitGroup)
	wg.Add(len(fileInfoList))

	usedSCs := make(map[string]*SortedChunkIndexing, 0)

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

		if _, ok := cm.scMap[hostname]; !ok {
			cm.scMap[hostname] = cm.CreateSortedChunkIndexing()
		}

		cc := cm.scMap[hostname]
		usedSCs[hostname] = cc
		cc.BeginTraceFile(genNum, fileInfo.Name)

		var traceReader algocommon.TraceDataReader = nil
		if tr, ok := cm.traceReaderMap[hostname]; !ok {
			log.Info("gen ", genNum, ": generate TraceDataReader for host: ", hostname)
			traceReader = algocommon.NewTraceDataReader(fileInfo.Path)
			cm.traceReaderMap[hostname] = traceReader
		} else {
			log.Info("gen ", genNum, ": recycling TraceDataReader for host: ", hostname)
			tr.Reset(fileInfo.Path)
			traceReader = tr
		}

		// cc instance is ready
		go func(gen int, tReader algocommon.TraceDataReader, sci *SortedChunkIndexing) {
			defer wg.Done()
			fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)
			go tReader.FeedAlgorithm(fileEntryChan)

			var chunks [][12]byte
			var fp [12]byte
			if l := len(cm.chunkIndex.allFp); l == 0 {
				// noop
			} else if l <= cm.chunkIndex.getNumChunksPerPage() {
				fp = cm.chunkIndex.allFp[l-1]
				chunks = cm.getFirstChunksSmallerThan(fp, fileEntryChan, sci.handleSingleFileEntry, tReader.GetFileEntryReturn())
			} else {
				fp = cm.chunkIndex.allFp[cm.chunkIndex.getNumChunksPerPage()-1]
				chunks = cm.getFirstChunksSmallerThan(fp, fileEntryChan, sci.handleSingleFileEntry, tReader.GetFileEntryReturn())
			}

			sci.Statistics.PredictedPageHitrate = cm.chunkIndex.predictPageHitrate(chunks)
			sci.Statistics.PredictedPageUtility = cm.chunkIndex.predictPageUtility(chunks)

			log.Infof("%v: num first chunks: %v (one page = %v). hitrate: %v, utility: %v", sci.streamId, len(chunks), cm.chunkIndex.getNumChunksPerPage(), sci.Statistics.PredictedPageHitrate, sci.Statistics.PredictedPageUtility)

			// let the algo process the rest
			sci.HandleFiles(fileEntryChan, tReader.GetFileEntryReturn())
			sci.EndTraceFile()
		}(genNum, traceReader, cc)
	}
	wg.Wait()

	// flush memindex to disk one
	log.Info("Flushing chunk index")
	cm.chunkIndex.Flush(nil) // no locking here because the manager is the only active instance at this point

	// collect stats
	log.Info("Collecting statistics")
	totalStats, ccStats := cm.collectStatistic(usedSCs)
	ccStats["total"] = totalStats
	cm.StatsList = append(cm.StatsList, ccStats)

	// print statistics
	if encodedStats, err := json.MarshalIndent(ccStats, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info("Generation ", genNum, ": Stats of all used SCIs:")
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
func (cm *SortedChunkIndexingManager) Quit() {

	if cm.ioCloseChan != nil {
		close(cm.ioChan)
		log.Info("wait for ioTraceRoutine")
		<-cm.ioCloseChan
	}

	if cm.outputFilename == "" {
		return
	}

	jsonMap := make(map[string]interface{})
	for i, generationStats := range cm.StatsList {
		jsonMap["generation "+strconv.Itoa(i)] = generationStats
	}

	if encodedStats, err := json.MarshalIndent(jsonMap, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(cm.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}

// extracts the first unique fp that are smaller than the given fp from the stream. If given, it calls the handler for each touched FileEntry and returns.
func (cm *SortedChunkIndexingManager) getFirstChunksSmallerThan(fp [12]byte, fentries <-chan *algocommon.FileEntry, handler func(*algocommon.FileEntry), fileEntryReturn chan<- *algocommon.FileEntry) [][12]byte {

	/*fileEntries := make([]*algocommon.FileEntry, 0)*/
	chunks := make([][12]byte, 0, cm.chunkIndex.getNumChunksPerPage())

	var lastFp [12]byte
	var currentFp [12]byte
	lastFp[1] = 42

	for {
		f, ok := <-fentries
		if !ok {
			log.Warnf("emptied channel while getting first chunks smaller than %x", fp)
			return chunks
		}

		if handler != nil {
			handler(f)
		}

		for i := range f.Chunks {
			copy(currentFp[:], f.Chunks[i].Digest)

			if bytes.Compare(currentFp[:], fp[:]) == 1 { // reached fp bigger than the given one?
				fileEntryReturn <- f
				return chunks
			}
			if !bytes.Equal(lastFp[:], currentFp[:]) {
				copy(lastFp[:], currentFp[:])
				chunks = append(chunks, lastFp)
			}
		}
		fileEntryReturn <- f
	}
}
