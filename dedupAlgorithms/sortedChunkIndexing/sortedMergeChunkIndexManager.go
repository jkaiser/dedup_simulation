package sortedChunkIndexing

import "fmt"
import "os"
import "sync"
import "bytes"
import "strings"
import "strconv"
import "encoding/json"
import "sync/atomic"

import log "github.com/cihub/seelog"
import "github.com/bradfitz/gomemcache/memcache"
import "github.com/jkaiser/dedup_simulations/common"
import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

// Kind of father struct which for creating and managing SortedMergeChunkIndexing instances.
type SortedMergeChunkIndexingManager struct {
	avgChunkSize int
	chunkIndex   *SortedChunkIndex

	clientServers string
	mcClient      *memcache.Client

	config         SortedMergeChunkIndexingConfig
	outputFilename string
	ioTraceFile    string
	ioChan         chan string // channel for each handler to send ioTrace entries into
	ioCloseChan    chan bool   // channel to signal finished ioTrace goroutine

	scMap                     map[string]*SortedMergeChunkIndexing
	traceReaderMap            map[string]algocommon.TraceDataReader
	lastContainerIdOfFirstGen uint32 // used for flushing local containers to memcache

	TotalStatsList []SortedMergeChunkIndexingStatistics            // holds the summarized statistics for each gen
	StatsList      []map[string]SortedMergeChunkIndexingStatistics // holds the statistics of all _used_ ccs after each gen.
}

func NewSortedMergeChunkIndexingManager(config SortedMergeChunkIndexingConfig, avgChunkSize int, clientServers string, outFilename string, ioTraceFile string) *SortedMergeChunkIndexingManager {
	cm := new(SortedMergeChunkIndexingManager)
	cm.config = config
	cm.avgChunkSize = avgChunkSize
	cm.clientServers = clientServers

	cm.lastContainerIdOfFirstGen = 0

	cm.outputFilename = outFilename
	cm.ioTraceFile = ioTraceFile
	cm.scMap = make(map[string]*SortedMergeChunkIndexing)
	cm.traceReaderMap = make(map[string]algocommon.TraceDataReader)
	return cm
}

func (cm *SortedMergeChunkIndexingManager) Init() bool {

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

// Creates a new SortedMergeChunkIndexing instance. All instances creates by this function share the
// same ChunkIndex and BlockIndex.
func (cm *SortedMergeChunkIndexingManager) CreateSortedMergeChunkIndexing() *SortedMergeChunkIndexing {
	cc := NewSortedMergeChunkIndexing(cm.config,
		cm.avgChunkSize,
		cm.chunkIndex,
		"",
		cm.ioChan)

	return cc
}

// collects the statistics of all handler instances and returns a summary as well as a list of all statistics.
func (cm *SortedMergeChunkIndexingManager) collectStatistic(ccs map[string]*SortedMergeChunkIndexing) (SortedMergeChunkIndexingStatistics, map[string]SortedMergeChunkIndexingStatistics) {

	stats := make(map[string]SortedMergeChunkIndexingStatistics)
	for host, cc := range ccs {
		stats[host] = cc.StatsList[len(cc.StatsList)-1]
	}

	var totalStats SortedMergeChunkIndexingStatistics

	for _, s := range stats {
		totalStats.add(s)
		totalStats.FileNumber = s.FileNumber
	}

	//if len(stats) > 0 {
	//totalStats.AvgPageUtility = (predHitrateSum / float32(len(stats)))
	//}

	totalStats.ChunkIndexStats.PageCount = cm.chunkIndex.getPageForPos(uint32(len(cm.chunkIndex.diskIndex) - 1))

	//if totalStats.UsedPagesCount > 0 {
	//totalStats.AvgPageUtility = float32(float64(totalStats.ChunkIndexStats.DiskIndexHits) / (float64(totalStats.UsedPagesCount) * float64(cm.chunkIndex.getNumChunksPerPage())))
	//}
	if (totalStats.UsedPagesCount > 0) && (cm.chunkIndex.getNumPages() > 0) {
		duplicatesDetectedByPages := totalStats.RedundantChunkCount - totalStats.ConsecRedundantChunkCount
		totalStats.AvgPageUtility = float32(float64(duplicatesDetectedByPages) / float64(cm.chunkIndex.getNumChunksPerPage()) / float64(cm.chunkIndex.getNumPages()))
	}

	totalStats.ChunkIndexStats.finish()
	totalStats.finish()

	cm.chunkIndex.Statistics.reset()
	return totalStats, stats
}

func (cm *SortedMergeChunkIndexingManager) RunGeneration(genNum int, fileInfoList []algocommon.TraceFileInfo) {
	wg := new(sync.WaitGroup)
	wg.Add(len(fileInfoList))

	sameMomentStartWG := new(sync.WaitGroup) // barrier to let all handlers start at the same time
	sameMomentStartWG.Add(len(fileInfoList))

	usedSCs := make(map[string]*SortedMergeChunkIndexing, 0)

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
			cm.scMap[hostname] = cm.CreateSortedMergeChunkIndexing()
		}

		sci := cm.scMap[hostname]
		usedSCs[hostname] = sci
		sci.BeginTraceFile(genNum, fileInfo.Name)

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

		// sci instance is ready
		go func(gen int, tReader algocommon.TraceDataReader, sci *SortedMergeChunkIndexing) {
			defer wg.Done()
			fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)
			go tReader.FeedAlgorithm(fileEntryChan)

			sameMomentStartWG.Done() // synchronize the start of every file backup
			sameMomentStartWG.Wait()

			// let the algo process the rest
			sci.HandleFiles(fileEntryChan, tReader.GetFileEntryReturn())
			sci.EndTraceFile()
		}(genNum, traceReader, sci)
	}

	closeChan := make(chan struct{})
	var numUsedPages int64
	go cm.performOneSequentialRun(fileInfoList[0].Name, usedSCs, closeChan, &numUsedPages)
	wg.Wait()
	// all handler finished, close sequential run
	close(closeChan)

	// flush memindex to disk one
	log.Info("Flushing chunk index")
	cm.chunkIndex.Flush(nil) // no locking here because the manager is the only active instance at this point
	log.Info("Chunk Index size: ", len(cm.chunkIndex.allFp))

	// collect stats
	log.Info("Collecting statistics")
	totalStats, ccStats := cm.collectStatistic(usedSCs)
	totalStats.StorageCount = numUsedPages
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

// performs one sequential run over the sorted ChunkIndex. It signals the waiting handler
// which page currently is in memory and can be processed.
func (cm *SortedMergeChunkIndexingManager) performOneSequentialRun(streamId string, scis map[string]*SortedMergeChunkIndexing, closeChan chan struct{}, numUsedPages *int64) {
	log.Info("sequential run started")
	for {
		smallestChunk := [12]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
		// get next requested chunks
		for _, sc := range scis {
			select {
			case tmp, ok := <-sc.NextRequestedChunk:
				if ok && (bytes.Compare(tmp[:], smallestChunk[:]) == -1) {
					smallestChunk = tmp
				}
			case <-closeChan:
				log.Info("closing sequential run")
				return
			}
		}

		//log.Infof("next small one is: %x", smallestChunk)

		// get the last fingperint of the same page the smallestChunk resides in. This marks
		// the end of the page. ==> page loaded, all can proceed until that chunk without IO
		fp := [12]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
		var loadedPage uint32
		if len(cm.chunkIndex.allFp) > 0 {

			fp, loadedPage = cm.chunkIndex.GetLastChunkFpOfPage(smallestChunk)
			atomic.AddInt64(numUsedPages, 1)
			// this is equivilant to loading a page =>
			if cm.ioChan != nil {
				cm.ioChan <- fmt.Sprintf("%v\t%v\n", streamId, loadedPage)
			}
			//log.Infof("next biggest chunk in open page is: %x", fp)
		} else {
			log.Infof("empty CI => next biggest chunk in open page is: %x", fp)
		}

		var allFinished bool = true
		for _, sc := range scis {
			sc.isFinishedMut.RLock()
			allFinished = allFinished && sc.isFinished
			if !sc.isFinished {
				sc.NextBiggestChunkInOpenPage <- fp
				//log.Infof("sent next one to %v", sc.streamId)
			}
			sc.isFinishedMut.RUnlock()
		}
		if allFinished {
			log.Info("closing sequential run")
			return
		}
	}
}

// Writes the final statistics and cleans up if necesary
func (cm *SortedMergeChunkIndexingManager) Quit() {

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
