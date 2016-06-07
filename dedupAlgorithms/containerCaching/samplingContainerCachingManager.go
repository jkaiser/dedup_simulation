package containerCaching

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

// Kind of father struct which for creating and managing SamplingContainerCaching instances.
type SamplingContainerCachingManager struct {
	avgChunkSize          int
	chunkIndex            map[[12]byte]uint32
	chunkIndexRWLock      *sync.RWMutex
	containerCache        *ContainerCache
	currentContainerCache *CurrentContainerCache
	containerStorage      *ContainerStorage

	clientServers string
	mcClient      *memcache.Client

	config         SamplingContainerCachingConfig
	outputFilename string
	ioTraceFile    string
	ioChan         chan string // channel for each handler to send ioTrace entries into
	ioCloseChan    chan bool   // channel to signal finished ioTrace goroutine

	ccMap                     map[string]*SamplingContainerCaching
	traceReaderMap            map[string]algocommon.TraceDataReader
	lastContainerIdOfFirstGen uint32 // used for flushing local containers to memcache

	TotalStatsList []SamplingContainerCachingStatistics            // holds the summarized statistics for each gen
	StatsList      []map[string]SamplingContainerCachingStatistics // holds the statistics of all _used_ ccs after each gen.
}

func NewSamplingContainerCachingManager(config SamplingContainerCachingConfig, avgChunkSize int, clientServers string, outFilename string, ioTraceFile string) *SamplingContainerCachingManager {
	cm := new(SamplingContainerCachingManager)
	cm.config = config
	cm.avgChunkSize = avgChunkSize
	cm.chunkIndex = make(map[[12]byte]uint32, 1E7)
	cm.chunkIndexRWLock = new(sync.RWMutex)
	cm.currentContainerCache = NewCurrentContainerCache()
	cm.clientServers = clientServers

	cm.lastContainerIdOfFirstGen = 0

	cm.outputFilename = outFilename
	cm.ioTraceFile = ioTraceFile
	cm.ccMap = make(map[string]*SamplingContainerCaching)
	cm.traceReaderMap = make(map[string]algocommon.TraceDataReader)
	return cm
}

func (cm *SamplingContainerCachingManager) Init() bool {

	if cm.ioTraceFile != "" {
		cm.ioCloseChan = make(chan bool)
		cm.ioChan = make(chan string, 1E5)
		if cm.ioTraceFile[len(cm.ioTraceFile)-2:] != "gz" {
			go common.WriteStringsCompressed(cm.ioTraceFile+".gz", cm.ioChan, cm.ioCloseChan)
		} else {
			go common.WriteStringsCompressed(cm.ioTraceFile, cm.ioChan, cm.ioCloseChan)
		}
	}

	cm.containerStorage = NewContainerStorage(cm.config.ContainerSize, cm.avgChunkSize, cm.ioChan)
	cm.containerCache = NewContainerCache(cm.config.CacheSize, cm.config.ContainerSize, cm.config.ContainerPageSize, cm.avgChunkSize, cm.containerStorage)

	var ret bool = true
	if len(cm.clientServers) > 0 {
		if cm.mcClient, ret = common.MemClientInit(cm.clientServers); ret {
			cm.containerStorage.containerStorage = cm.mcClient
		}
	}
	return ret
}

// Creates a new SamplingContainerCaching instance. All instances creates by this function share the
// same ChunkIndex and BlockIndex.
func (cm *SamplingContainerCachingManager) CreateSamplingContainerCaching() *SamplingContainerCaching {
	cc := NewSamplingContainerCaching(cm.config,
		cm.avgChunkSize,
		cm.containerStorage,
		cm.containerCache,
		cm.currentContainerCache,
		cm.chunkIndex,
		cm.chunkIndexRWLock,
		"",
		cm.ioChan)
	return cc
}

// collects the statistics of all CC instances and returns a summary as well as a list of all statistics.
func (cm *SamplingContainerCachingManager) collectStatistic(ccs map[string]*SamplingContainerCaching) (SamplingContainerCachingStatistics, map[string]SamplingContainerCachingStatistics) {

	stats := make(map[string]SamplingContainerCachingStatistics)
	for host, cc := range ccs {
		stats[host] = cc.StatsList[len(cc.StatsList)-1]
	}

	var totalStats SamplingContainerCachingStatistics

	for _, s := range stats {
		totalStats.add(s)
	}

	totalStats.ChunkCacheStats.Finish()
	totalStats.ChunkIndexStats.Finish()
	totalStats.ContainerCacheStats.finish()
	totalStats.ContainerStorageStats.finish()

	totalStats.finish()

	return totalStats, stats
}

func (cm *SamplingContainerCachingManager) RunGeneration(genNum int, fileInfoList []algocommon.TraceFileInfo) {
	wg := new(sync.WaitGroup) // to wait for all handlers to finish
	wg.Add(len(fileInfoList))

	sameMomentStartWG := new(sync.WaitGroup) // barrier to let all handlers start at the same time
	sameMomentStartWG.Add(len(fileInfoList))

	usedCCs := make(map[string]*SamplingContainerCaching, 0)

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

		if _, ok := cm.ccMap[hostname]; !ok {
			cm.ccMap[hostname] = cm.CreateSamplingContainerCaching()
		}

		cc := cm.ccMap[hostname]
		usedCCs[hostname] = cc
		cc.BeginTraceFile(genNum, fileInfo.Name)

		// cc instance is ready
		go func(gen int, ccHandler *SamplingContainerCaching, tracePath string) {
			defer wg.Done()
			fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)

			log.Info("gen ", gen, ": generate TraceDataReader for trace: ", tracePath)
			tReader := algocommon.NewTraceDataReader(tracePath)
			go tReader.FeedAlgorithm(fileEntryChan)

			sameMomentStartWG.Done() // synchronize the start of every file backup
			sameMomentStartWG.Wait()

			ccHandler.HandleFiles(fileEntryChan, tReader.GetFileEntryReturn())
			ccHandler.EndTraceFile()
		}(genNum, cc, fileInfo.Path)
	}
	wg.Wait()

	// flush containers to memcache if necessary
	if cm.lastContainerIdOfFirstGen == 0 { // this was the first gen
		for _, cc := range cm.ccMap {
			if (cc.currentContainerId - 1) > cm.lastContainerIdOfFirstGen {
				//'-1' because the cuurent one isn't committed yet
				cm.lastContainerIdOfFirstGen = cc.currentContainerId - 1
			}
		}

	} else if cm.mcClient != nil { // flush
		log.Info("Flushing container storage to memcache...")
		if !cm.containerStorage.FlushIDsFrom(cm.lastContainerIdOfFirstGen) {
			log.Error("Error during flushing of containers after Gen ", genNum)
		}
	}

	totalStats, ccStats := cm.collectStatistic(usedCCs)
	ccStats["total"] = totalStats
	cm.StatsList = append(cm.StatsList, ccStats)

	// print statistics
	if encodedStats, err := json.MarshalIndent(ccStats, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info("Generation ", genNum, ": Stats of all used CCs:")
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
func (cm *SamplingContainerCachingManager) Quit() {

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
