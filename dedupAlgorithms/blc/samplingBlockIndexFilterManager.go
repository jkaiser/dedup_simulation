package blc

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

// Kind of father struct which for creating and managing SamplingBlockIndexFilter instances.
type SamplingBlockIndexFilterManager struct {
	avgChunkSize     int
	chunkIndex       map[[12]byte]uint32
	chunkIndexRWLock *sync.RWMutex
	blockIndex       *BlockIndex
	config           SamplingBlockIndexFilterConfig
	outputFilename   string

	clientServers string
	mcClient      *memcache.Client

	blcMap             map[string]*SamplingBlockIndexFilter
	blcRangeMap        map[string][2]uint32 // stores the start block ID of the blcHandler and the end blockId of the last generation: [startFirstGen, startLastGen]
	traceReaderMap     map[string]algocommon.TraceDataReader
	startBlockOfNewBLC uint32

	ioTraceFile string
	ioChan      chan string // channel for each handler to send ioTrace entries into
	ioCloseChan chan bool   // channel to signal finished ioTrace goroutine

	TotalStatsList []SamplingBlockIndexFilterStatistics            // holds the summarized statistics for each gen
	StatsList      []map[string]SamplingBlockIndexFilterStatistics // holds the statistics of all _used_ blcs after each gen.
}

func NewSamplingBlockIndexFilterManager(config SamplingBlockIndexFilterConfig, avgChunkSize int, clientServers string, outFilename string, ioTraceFile string) *SamplingBlockIndexFilterManager {
	b := new(SamplingBlockIndexFilterManager)
	b.config = config
	b.avgChunkSize = avgChunkSize
	b.chunkIndex = make(map[[12]byte]uint32, 1E7)
	b.chunkIndexRWLock = new(sync.RWMutex)
	/*b.blockIndex = NewBlockIndex(config.BlockSize, config.BlockIndexPageSize, avgChunkSize)*/
	b.clientServers = clientServers
	b.outputFilename = outFilename
	b.ioTraceFile = ioTraceFile
	b.blcMap = make(map[string]*SamplingBlockIndexFilter)
	b.blcRangeMap = make(map[string][2]uint32)
	b.traceReaderMap = make(map[string]algocommon.TraceDataReader)
	return b
}

func (bf *SamplingBlockIndexFilterManager) Init() bool {

	if bf.ioTraceFile != "" {
		bf.ioCloseChan = make(chan bool)
		bf.ioChan = make(chan string, 1e4)
		if bf.ioTraceFile[len(bf.ioTraceFile)-2:] != "gz" {
			go common.WriteStringsCompressed(bf.ioTraceFile+".gz", bf.ioChan, bf.ioCloseChan)
		} else {
			go common.WriteStringsCompressed(bf.ioTraceFile, bf.ioChan, bf.ioCloseChan)
		}
	}

	bf.blockIndex = NewBlockIndex(bf.config.BlockSize, bf.config.BlockIndexPageSize, bf.avgChunkSize, bf.ioChan)

	var ret bool = true
	if len(bf.clientServers) > 0 {
		if bf.mcClient, ret = common.MemClientInit(bf.clientServers); ret {
			bf.blockIndex.setIndex(bf.mcClient)
		}
	}
	return ret
}

// Creates a new SamplingBlockIndexFilter instance. All instances creates by this function share the
// same ChunkIndex and BlockIndex.
func (bf *SamplingBlockIndexFilterManager) CreateBLockIndexFilter() *SamplingBlockIndexFilter {
	b := NewSamplingBlockIndexFilter(bf.config,
		bf.avgChunkSize,
		bf.chunkIndex,
		bf.chunkIndexRWLock,
		bf.blockIndex,
		"")

	// determine the max block ID and start new generations at 10M boundaries.
	// 10M BlockIndex 256KB-blocks roughly represent 1TB logical backupped data.
	// Here I assume that no single backup stream logicallt is bigger than 2TB.
	b.currentBlockId = bf.startBlockOfNewBLC
	bf.startBlockOfNewBLC += 2 * 5 * 1E6

	return b
}

// collects the statistics of all BLC instances and returns a summary as well as a list of all statistics.
func (bf *SamplingBlockIndexFilterManager) collectStatistic(blcs map[string]*SamplingBlockIndexFilter) (SamplingBlockIndexFilterStatistics, map[string]SamplingBlockIndexFilterStatistics) {

	stats := make(map[string]SamplingBlockIndexFilterStatistics)
	for host, blc := range blcs {
		stats[host] = blc.StatsList[len(blc.StatsList)-1]
	}

	var totalStats SamplingBlockIndexFilterStatistics
	totalStats.BlockLocalityCacheStats = NewBlockLocalityCacheStatistics()

	for _, s := range stats {
		totalStats.add(s)
	}

	totalStats.ChunkCacheStats.Finish()
	totalStats.BlockLocalityCacheStats.finish()
	totalStats.BlockIndexStats.finish()
	totalStats.ChunkIndexStats.Finish()

	totalStats.finish()

	return totalStats, stats
}

func (bf *SamplingBlockIndexFilterManager) RunGeneration(genNum int, fileInfoList []algocommon.TraceFileInfo) {
	wg := new(sync.WaitGroup)
	wg.Add(len(fileInfoList))

	usedBLCs := make(map[string]*SamplingBlockIndexFilter, 0)

	for _, fileInfo := range fileInfoList {

		var hostname string

		// start new generation and create data structures if necesary
		if strings.Contains(bf.config.DataSet, "multi") { // if not parallel run
			var splits []string = strings.Split(fileInfo.Name, "_")
			hostname = strings.Split(splits[len(splits)-1], "-")[0]
		} else {
			hostname = "host0"
		}
		log.Info("gen ", genNum, ": processing host: ", hostname)

		var sblc *SamplingBlockIndexFilter
		var ok bool
		if sblc, ok = bf.blcMap[hostname]; !ok { // new stream
			sblc = bf.CreateBLockIndexFilter()
			bf.blcMap[hostname] = sblc
			bf.blcRangeMap[hostname] = [2]uint32{bf.blcMap[hostname].currentBlockId, 0}

		} else if blockIDTuple, ok := bf.blcRangeMap[hostname]; !ok { // old stream -> flush block index to memcache
			log.Error("Found old blc without blcRangeMap entry for stream: ", hostname)

		} else if blockIDTuple[1] == 0 {
			blockIDTuple[1] = sblc.currentBlockId
			bf.blcRangeMap[hostname] = blockIDTuple

		} else if ok = bf.blockIndex.FlushIDsFromDownto(blockIDTuple[1], blockIDTuple[0]); !ok {
			log.Errorf("Couldn't flush old entries (%v downto %v) for stream %s", blockIDTuple[1], blockIDTuple[0], hostname)

		} else { // successfully flushed -> set the current block as new limit to last gen
			blockIDTuple[1] = sblc.currentBlockId
			bf.blcRangeMap[hostname] = blockIDTuple
		}

		usedBLCs[hostname] = sblc

		sblc.BeginTraceFile(genNum, fileInfo.Name)

		// blc instance is ready
		go func(gen int, sblcHandler *SamplingBlockIndexFilter, tracePath string) {
			defer wg.Done()
			fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)

			log.Info("gen ", gen, ": generate TraceDataReader for path: ", tracePath)
			tReader := algocommon.NewTraceDataReader(tracePath)
			go tReader.FeedAlgorithm(fileEntryChan)

			sblcHandler.HandleFiles(fileEntryChan, tReader.GetFileEntryReturn())
			sblcHandler.EndTraceFile()
		}(genNum, sblc, fileInfo.Path)
	}
	wg.Wait()

	totalStats, blcStats := bf.collectStatistic(usedBLCs)
	blcStats["total"] = totalStats
	bf.StatsList = append(bf.StatsList, blcStats)

	// print statistics
	if encodedStats, err := json.MarshalIndent(blcStats, "", "    "); err != nil {
		log.Error("Couldn't marshal statistics: ", err)
	} else {
		log.Info("Generation ", genNum, ": Stats of all used BLCs:")
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
func (bf *SamplingBlockIndexFilterManager) Quit() {

	if bf.ioCloseChan != nil {
		close(bf.ioChan)
		log.Info("wait for ioTraceRoutine")
		<-bf.ioCloseChan
	}

	if bf.outputFilename == "" {
		return
	}

	jsonMap := make(map[string]interface{})
	for i, generationStats := range bf.StatsList {
		jsonMap["generation "+strconv.Itoa(i)] = generationStats
	}

	if encodedStats, err := json.MarshalIndent(jsonMap, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(bf.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}
