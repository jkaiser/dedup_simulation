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

// helper struct for bundling trace-handler-metadata tuple. It is a kind of work package.
type traceTuple struct {
	hostname string
	fileInfo algocommon.TraceFileInfo
	blc      *BlockIndexFilter
}

// Kind of father struct which for creating and managing BlockIndexFilter instances.
type BlockIndexFilterManager struct {
	avgChunkSize     int
	chunkIndex       map[[12]byte]uint32
	chunkIndexRWLock *sync.RWMutex
	blockIndex       *BlockIndex

	clientServers string
	mcClient      *memcache.Client

	config             BlockIndexFilterConfig
	outputFilename     string
	ioTraceFile        string
	ioChan             chan string // channel for each handler to send ioTrace entries into
	ioCloseChan        chan bool   // channel to signal finished ioTrace goroutine
	startBlockOfNewBLC uint32

	blcMap         map[string]*BlockIndexFilter
	blcRangeMap    map[string][2]uint32 // stores the start block ID of the blcHandler and the end blockId of the last generation: [startFirstGen, startLastGen]
	traceReaderMap map[string]algocommon.TraceDataReader

	TotalStatsList []BlockIndexFilterStatistics            // holds the summarized statistics for each gen
	StatsList      []map[string]BlockIndexFilterStatistics // holds the statistics of all _used_ blcs after each gen.
}

func (bf *BlockIndexFilterManager) Init() bool {

	if bf.ioTraceFile != "" {
		bf.ioCloseChan = make(chan bool)
		bf.ioChan = make(chan string, 100000)
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

func NewBlockIndexFilterManager(config BlockIndexFilterConfig, avgChunkSize int, clientServers string, outFilename string, ioTraceFile string) *BlockIndexFilterManager {
	b := new(BlockIndexFilterManager)
	b.config = config
	b.avgChunkSize = avgChunkSize
	b.chunkIndex = make(map[[12]byte]uint32, 1E7)
	b.chunkIndexRWLock = new(sync.RWMutex)
	/*b.chunkCache = NewChunkCache(config.ChunkCacheSize)*/
	b.clientServers = clientServers
	b.outputFilename = outFilename
	b.ioTraceFile = ioTraceFile
	b.blcMap = make(map[string]*BlockIndexFilter)
	b.blcRangeMap = make(map[string][2]uint32)
	b.traceReaderMap = make(map[string]algocommon.TraceDataReader)
	return b
}

// Creates a new BlockIndexFilter instance. All instances creates by this function share the
// same ChunkIndex and BlockIndex.
func (bf *BlockIndexFilterManager) CreateBLockIndexFilter() *BlockIndexFilter {
	b := NewBlockIndexFilter(bf.config,
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
func (bf *BlockIndexFilterManager) collectStatistic(tuples []*traceTuple) (BlockIndexFilterStatistics, map[string]BlockIndexFilterStatistics) {

	stats := make(map[string]BlockIndexFilterStatistics)
	for _, t := range tuples {
		stats[t.hostname] = t.blc.StatsList[len(t.blc.StatsList)-1]
	}

	var totalStats BlockIndexFilterStatistics
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

func (bf *BlockIndexFilterManager) RunGeneration(genNum int, fileInfoList []algocommon.TraceFileInfo) {

	allBLCTuples := make([]*traceTuple, 0)

	// 1) build blcs as necessary and flush old ones
	for _, fileInfo := range fileInfoList {

		tuple := new(traceTuple)
		tuple.fileInfo = fileInfo
		allBLCTuples = append(allBLCTuples, tuple)

		if strings.Contains(bf.config.DataSet, "multi") { // if not parallel run
			var splits []string = strings.Split(fileInfo.Name, "_")
			tuple.hostname = strings.Split(splits[len(splits)-1], "-")[0]
		} else {
			tuple.hostname = "host0"
		}
		log.Info("gen ", genNum, ": processing host: ", tuple.hostname)

		// check for flushing etc
		var ok bool
		if tuple.blc, ok = bf.blcMap[tuple.hostname]; !ok { // new stream
			tuple.blc = bf.CreateBLockIndexFilter()
			bf.blcMap[tuple.hostname] = tuple.blc
			bf.blcRangeMap[tuple.hostname] = [2]uint32{bf.blcMap[tuple.hostname].currentBlockId, 0}

		} else if blockIDTuple, ok := bf.blcRangeMap[tuple.hostname]; !ok { // old stream -> flush block index to memcache
			log.Error("Found old blc without blcRangeMap entry for stream: ", tuple.hostname)

		} else if blockIDTuple[1] == 0 {
			blockIDTuple[1] = tuple.blc.currentBlockId
			bf.blcRangeMap[tuple.hostname] = blockIDTuple

		} else if ok = bf.blockIndex.FlushIDsFromDownto(blockIDTuple[1], blockIDTuple[0]); !ok {
			log.Errorf("Couldn't flush old entries (%v downto %v) for stream %s", blockIDTuple[1], blockIDTuple[0], tuple.hostname)

		} else { // successfully flushed -> set the current block as new limit to last gen
			blockIDTuple[1] = tuple.blc.currentBlockId
			bf.blcRangeMap[tuple.hostname] = blockIDTuple
		}
	}

	// 2) start all blcs
	log.Info("Start all blc handlers...")
	wg := new(sync.WaitGroup)
	wg.Add(len(allBLCTuples))
	for _, t := range allBLCTuples {
		go func(gen int, tuple *traceTuple) {
			defer wg.Done()
			fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)

			// generate traceDataReader
			log.Info("gen ", gen, ": generate TraceDataReader for path: ", tuple.fileInfo.Path)
			tReader := algocommon.NewTraceDataReader(tuple.fileInfo.Path)
			go tReader.FeedAlgorithm(fileEntryChan)

			tuple.blc.BeginTraceFile(gen, tuple.fileInfo.Name)
			tuple.blc.HandleFiles(fileEntryChan, tReader.GetFileEntryReturn())
			tuple.blc.EndTraceFile()
		}(genNum, t)
	}
	wg.Wait()

	totalStats, blcStats := bf.collectStatistic(allBLCTuples)
	blcStats["total"] = totalStats
	bf.StatsList = append(bf.StatsList, blcStats)

	// print statistics
	if encodedStats, err := json.MarshalIndent(blcStats, "", "    "); err != nil {
		log.Error("Couldn't marshal generation statistics: ", err)
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
func (bf *BlockIndexFilterManager) Quit() {

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
