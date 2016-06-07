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

// helper struct for initial similarity computation
type traceTuple struct {
	hostname     string
	stream       chan *algocommon.FileEntry
	reader       algocommon.TraceDataReader
	firstUniques []common.Chunk
	firstChunks  []common.Chunk // all chunks that were accesses while finding the first uniques
}

// helper struct for initial similarity computation
type matrixEntry struct {
	firstNChunks []common.Chunk
	traces       []*traceTuple
	utility      float32
	avgUtility   float32 // average utility of both "directions"
}

// Kind of father struct which for creating and managing SortedChunkIndexing instances.
type SimilarityChunkIndexingManager struct {
	avgChunkSize int
	chunkIndex   *SortedChunkIndex // the default one. Should always be nil

	ciMutex     sync.RWMutex
	allCIs      []*SortedChunkIndex // List of all chunk indices
	allCIsUsage []int               // Usage Counts of the indices
	ciMap       map[string]int      // holds mapping hostname/streamid -> pos(=ID) of chunk index in allCIs

	clientServers string
	mcClient      *memcache.Client

	config         SortedChunkIndexingConfig
	outputFilename string
	ioTraceFile    string
	ioChan         chan string // channel for each handler to send ioTrace entries into
	ioCloseChan    chan bool   // channel to signal finished ioTrace goroutine

	/*scMap  map[string]*SortedChunkIndexing // mapping stream -> handler*/

	TotalStatsList []SortedChunkIndexingStatistics            // holds the summarized statistics for each gen
	StatsList      []map[string]SortedChunkIndexingStatistics // holds the statistics of all _used_ ccs after each gen.
}

func NewSimilarityChunkIndexingManager(config SortedChunkIndexingConfig, avgChunkSize int, clientServers string, outFilename string, ioTraceFile string) *SimilarityChunkIndexingManager {
	scm := new(SimilarityChunkIndexingManager)
	scm.config = config
	scm.avgChunkSize = avgChunkSize
	scm.clientServers = clientServers
	scm.ciMap = make(map[string]int)

	scm.outputFilename = outFilename
	scm.ioTraceFile = ioTraceFile
	/*scm.scMap = make(map[string]*SortedChunkIndexing)*/
	return scm
}

func (scm *SimilarityChunkIndexingManager) Init() bool {

	if scm.ioTraceFile != "" {
		scm.ioCloseChan = make(chan bool)
		scm.ioChan = make(chan string, 1E4)
		if scm.ioTraceFile[len(scm.ioTraceFile)-2:] != "gz" {
			go common.WriteStringsCompressed(scm.ioTraceFile+".gz", scm.ioChan, scm.ioCloseChan)
		} else {
			go common.WriteStringsCompressed(scm.ioTraceFile, scm.ioChan, scm.ioCloseChan)
		}
	}

	/*scm.chunkIndex = NewSortedChunkIndex(scm.config.ChunkIndexPageSize, scm.config.IndexMemoryLimit, scm.ioChan)*/

	return true
}

// Creates a new SortedChunkIndexing instance. All instances creates by this function share the
// same ChunkIndex and BlockIndex.
func (scm *SimilarityChunkIndexingManager) CreateSortedChunkIndexing(scI *SortedChunkIndex) *SortedChunkIndexing {
	if scI == nil {
		cc := NewSortedChunkIndexing(scm.config,
			scm.avgChunkSize,
			nil,
			"",
			scm.ioChan)
		return cc
	} else {
		cc := NewSortedChunkIndexing(scm.config,
			scm.avgChunkSize,
			scI,
			"",
			scm.ioChan)
		return cc
		return cc
	}
}

// collects the statistics of all CC instances and returns a summary as well as a list of all statistics.
func (scm *SimilarityChunkIndexingManager) collectStatistic(ccs map[string]*SortedChunkIndexing) (SortedChunkIndexingStatistics, map[string]SortedChunkIndexingStatistics) {

	stats := make(map[string]SortedChunkIndexingStatistics)
	for host, cc := range ccs {
		stats[host] = cc.StatsList[len(cc.StatsList)-1]
	}

	var totalStats SortedChunkIndexingStatistics

	for _, s := range stats {
		totalStats.add(s)
		totalStats.FileNumber = s.FileNumber
	}

	totalStats.ChunkIndexStats.finish()

	totalStats.finish()

	for i := range scm.allCIs {
		scm.allCIs[i].Statistics.reset()
	}
	/*scm.chunkIndex.Statistics.reset()*/

	return totalStats, stats
}

// Merges two matrix entries and returns the merge result
func mergeMatrixEntries(a, b matrixEntry, numEntriesPerPage int) matrixEntry {

	var newEntry matrixEntry

	firstPage := make([]common.Chunk, 0, numEntriesPerPage)

	var i_index, j_index int
	// invariant: a.firstNChunks[i_index] and b.firstNChunks[j_index] are unqual!
	for i_index = 0; (i_index < len(a.firstNChunks)) && bytes.Equal(a.firstNChunks[i_index].Digest, b.firstNChunks[j_index].Digest); i_index++ {
	}

	// perform merge-sort until page is filled
	for (len(firstPage) < numEntriesPerPage) &&
		((i_index < len(a.firstNChunks)) ||
			(j_index < len(b.firstNChunks))) {

		switch {
		case (i_index < len(a.firstNChunks)) && (j_index < len(b.firstNChunks)): // both are NOT depleted
			if bytes.Compare(a.firstNChunks[i_index].Digest, b.firstNChunks[j_index].Digest) == -1 {
				firstPage = append(firstPage, a.firstNChunks[i_index])
				i_index++
				for (i_index < len(a.firstNChunks)) && bytes.Equal(a.firstNChunks[i_index].Digest, b.firstNChunks[j_index].Digest) {
					i_index++
				}
			} else {
				firstPage = append(firstPage, b.firstNChunks[j_index])
				j_index++
				for (j_index < len(b.firstNChunks)) && bytes.Equal(a.firstNChunks[i_index].Digest, b.firstNChunks[j_index].Digest) {
					j_index++
				}
			}

		case i_index < len(a.firstNChunks): // only b.firstNChunks is depleted
			firstPage = append(firstPage, a.firstNChunks[i_index])
			i_index++

		case j_index < len(b.firstNChunks): // only a.firstNChunks is depleted
			firstPage = append(firstPage, b.firstNChunks[j_index])
			j_index++
		}
	}

	newEntry.firstNChunks = firstPage
	newEntry.traces = make([]*traceTuple, 0)
	newEntry.traces = append(newEntry.traces, a.traces...)
	newEntry.traces = append(newEntry.traces, b.traces...)

	return newEntry
}

// predicts the average page utility for both streams if both get merged. ASSUMPTION: the chunks are sorted and unique in their list
func computePageUtility(chunksForI []common.Chunk, chunksForJ []common.Chunk, numEntriesPerPage int) (utilityI float32, utilityJ float32) {

	if len(chunksForI)+len(chunksForJ) == 0 {
		return 0, 0
	}

	// compute common first page
	var i_index, j_index int
	for i_index = 0; (i_index < len(chunksForI)) && bytes.Equal(chunksForI[i_index].Digest, chunksForJ[j_index].Digest); i_index++ {
	}

	firstPage := make([][]byte, 0, numEntriesPerPage)

	// perform merge-sort until page is filled
	// invariant: chunksForI[i_index] and chunksForI[j_index] are unqual!
	for (len(firstPage) < numEntriesPerPage) &&
		((i_index < len(chunksForI)) ||
			(j_index < len(chunksForJ))) {

		switch {
		case (i_index < len(chunksForI)) && (j_index < len(chunksForJ)): // both are NOT depleted
			if bytes.Compare(chunksForI[i_index].Digest, chunksForJ[j_index].Digest) == -1 {
				firstPage = append(firstPage, chunksForI[i_index].Digest)
				i_index++
				for (i_index < len(chunksForI)) && bytes.Equal(chunksForI[i_index].Digest, chunksForJ[j_index].Digest) {
					i_index++
				}
			} else {
				firstPage = append(firstPage, chunksForJ[j_index].Digest)
				j_index++
				for (j_index < len(chunksForJ)) && bytes.Equal(chunksForI[i_index].Digest, chunksForJ[j_index].Digest) {
					j_index++
				}
			}

		case i_index < len(chunksForI): // only chunksForJ is depleted
			firstPage = append(firstPage, chunksForI[i_index].Digest)
			i_index++

		case j_index < len(chunksForJ): // only chunksForI is depleted
			firstPage = append(firstPage, chunksForJ[j_index].Digest)
			j_index++
		}
	}

	// compute utility of stream i
	var iHits, jHits int
	i_index = 0
	j_index = 0

	for _, fp := range firstPage {
		// check/advance i
		var cmp int
		if i_index < len(chunksForI) {
			for cmp = bytes.Compare(chunksForI[i_index].Digest, fp); (cmp < 0) && (i_index < len(chunksForI)); { // while smaller than fp: advance
				i_index++
			}
			if cmp == 0 {
				iHits++
				i_index++
			}
		}

		// check/advance j
		if j_index < len(chunksForJ) {
			for cmp = bytes.Compare(chunksForJ[j_index].Digest, fp); (cmp < 0) && (j_index < len(chunksForJ)); { // while smaller than fp
				j_index++
			}
			if cmp == 0 {
				jHits++
				j_index++
			}
		}
	}

	// compute utilities and return
	utilityI = float32(float64(iHits) / float64(len(firstPage)))
	utilityJ = float32(float64(jHits) / float64(len(firstPage)))
	return
}

// Takes a list of traceTuples and computes a new list of trace groups. A traceGroup consists of several "similar"
// traceTuples. In contrast to the other "compute...Trace..." this function also compares the traces to existing chunk indices.
// Returns two values. The first is a mapping for traceTuple groups that should be run with an existing chunkIndex. The second one
// is a list of groups of traceTuples that should be run with a new one.
func (scm *SimilarityChunkIndexingManager) computeTraceGroups(ttuples []*traceTuple, threshold float32, numEntriesPerPage int) (map[int][]*traceTuple, [][]*traceTuple) {

	tracesWithOldIndex := make(map[int][]*traceTuple)
	tracesWithNewIndex := make([]*traceTuple, 0) // will hold all traceTuples that are unsimilar to existing traces

	/*var fpBuf [12]byte*/
	/*chunkFPs := make([]common.Chunk, 0)*/

	chunks := make([]common.Chunk, 0)
	for _, tuple := range ttuples {

		// build the list of the first chunks
		/*for i := range tuple.firstUniques {*/
		/*copy(fpBuf[:], tuple.firstUniques[i].Digest)*/
		/*chunkFPs = append(chunkFPs, fpBuf)*/
		/*}*/

		// check for best fit
		var ciFound = false
		var bestCI int
		var bestUtil float32
		for i := range scm.allCIs {

			firstPage := scm.allCIs[i].GetFirstPage()
			chunks = chunks[:0]
			for j := range firstPage {
				var c common.Chunk = common.Chunk{Digest: make([]byte, 12)}
				copy(c.Digest, firstPage[j][:])
				chunks = append(chunks, c)
			}

			utilStreamCI, utilCIStream := computePageUtility(tuple.firstUniques, chunks, scm.allCIs[i].getNumChunksPerPage())
			avgUtil := (utilStreamCI*float32((1/(1+scm.allCIsUsage[i]))) + utilCIStream*(float32(scm.allCIsUsage[i])/(float32(1+scm.allCIsUsage[i])))) / 2

			if (avgUtil >= bestUtil) && (avgUtil >= threshold) {
				ciFound = true
				bestCI = i
				bestUtil = avgUtil
			}
			/*log.Infof("util of stream %v to old CI %v: %v", tuple.hostname, i, scm.allCIs[i].predictPageUtility(chunkFPs))*/
			log.Infof("avgUtil of stream %v to old CI %v: %v; (stream->ci: %v, ci->stream: %v)", tuple.hostname, i, avgUtil, utilStreamCI, utilCIStream)
		}

		if ciFound {
			log.Infof("found old CI (%v) for new trace '%v', util is: %v", bestCI, tuple.hostname, bestUtil)

			if grp, ok := tracesWithOldIndex[bestCI]; ok {
				grp = append(grp, tuple)
				tracesWithOldIndex[bestCI] = grp
			} else {
				tracesWithOldIndex[bestCI] = []*traceTuple{tuple}
			}
		} else {
			tracesWithNewIndex = append(tracesWithNewIndex, tuple)
		}
	}

	// compute groups for yet unmapped ones
	var newGroups [][]*traceTuple = make([][]*traceTuple, 0)
	if len(tracesWithNewIndex) > 0 {
		newGroups = computeNewTraceGroups(tracesWithNewIndex, threshold, numEntriesPerPage)
		log.Infof("computed %v tracegroups for total %v yet unmapped traces ", len(newGroups), len(tracesWithNewIndex))
	} else {
		log.Info("All traces could be mapped to existing indices.")
	}

	return tracesWithOldIndex, newGroups
}

// Takes a list of traceTuples and computes a new list of trace groups. A traceGroup consists of several "similar" traceTuples.
func computeNewTraceGroups(ttuples []*traceTuple, threshold float32, numEntriesPerPage int) [][]*traceTuple {
	// The idea here is to express all predicted utilitiy values between the traces in a big matrix. The main
	// diagonal holds the currently merged traces (= their traceTuples and a representation of the common first page
	// in the chunk index). The other entries only hold at any time the predicted utility of the i-th and j-th
	// (merged-)stream IF both would be merged.

	names := make([]string, 0)
	for i := range ttuples {
		names = append(names, ttuples[i].hostname)
	}
	log.Infof("Compute trace groups out of %v traces: %v", len(ttuples), names)

	// init matrix
	var matrix [][]matrixEntry = make([][]matrixEntry, len(ttuples))
	for i := range matrix {
		matrix[i] = make([]matrixEntry, len(ttuples))
	}

	for i := range ttuples {
		matrix[i][i].firstNChunks = ttuples[i].firstUniques
		matrix[i][i].traces = []*traceTuple{ttuples[i]}
	}

	var maxUtil float32
	var maxUtil_i, maxUtil_j int

	// build up matrix
	for i := range matrix {
		// compute everything for ith row // TODO: 1 goroutine for each row
		for j := i + 1; j < len(matrix[i]); j++ {
			matrix[i][j].utility, matrix[j][i].utility = computePageUtility(matrix[i][i].firstNChunks, matrix[j][j].firstNChunks, numEntriesPerPage)
			avg := (matrix[i][j].utility + matrix[j][i].utility) / 2
			matrix[i][j].avgUtility = avg
			matrix[j][i].avgUtility = avg

			if (avg >= maxUtil) && (i != j) {
				maxUtil_i = i
				maxUtil_j = j
				maxUtil = avg
			}
		}
	}

	if maxUtil_i > maxUtil_j {
		maxUtil_i, maxUtil_j = maxUtil_j, maxUtil_i
	}

	log.Infof("maxUtil:%v", maxUtil)
	for (maxUtil >= threshold) && (maxUtil_i != maxUtil_j) {
		log.Infof("maxUtil:%v, merge %v and %v", maxUtil, maxUtil_i, maxUtil_j)
		// "merge" candidates // actually do temporal merge of first n uniques and use new list as base
		matrix[maxUtil_i][maxUtil_i] = mergeMatrixEntries(matrix[maxUtil_i][maxUtil_i], matrix[maxUtil_j][maxUtil_j], numEntriesPerPage)

		// shrink matrix. Done by deleting the jth row and jth column
		copy(matrix[maxUtil_j:len(matrix)-1], matrix[maxUtil_j+1:len(matrix)])
		matrix = matrix[:len(matrix)-1]

		for i := range matrix {
			copy(matrix[i][maxUtil_j:len(matrix[i])-1], matrix[i][maxUtil_j+1:len(matrix[i])])
			matrix[i] = matrix[i][:len(matrix[i])-1]
		}

		// the merged invalidaed the values in the corresponding row/column -> update matrix
		for j := 0; j < len(matrix[0]); j++ {
			if j == maxUtil_i {
				continue
			}
			util_ij, util_ji := computePageUtility(matrix[maxUtil_i][maxUtil_i].firstNChunks, matrix[j][j].firstNChunks, numEntriesPerPage)
			matrix[maxUtil_i][j].utility = util_ij * (float32(len(matrix[maxUtil_i][maxUtil_i].traces)) / float32(len(matrix[maxUtil_i][maxUtil_i].traces)+len(matrix[j][j].traces)))
			matrix[j][maxUtil_i].utility = util_ji * (float32(len(matrix[j][j].traces)) / float32(len(matrix[maxUtil_i][maxUtil_i].traces)+len(matrix[j][j].traces)))

			/*matrix[maxUtil_i][j].utility, matrix[j][maxUtil_i].utility = computePageUtility(matrix[maxUtil_i][maxUtil_i].firstNChunks, matrix[j][j].firstNChunks, numEntriesPerPage)*/

			avg := (matrix[maxUtil_i][j].utility + matrix[j][maxUtil_i].utility) / 2
			matrix[maxUtil_i][j].avgUtility = avg
			matrix[j][maxUtil_i].avgUtility = avg
		}

		maxUtil = 0
		maxUtil_i = 0
		maxUtil_j = 0

		// recompute max
		for i := 0; i < len(matrix); i++ {
			for j := 0; j < len(matrix[i]); j++ {
				if matrix[i][j].avgUtility > maxUtil {
					maxUtil_i = i
					maxUtil_j = j
					maxUtil = matrix[i][j].avgUtility
				}
			}
		}
	}

	log.Infof("no merge after reaching maxUtil: %v", maxUtil)

	// The merged traces are on the main diagonal. Collect them and return.
	tuples := make([][]*traceTuple, 0, len(matrix))
	for i := range matrix {
		tuples = append(tuples, matrix[i][i].traces)
	}

	return tuples
}

// Runs the algorithm for a given stream group (== the same chunk index). Returns a map of created handler
// in the given channel.
func (scm *SimilarityChunkIndexingManager) runStreamGroup(genNum int, group []*traceTuple, ret chan<- map[string]*SortedChunkIndexing) {

	log.Infof("Start group with %v streams", len(group))
	usedSCs := make(map[string]*SortedChunkIndexing, 0)

	// now we have mapping chunk index -> [...] list of streams
	// for each group: create an index and start it
	wg := new(sync.WaitGroup)
	wg.Add(len(group))
	var cindex *SortedChunkIndex
	if id, ok := scm.ciMap[group[0].hostname]; ok {
		cindex = scm.allCIs[id]
	} else {
		cindex = NewSortedChunkIndex(scm.config.ChunkIndexPageSize, scm.config.IndexMemoryLimit, scm.ioChan)
		scm.ciMutex.Lock()
		scm.allCIs = append(scm.allCIs, cindex)
		scm.allCIsUsage = append(scm.allCIsUsage, len(group))
		for _, tt := range group {
			scm.ciMap[tt.hostname] = len(scm.allCIs) - 1
			log.Infof("Added ID of new index %v for stream %v", scm.ciMap[tt.hostname], tt.hostname)
		}
		scm.ciMutex.Unlock()
	}

	// build handler
	// store cindex for later usage
	for i := range group {
		group[i].firstUniques = nil

		handler := scm.CreateSortedChunkIndexing(cindex)
		handler.BeginTraceFile(genNum, group[i].hostname)
		usedSCs[group[i].hostname] = handler

		log.Info("gen ", genNum, ": processing host: ", group[i].hostname)

		go func(tt *traceTuple, algoHandler *SortedChunkIndexing, wgroup *sync.WaitGroup) {
			if tt.firstChunks != nil {
				algoHandler.handleChunks(tt.firstChunks)
				tt.firstChunks = nil
			}

			algoHandler.HandleFiles(tt.stream, tt.reader.GetFileEntryReturn())
			algoHandler.EndTraceFile()
			wgroup.Done()
		}(group[i], handler, wg)
	}

	wg.Wait()
	ret <- usedSCs
}

// Runs a single generation
func (scm *SimilarityChunkIndexingManager) RunGeneration(genNum int, fileInfoList []algocommon.TraceFileInfo) {
	wg := new(sync.WaitGroup)
	newTraces := make([]*traceTuple, 0)
	indexToGroupsMapping := make(map[int][]*traceTuple) // collects all traceTuples in this genration for a given chunkIndex

	// 1: get chunks for new streams and start feeding
	// start feeding and determine new streams
	for _, fileInfo := range fileInfoList {
		var hostname string

		// start new generation and create data structures if necesary
		if strings.Contains(scm.config.DataSet, "multi") { // if not parallel run
			var splits []string = strings.Split(fileInfo.Name, "_")
			hostname = strings.Split(splits[len(splits)-1], "-")[0]
		} else {
			hostname = "host0"
		}
		log.Info("gen ", genNum, ": processing host: ", hostname)

		t := &traceTuple{
			hostname:     hostname,
			stream:       make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries),
			reader:       algocommon.NewTraceDataReader(fileInfo.Path),
			firstUniques: make([]common.Chunk, 0),
			firstChunks:  make([]common.Chunk, 0),
		}
		go t.reader.FeedAlgorithm(t.stream)

		if id, ok := scm.ciMap[t.hostname]; ok { // is it an old stream?
			// yes, add to indexToGroupsMapping
			if list, k := indexToGroupsMapping[id]; k {
				list = append(list, t)
				indexToGroupsMapping[id] = list
			} else {
				indexToGroupsMapping[id] = []*traceTuple{t}
			}

		} else { // new trace -> get first chunks
			wg.Add(1)
			newTraces = append(newTraces, t)
			go func(tt *traceTuple, waitgrp *sync.WaitGroup) {
				defer waitgrp.Done()
				t.firstChunks, t.firstUniques = scm.getFirstNChunks(1000, t.stream, t.reader.GetFileEntryReturn())
			}(t, wg)
		}
	}

	wg.Wait()

	// 2) We have seperated old from new. Now compute/set the best chunk index for the new ones
	oldGroups, newGroups := scm.computeTraceGroups(newTraces, scm.config.SimilarityThreshold, scm.config.ChunkIndexPageSize/ciEntrySize)

	// merge groups for existing indices
	for id, group := range oldGroups {
		if list, ok := indexToGroupsMapping[id]; ok {
			list = append(list, group...)
			indexToGroupsMapping[id] = list
		} else {
			indexToGroupsMapping[id] = list
		}
	}

	// 3) Run all groups
	usedHandler := make(chan map[string]*SortedChunkIndexing, len(newGroups)+len(indexToGroupsMapping))

	for i := range newGroups {
		go func(gNum int, group []*traceTuple, waitGroup *sync.WaitGroup, ret chan<- map[string]*SortedChunkIndexing) {
			scm.runStreamGroup(gNum, group, ret)
		}(genNum, newGroups[i], wg, usedHandler)
	}

	for id, oldGrp := range indexToGroupsMapping {
		log.Info("start handler for old chunk index", id)
		go func(gNum int, group []*traceTuple, waitGroup *sync.WaitGroup, ret chan<- map[string]*SortedChunkIndexing) {
			scm.runStreamGroup(gNum, group, ret)
		}(genNum, oldGrp, wg, usedHandler)
	}

	usedSCs := make(map[string]*SortedChunkIndexing, 0)
	for i := 0; i < len(newGroups)+len(indexToGroupsMapping); i++ {
		hm := <-usedHandler
		for k, v := range hm {
			usedSCs[k] = v
		}
	}

	log.Infof("ci usage distribution: %v", scm.allCIsUsage)

	// all joined => all are finished
	scm.finishGeneration(genNum, usedSCs)
}

// Performs the maintances work after a generation, i.e., it flushes the indices and collectes the statistics.
func (scm *SimilarityChunkIndexingManager) finishGeneration(genNum int, usedSCs map[string]*SortedChunkIndexing) {
	wg := new(sync.WaitGroup)
	wg.Add(len(scm.allCIs))
	// flush memindex to disk one
	for id, ci := range scm.allCIs {
		log.Infof("trigger flushing chunk index %v ...", id)
		go func(index *SortedChunkIndex, waitGrp *sync.WaitGroup) {
			index.Flush(nil) // no locking here because this is the only active goroutine that accesses the index at this point
			waitGrp.Done()
		}(ci, wg)
	}

	wg.Wait()

	// collect stats
	log.Info("Collecting statistics")
	totalStats, ccStats := scm.collectStatistic(usedSCs)
	ccStats["total"] = totalStats
	scm.StatsList = append(scm.StatsList, ccStats)

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
func (scm *SimilarityChunkIndexingManager) Quit() {

	if scm.ioCloseChan != nil {
		close(scm.ioChan)
		log.Info("wait for ioTraceRoutine")
		<-scm.ioCloseChan
	}

	if scm.outputFilename == "" {
		return
	}

	jsonMap := make(map[string]interface{})
	for i, generationStats := range scm.StatsList {
		jsonMap["generation "+strconv.Itoa(i)] = generationStats
	}

	if encodedStats, err := json.MarshalIndent(jsonMap, "", "    "); err != nil {
		log.Error("Couldn't marshal results: ", err)
	} else if f, err := os.Create(scm.outputFilename); err != nil {
		log.Error("Couldn't create results file: ", err)
	} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {
		log.Error("Couldn't write to results file: ", err)
	} else {
		log.Info(bytes.NewBuffer(encodedStats).String())
		f.Close()
	}
}

// extracts chunks until it found N unique ones. Returns two lists. The first contains all chunks from all used fileEntries (i.e. with duplicates), the second contains only the first N unique ones. The order of the chunk is the same as in the fileEntry chan.
func (scm *SimilarityChunkIndexingManager) getFirstNChunks(num int, fentries <-chan *algocommon.FileEntry, fileEntryReturn chan<- *algocommon.FileEntry) ([]common.Chunk, []common.Chunk) {

	allChunks := make([]common.Chunk, 0, num)
	uniqueChunks := make([]common.Chunk, 0, num)

	var lastFp [12]byte
	var currentFp [12]byte
	lastFp[1] = 42

	for len(uniqueChunks) < num {
		f, ok := <-fentries
		if !ok {
			log.Warnf("emptied channel while getting first %v chunks", num)
			return allChunks, uniqueChunks
		}

		for i := range f.Chunks {
			copy(currentFp[:], f.Chunks[i].Digest)
			allChunks = append(allChunks, common.Chunk{})
			allChunks[len(allChunks)-1].CopyFrom(&f.Chunks[i])

			if (len(uniqueChunks) < num) && (!bytes.Equal(lastFp[:], currentFp[:])) { // not finished and found a new unique one
				copy(lastFp[:], currentFp[:])
				uniqueChunks = append(uniqueChunks, common.Chunk{})
				uniqueChunks[len(uniqueChunks)-1].CopyFrom(&f.Chunks[i])
			}

		}
		fileEntryReturn <- f
	}

	return allChunks, uniqueChunks
}
