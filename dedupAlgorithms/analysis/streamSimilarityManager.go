package analysis

import "os"
import "bytes"
import "strings"
import "strconv"
import log "github.com/cihub/seelog"
import "encoding/json"

import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"

// Kind of father struct that manages the execution of several StreamSimilarity instances
type StreamSimilarityManager struct {
	config        StreamSimilarityConfig
	finalStatsOut string
	allStats      map[string]TraceSimilarityStatistics
}

func NewStreamSimilarityManager(config StreamSimilarityConfig, finalStatsOut string) *StreamSimilarityManager {
	s := new(StreamSimilarityManager)
	s.config = config
	s.finalStatsOut = finalStatsOut
	s.allStats = make(map[string]TraceSimilarityStatistics)
	return s
}

// Runs the similarity-trace generation for a single backup generation. This function is
// thread-safe.
func (ssm *StreamSimilarityManager) RunGeneration(genNum int, fileInfoList []algocommon.TraceFileInfo) {

	genName := "gen" + strconv.Itoa(genNum)

	statsOutFilename := ssm.finalStatsOut + "_" + genName + "_stats"
	traceOutFilename := ssm.finalStatsOut + "_" + genName + "_data"
	fileEntries := make([]<-chan *algocommon.FileEntry, 0)
	fileEntryReturn := make([]chan<- *algocommon.FileEntry, 0)

	ssh := NewStreamSimilarityTraceHandler(&ssm.config, traceOutFilename, statsOutFilename)

	// prepare input
	for _, fileInfo := range fileInfoList {

		var hostname string

		// start new generation and create data structures if necesary
		if strings.Contains(ssm.config.DataSet, "multi") { // if not parallel run
			var splits []string = strings.Split(fileInfo.Name, "_")
			hostname = strings.Split(splits[len(splits)-1], "-")[0]
		} else {
			hostname = "host0"
		}
		log.Info("gen ", genNum, ": processing host: ", hostname)

		traceReader := algocommon.NewTraceDataReader(fileInfo.Path)
		fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)
		fileEntries = append(fileEntries, fileEntryChan)
		fileEntryReturn = append(fileEntryReturn, traceReader.GetFileEntryReturn())
		go traceReader.FeedAlgorithm(fileEntryChan)
	}

	ssh.BeginTraceFile(genNum)
	ssh.HandleFiles(fileEntries, fileEntryReturn)
	ssm.allStats[genName] = ssh.Statistics
	ssh.Quit()
	/*ssh.EndTraceFile(genNum)*/

	/*// print/write statistics*/
	/*if encodedStats, err := json.MarshalIndent(ssh.Statistics, "", "    "); err != nil {*/
	/*log.Error("Couldn't marshal statistics: ", err)*/
	/*} else if f, err := os.Create(statsOutFilename); err != nil {*/
	/*log.Error("Couldn't create results file: ", err)*/
	/*} else if n, err := f.Write(encodedStats); (n != len(encodedStats)) || err != nil {*/
	/*log.Error("Couldn't write to results file: ", err)*/
	/*} else {*/
	/*log.Info("Generation ", genNum, " statistics:")*/
	/*log.Info(bytes.NewBuffer(encodedStats).String())*/
	/*}*/
}

// Writes the final statistics and cleans up if necesary
func (ssm *StreamSimilarityManager) Quit() {

	if ssm.finalStatsOut == "" {
		return
	}

	if encodedStats, err := json.MarshalIndent(ssm.allStats, "", "    "); err != nil {
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
