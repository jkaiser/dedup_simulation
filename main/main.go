package main

import "fmt"
import "flag"
import "time"
import "os"
import "io/ioutil"
import "bytes"
import "encoding/json"
import "sync"
import "runtime"
import "runtime/pprof"
import log "github.com/cihub/seelog"
import "net/http"
import _ "net/http/pprof"

import "github.com/jkaiser/dedup_simulations/common"
import algocommon "github.com/jkaiser/dedup_simulations/dedupAlgorithms/common"
import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/blc"
import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/sparseIndexing"
import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/containerCaching"
import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/analysis"
import "github.com/jkaiser/dedup_simulations/dedupAlgorithms/sortedChunkIndexing"

func setupLogger(debug bool) {
	var testConfig string
	if debug {
		testConfig = `
<seelog type="sync">
    <outputs formatid="main">
        <filter levels="debug">
            <console/>
        </filter>
        <filter levels="info">
            <console/>
        </filter>
        <filter levels="error">
            <console/>
        </filter>
        <filter levels="warn">
            <console/>
        </filter>
        <filter levels="critical">
            <console/>
        </filter>
    </outputs>
    <formats>
        <format id="main" format="%Date %Time [%Level] %Msg%n"/>
    </formats>
</seelog>`

	} else {
		testConfig = `
<seelog type="sync">
    <outputs formatid="main">
        <filter levels="info">
            <console/>
        </filter>
        <filter levels="error">
            <console/>
        </filter>
        <filter levels="warn">
            <console/>
        </filter>
        <filter levels="critical">
            <console/>
        </filter>
    </outputs>
    <formats>
        <format id="main" format="%Date %Time [%Level] %Msg%n"/>
    </formats>
</seelog>`
	}

	if logger, err := log.LoggerFromConfigAsBytes([]byte(testConfig)); err != nil {
		fmt.Println(err)
	} else {
		if loggerErr := log.ReplaceLogger(logger); loggerErr != nil {
			fmt.Println(loggerErr)
		}
	}
}

func reportMemUsage(interval time.Duration) {

	const layout = "3:04pm"
	tickChan := time.Tick(interval)
	log.Flush()
	for {
		<-tickChan
		mStats := new(runtime.MemStats)
		runtime.ReadMemStats(mStats)
		log.Info("totalAlloc: ", mStats.TotalAlloc/(1024*1024), "MB; used: ", mStats.Alloc/(1024*1024), "MB; from system: ", mStats.Sys/(1024*1024), "MB; lastGC: ", time.Unix(0, int64(mStats.LastGC)).Format(layout), "; nextGC at: ", mStats.NextGC/(1024*1024), "MB")
	}
}

// runs the show for sparse-indexing
func executeSparseIndexing(dataDir string, configFile string, clientServers string, outputFile string, ioTraceFile string) {

	// read config
	log.Info("read config")
	var config sparseIndexing.SparseIndexingConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var siManager *sparseIndexing.SparseIndexingManager = sparseIndexing.NewSparseIndexingManager(config, algocommon.GetDataSetInfo(dataDir, config.DataSet).AverageChunkSize, clientServers, outputFile, ioTraceFile)
	if !siManager.Init() {
		log.Error("Error during siManager initialization")
		return
	}

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		siManager.RunGeneration(generation, fileInfoList)
	}

	siManager.Quit()
}

// runs the show for sampling-container-caching
func executeSamplingContainerCaching(dataDir string, configFile string, clientServers string, outputFile string, ioTraceFile string) {

	// read config
	log.Info("read config")
	var config containerCaching.SamplingContainerCachingConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var ccManager *containerCaching.SamplingContainerCachingManager = containerCaching.NewSamplingContainerCachingManager(config, algocommon.GetDataSetInfo(dataDir, config.DataSet).AverageChunkSize, clientServers, outputFile, ioTraceFile)
	if !ccManager.Init() {
		log.Error("Error during sccManager initialization")
		return
	}

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		ccManager.RunGeneration(generation, fileInfoList)
	}

	ccManager.Quit()
}

// runs the show for container-caching
func executeContainerCaching(dataDir string, configFile string, clientServers string, outputFile string, ioTraceFile string) {

	// read config
	log.Info("read config")
	var config containerCaching.ContainerCachingConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var ccManager *containerCaching.ContainerCachingManager = containerCaching.NewContainerCachingManager(config, algocommon.GetDataSetInfo(dataDir, config.DataSet).AverageChunkSize, clientServers, outputFile, ioTraceFile)
	if !ccManager.Init() {
		log.Error("Error during ccManager initialization")
		return
	}

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		ccManager.RunGeneration(generation, fileInfoList)
	}

	ccManager.Quit()
}

// runs the show for sorted-ChunkIndexing
func executeSortedChunkIndexing(dataDir string, configFile string, clientServers string, outputFile string, ioTraceFile string) {

	// read config
	log.Info("read config")
	var config sortedChunkIndexing.SortedChunkIndexingConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var scManager *sortedChunkIndexing.SortedChunkIndexingManager = sortedChunkIndexing.NewSortedChunkIndexingManager(config, algocommon.GetDataSetInfo(dataDir, config.DataSet).AverageChunkSize, clientServers, outputFile, ioTraceFile)
	if !scManager.Init() {
		log.Error("Error during sciManager initialization")
		return
	}

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		scManager.RunGeneration(generation, fileInfoList)
	}

	scManager.Quit()
}

// runs the show for sorted-MergeCIPageUsage
func executeSortedMergeCIPageUsage(dataDir string, configFile string, clientServers string, outputFile string, ioTraceFile string) {

	// read config
	log.Info("read config")
	var config sortedChunkIndexing.SortedMergeCIPageUsageConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var scManager *sortedChunkIndexing.SortedMergeCIPageUsageManager = sortedChunkIndexing.NewSortedMergeCIPageUsageManager(config, algocommon.GetDataSetInfo(dataDir, config.DataSet).AverageChunkSize, clientServers, outputFile, ioTraceFile)
	if !scManager.Init() {
		log.Error("Error during sciManager initialization")
		return
	}

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	allFileInfos := traceDataSet.GetFileInfos()
	for gen := 0; gen < config.NumGenerationsBeforeSim; gen++ {
		scManager.RunGeneration(gen, allFileInfos[gen], true)
	}

	log.Infof("Now run page size simulation as generation %v with %v input files", config.NumGenerationsBeforeSim, len(allFileInfos[config.NumGenerationsBeforeSim]))
	scManager.RunPSSimulation(config.NumGenerationsBeforeSim, allFileInfos[config.NumGenerationsBeforeSim])

	scManager.Quit()
}

// runs the show for sorted-MergeChunkIndexing
func executeSortedMergeChunkIndexing(dataDir string, configFile string, clientServers string, outputFile string, ioTraceFile string) {

	// read config
	log.Info("read config")
	var config sortedChunkIndexing.SortedMergeChunkIndexingConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var scManager *sortedChunkIndexing.SortedMergeChunkIndexingManager = sortedChunkIndexing.NewSortedMergeChunkIndexingManager(config, algocommon.GetDataSetInfo(dataDir, config.DataSet).AverageChunkSize, clientServers, outputFile, ioTraceFile)
	if !scManager.Init() {
		log.Error("Error during sciManager initialization")
		return
	}

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		scManager.RunGeneration(generation, fileInfoList)
	}

	scManager.Quit()
}

// runs the show for similarity-ChunkIndexing
func executeSimilarityChunkIndexing(dataDir string, configFile string, clientServers string, outputFile string, ioTraceFile string) {

	// read config
	log.Info("read config")
	var config sortedChunkIndexing.SortedChunkIndexingConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var scManager *sortedChunkIndexing.SimilarityChunkIndexingManager = sortedChunkIndexing.NewSimilarityChunkIndexingManager(config, algocommon.GetDataSetInfo(dataDir, config.DataSet).AverageChunkSize, clientServers, outputFile, ioTraceFile)
	if !scManager.Init() {
		log.Error("Error during simCIManager initialization")
		return
	}

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		scManager.RunGeneration(generation, fileInfoList)
	}

	scManager.Quit()
}

// runs the show for block-index-filter
func executeBlockIndexFilter(dataDir string, configFile string, clientServers string, outputFile string, ioTraceFile string) {

	// read config
	log.Info("read config")
	var config blc.BlockIndexFilterConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var blcManager *blc.BlockIndexFilterManager = blc.NewBlockIndexFilterManager(config, algocommon.GetDataSetInfo(dataDir, config.DataSet).AverageChunkSize, clientServers, outputFile, ioTraceFile)
	if !blcManager.Init() {
		log.Error("Error during blcManager initialization")
		return
	}

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		blcManager.RunGeneration(generation, fileInfoList)
	}

	blcManager.Quit()
}

// runs the show for sampling-block-index-filter
func executeSamplingBlockIndexFilter(dataDir string, configFile string, clientServers string, outputFile string, ioTraceFile string) {

	// read config
	log.Info("read config")
	var config blc.SamplingBlockIndexFilterConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var sblcManager *blc.SamplingBlockIndexFilterManager = blc.NewSamplingBlockIndexFilterManager(config, algocommon.GetDataSetInfo(dataDir, config.DataSet).AverageChunkSize, clientServers, outputFile, ioTraceFile)
	if !sblcManager.Init() {
		log.Error("Error during sblcManager initialization")
		return
	}

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		sblcManager.RunGeneration(generation, fileInfoList)
	}

	sblcManager.Quit()
}

func executeFullFile(dataDir string, configFile string, outputFile string) {

	// read config
	log.Info("read config")
	var config analysis.FullFileConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	fullFileHandler := analysis.NewFullFileHandler(outputFile)
	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)

	// run it
	executeAlgorithm(dataDir, traceDataSet, fullFileHandler)
}

// runs the show for stream-analysis. The difference between StreamAnalysis and StreamSimilarityTrace is
// that the similarityTrace only is about generating a new trace that reflects the similarity of the
// input traces. The StreamAnalysis is a general analysis.
func executeStreamAnalysis(dataDir string, configFile string, outputFile string) {

	// read config
	log.Info("read config")
	var config analysis.StreamAnalysisConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var saManager *analysis.StreamAnalysisManager = analysis.NewStreamAnalysisManager(config, outputFile)
	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		saManager.RunGeneration(generation, fileInfoList)
	}

	saManager.Quit()
}

// runs the show for similarity-trace generation
func executeStreamSimilarityTrace(dataDir string, configFile string, outputFile string) {

	// read config
	log.Info("read config")
	var config analysis.StreamSimilarityConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var i interface{} = config
	if checker, ok := i.(common.SanityChecker); ok {
		checker.SanityCheck()
	}

	var ssManager *analysis.StreamSimilarityManager = analysis.NewStreamSimilarityManager(config, outputFile)

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		ssManager.RunGeneration(generation, fileInfoList)
	}

	ssManager.Quit()
}

// runs the show for streamShare
func executeStreamShareAnalysis(dataDir string, configFile string, outputFile string) {

	// read config
	log.Info("read config")
	var config analysis.StreamSimilarityConfig
	f, _ := os.Open(configFile)
	if data, err := ioutil.ReadAll(f); err != nil {
		log.Error("Couldn't read config file: ", err)
		return
	} else if err = json.Unmarshal(data, &config); err != nil {
		log.Error("Couldn't unmarshal config: ", err)
		return
	} else {
		log.Debug("config is: ", bytes.NewBuffer(data).String())
	}

	var ssManager *analysis.StreamShareManager = analysis.NewStreamShareManager(config, outputFile)

	traceDataSet := algocommon.NewTraceDataSet(dataDir, config.DataSet)
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {
		ssManager.RunGeneration(generation, fileInfoList)
	}

	ssManager.Quit()
}

// Executes the given algorithm and feeds it with tracedata.
func executeAlgorithm(dataDir string, traceDataSet *algocommon.TraceDataSet, algo algocommon.FileDataHandler) {

	var traceReader algocommon.TraceDataReader
	for generation, fileInfoList := range traceDataSet.GetFileInfos() {

		filePath := fileInfoList[0].Path
		fileEntryChan := make(chan *algocommon.FileEntry, algocommon.ConstMaxFileEntries)

		if traceReader == nil {
			traceReader = algocommon.NewTraceDataReader(filePath)
		} else {
			traceReader.Reset(filePath)
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			algo.BeginTraceFile(generation, filePath)
			go traceReader.FeedAlgorithm(fileEntryChan)
			algo.HandleFiles(fileEntryChan, traceReader.GetFileEntryReturn())
			algo.EndTraceFile(generation)
		}()
		wg.Wait()
	}
	// finish
	algo.Quit()
}

func main() {
	defer log.Flush()

	data_dir := flag.String("data_dir", "", "The directory containing the trace files.")
	tick_interval := flag.Int("report_interval", 0, "The interval (in minutes) the memory usage is reported. 0 disables reporting.")
	algorithm := flag.String("algorithm", "sampling-block-index-filter", "The algorithm to run.")
	configFile := flag.String("config", "", "The configuration file for the chosen algorithm.")
	resultsFile := flag.String("out", "", "The outputfile for the simulation results.")
	clientServers := flag.String("clientservers", "", "The adresses of the memcacheservers for the memcache Client.")
	ioTraceFile := flag.String("ioTrace", "", "The output file the simulation will write the ioTrace into.")
	debug := flag.Bool("debug", false, "Enables full debug output.")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write memory profile to this file")
	liveprofile := flag.Bool("liveprofile", false, "Provide a live profile (CPU + Mem + Block). See http://blog.golang.org/profiling-go-programs for details.")
	flag.Parse()

	setupLogger(*debug)

	// check configuration file
	if *configFile == "" {
		log.Critical("no configuration given")
		return
	} else if fi, err := os.Stat(*configFile); err != nil {
		log.Critical("file does not exists ", err)
		return
	} else if !fi.Mode().IsRegular() {
		log.Critical("config file", *configFile, " isn't regular file")
		return
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Critical(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *tick_interval > 0 {
		go reportMemUsage(time.Duration(*tick_interval) * time.Minute)
	}

	if *liveprofile {
		go func() {
			log.Info(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	// start algorithm
	switch *algorithm {
	case "sampling-block-index-filter":
		executeSamplingBlockIndexFilter(*data_dir, *configFile, *clientServers, *resultsFile, *ioTraceFile)
	case "block-index-filter":
		executeBlockIndexFilter(*data_dir, *configFile, *clientServers, *resultsFile, *ioTraceFile)
	case "sampling-container-caching":
		executeSamplingContainerCaching(*data_dir, *configFile, *clientServers, *resultsFile, *ioTraceFile)
	case "container-caching":
		executeContainerCaching(*data_dir, *configFile, *clientServers, *resultsFile, *ioTraceFile)
	case "sparse-indexing":
		executeSparseIndexing(*data_dir, *configFile, *clientServers, *resultsFile, *ioTraceFile)
	case "sorted-chunk-indexing":
		executeSortedChunkIndexing(*data_dir, *configFile, *clientServers, *resultsFile, *ioTraceFile)
	case "sorted-merge-chunk-indexing":
		executeSortedMergeChunkIndexing(*data_dir, *configFile, *clientServers, *resultsFile, *ioTraceFile)
	case "sorted-merge-chunk-indexing-page-usage":
		executeSortedMergeCIPageUsage(*data_dir, *configFile, *clientServers, *resultsFile, *ioTraceFile)
	case "similarity-chunk-indexing":
		executeSimilarityChunkIndexing(*data_dir, *configFile, *clientServers, *resultsFile, *ioTraceFile)
	case "full-file":
		executeFullFile(*data_dir, *configFile, *resultsFile)
	case "stream-analysis":
		executeStreamAnalysis(*data_dir, *configFile, *resultsFile)
	case "stream-similarity-trace":
		executeStreamSimilarityTrace(*data_dir, *configFile, *resultsFile)
	case "stream-share-analysis":
		executeStreamShareAnalysis(*data_dir, *configFile, *resultsFile)
	default:
		log.Error("Don't know algorithm '", *algorithm, "'")
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Critical(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
		return
	}
}
