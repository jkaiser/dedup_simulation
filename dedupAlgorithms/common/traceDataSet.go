package common

import "fmt"
import "os"
import "path"
import "io/ioutil"
import "regexp"
import "strings"
import "strconv"
import "sort"
import log "github.com/cihub/seelog"

type TraceFileInfo struct {
	Name string
	Path string
	Id   int64
}
type TraceFileInfoList []TraceFileInfo

// helper function to fulfill the Sort interface
func (list TraceFileInfoList) Less(i, j int) bool {
	return list[i].Id < list[j].Id
}

func (list TraceFileInfoList) Len() int {
	return len(list)
}
func (list TraceFileInfoList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

// Struct and representation of a Trace data set build by the fs-c tool
// from Dirk Meister.
type TraceDataSet struct {
	traceFilesInfos TraceFileInfoList
	ChunkSize       int
}

type DataSetInfo struct {
	Regex            *regexp.Regexp
	Reverse          bool
	AverageChunkSize int // in bytes
}

// Central function to get basic information about a given dataSet. Returns
func GetDataSetInfo(baseDataDirectory string, dataSet string) *DataSetInfo {
	if len(dataSet) == 0 {
		log.Critical("no dataSet given")
		return nil
	}

	jguRegex, err := regexp.Compile("(ma.+)_w([0-9]+)_r[0-9]+-cdc([0-9]+)")
	if err != nil {
		log.Critical(err)
		panic("")
	}
	microsoftRegex, err := regexp.Compile("(ms)_w([0-9]+)-cdc([0-9]+)")
	if err != nil {
		log.Critical(err)
		panic("")
	}
	multiRegex, err := regexp.Compile("(gen)_([0-9]+)_(.+)")
	if err != nil {
		log.Critical(err)
		panic("")
	}
	//chunks-upb-12-rabin8
	imtRegex, err := regexp.Compile("(chunks-upb)-([0-9]+)-rabin8")
	if err != nil {
		log.Critical(err)
		panic("")
	}

	infoMap := map[string]*DataSetInfo{"jgu4": &DataSetInfo{jguRegex, true, 8 * 1024},
		"jgu4r":        &DataSetInfo{jguRegex, true, 8 * 1024},
		"jgu20_cdc2r":  &DataSetInfo{jguRegex, true, 2 * 1024},
		"jgu20_cdc4r":  &DataSetInfo{jguRegex, true, 4 * 1024},
		"jgu20_cdc8r":  &DataSetInfo{jguRegex, true, 8 * 1024},
		"jgu20_cdc16r": &DataSetInfo{jguRegex, true, 16 * 1024},
		"ms_cdc8r":     &DataSetInfo{microsoftRegex, false, 8 * 1024},
		"ms_cdc16r":    &DataSetInfo{microsoftRegex, false, 16 * 1024},
		"multi":        &DataSetInfo{multiRegex, false, 8 * 1024},
		"imt":          &DataSetInfo{imtRegex, false, 8 * 1024},
	}

	if strings.HasPrefix(dataSet, "multi") {
		return infoMap["multi"]
	} else if info, ok := infoMap[dataSet]; ok {
		return info
	} else {
		log.Critical("Don't know data set: ", dataSet)
		return new(DataSetInfo)
	}
}

func NewTraceDataSet(baseDataDirectory string, dataSet string) *TraceDataSet {

	fullPath := path.Join(baseDataDirectory, dataSet)
	dsInfo := GetDataSetInfo(baseDataDirectory, dataSet)

	// get files
	if _, err := os.Stat(fullPath); err != nil {
		log.Critical(fmt.Sprintf("could not get info for directory %v. err: ", baseDataDirectory), err)
		panic(err)
	}

	files, err := ioutil.ReadDir(fullPath)
	if err != nil {
		panic(err)
	}

	// sort
	fdList := make(TraceFileInfoList, 0)
	for _, fileInfo := range files {

		if s := dsInfo.Regex.FindStringSubmatch(fileInfo.Name()); !fileInfo.IsDir() && len(s) > 0 {
			if id, err := strconv.ParseInt(s[2], 0, 0); err != nil {
				panic(err)
			} else {
				fdList = append(fdList, TraceFileInfo{fileInfo.Name(), path.Join(fullPath, fileInfo.Name()), id})
			}
		}
	}

	sort.Sort(fdList)
	if len(fdList) > 0 && !dsInfo.Reverse {
		return &TraceDataSet{fdList, dsInfo.AverageChunkSize}
	} else { // file ordering must be reversed
		reverseList := make(TraceFileInfoList, len(fdList))
		for i, _ := range fdList {
			reverseList[i] = fdList[len(fdList)-i-1]
		}

		return &TraceDataSet{reverseList, dsInfo.AverageChunkSize}
	}
}

// Returns the TraceFileInfos of all data files of this data set
func (t *TraceDataSet) GetFileInfos() [][]TraceFileInfo {

	files := make([][]TraceFileInfo, 0)

	currentId := int64(-1)

	for _, traceFileInfo := range t.traceFilesInfos {
		if currentId != traceFileInfo.Id {
			files = append(files, make([]TraceFileInfo, 0))
			currentId = traceFileInfo.Id
		}
		files[len(files)-1] = append(files[len(files)-1], traceFileInfo)
	}
	return files
}
