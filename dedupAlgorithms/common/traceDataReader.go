package common

import log "github.com/cihub/seelog"
import "strings"

const ConstMaxFileEntries = 30

type TraceDataReader interface {
	GetFileEntryReturn() chan *FileEntry
	Close()
	Reset(path string)
	FeedAlgorithm(outputChan chan<- *FileEntry)
}

func NewTraceDataReader(path string) TraceDataReader {

	split := strings.Split(path, "/")
	legacy := false
	for i := range split {
		if split[i] == "imt" {
			legacy = true
			log.Info("Legacyformat detected. Using LegacyTraceReader.")
			break
		}
	}

	if legacy {
		return NewLegacyTraceDataReader(path)

	} else {
		return NewProtoTraceDataReader(path)
	}
}
