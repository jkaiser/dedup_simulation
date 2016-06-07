package common

import "os"
import "fmt"
import "bufio"
import "compress/gzip"
import log "github.com/cihub/seelog"

// Interface for general sanity checks.
type SanityChecker interface {
	// checks the struct and prints warnings to the logging framework
	SanityCheck()
}

/* Writes all integers in the channel to the given file. When the channel is closed, it closes the file and sends a bool to the closeSignal chan. */
func WriteIntegers(path string, accesses <-chan uint32, closeSignal chan<- bool) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Error("couldn't open output file: ", err)
		closeSignal <- false
		return
	}

	bufWriter := bufio.NewWriterSize(f, 4*1024*1024)

	for io := range accesses {
		if _, err := bufWriter.WriteString(fmt.Sprintf("%v\n", io)); err != nil {
			bufWriter.Flush()
			f.Close()
			panic("Couldn't write io trace entry")
		}
	}

	bufWriter.Flush()
	f.Close()

	closeSignal <- true
}

/* Writes all strings in the channel to the given file. When the channel is closed, it closes the file and sends a bool to the closeSignal chan. */
func WriteStrings(path string, accesses <-chan string, closeSignal chan<- bool) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Error("couldn't open output file: ", err)
		closeSignal <- false
		return
	}

	bufWriter := bufio.NewWriterSize(f, 4*1024*1024)

	for io := range accesses {
		if _, err := bufWriter.WriteString(io); err != nil {
			bufWriter.Flush()
			f.Close()
			panic("Couldn't write io trace entry")
		}
	}

	bufWriter.Flush()
	f.Close()

	closeSignal <- true
}

/* Writes all strings in the channel to the given file in gzip format. When the channel is closed, it closes the file and sends a bool to the closeSignal chan. */
func WriteStringsCompressed(path string, accesses <-chan string, closeSignal chan<- bool) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Error("couldn't open output file: ", err)
		closeSignal <- false
		return
	}

	writer := gzip.NewWriter(f)
	bufWriter := bufio.NewWriterSize(writer, 4*1024*1024)

	for io := range accesses {
		if _, err := bufWriter.WriteString(io); err != nil {
			bufWriter.Flush()
			writer.Close()
			f.Close()
			panic("Couldn't write io trace entry")
		}
	}

	bufWriter.Flush()
	writer.Close()
	f.Close()

	closeSignal <- true
}
