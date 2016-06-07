package containerCaching

import "fmt"
import "sync"
import "sync/atomic"
import "encoding/gob"
import "strconv"
import "bytes"

import log "github.com/cihub/seelog"
import "github.com/bradfitz/gomemcache/memcache"

type ContainerStorageStatistics struct {
	ContainerCount   int
	StorageCount     int64
	StorageByteCount int64
}

func (cs *ContainerStorageStatistics) add(that ContainerStorageStatistics) {
	cs.ContainerCount += that.ContainerCount
	cs.StorageCount += that.StorageCount
	cs.StorageByteCount += that.StorageByteCount
}
func (cs *ContainerStorageStatistics) finish() {}
func (cs *ContainerStorageStatistics) reset() {
	cs.ContainerCount = 0
	cs.StorageCount = 0
	cs.StorageByteCount = 0
}

func sizeOfContainerMetaData(containerSize int, avgChunkSize int) int64 {
	const metaDataSizePerChunk = 24
	chunksPerContainer := containerSize / avgChunkSize

	d := (chunksPerContainer * metaDataSizePerChunk) / 4096
	r := (chunksPerContainer * metaDataSizePerChunk) % 4096

	// size must be a multiple of 4KB
	if r == 0 {
		return int64(chunksPerContainer * metaDataSizePerChunk)
	} else {
		return int64((d + 1) * 4096)
	}
}

type ContainerStorage struct {
	containerSize int
	avgChunkSize  int
	metaDataSize  int64
	/*containerStorage       map[uint32][][12]byte*/

	localContainerStorage map[uint32][][12]byte
	containerStorage      *memcache.Client

	contStRWMutex          sync.RWMutex
	currentContainerId     uint32
	currentContainerChunks [][12]byte
	maxContainerId         uint32

	freeSpace      uint32
	sliceAllocSize int
	traceChan      chan string
}

func NewContainerStorage(containerSize int, avgChunkSize int, traceChan chan string) *ContainerStorage {
	return &ContainerStorage{
		containerSize:         containerSize,
		avgChunkSize:          avgChunkSize,
		metaDataSize:          sizeOfContainerMetaData(containerSize, avgChunkSize),
		localContainerStorage: make(map[uint32][][12]byte, 1E5),
		traceChan:             traceChan,
	}
}

// returns a unique container id
func (cs *ContainerStorage) GetContainerId() uint32 {
	return atomic.AddUint32(&cs.currentContainerId, 1)
}

func (cs *ContainerStorage) AddNewContainer(container [][12]byte, containerId uint32) uint32 {
	newId := cs.GetContainerId()

	cs.contStRWMutex.Lock()
	cs.localContainerStorage[containerId] = container
	if containerId > cs.maxContainerId {
		cs.maxContainerId = containerId
	}
	cs.contStRWMutex.Unlock()

	return newId
}

func (cs *ContainerStorage) ReadContainer(containerId uint32, countIO bool, streamId string, stats interface{}) [][12]byte {

	var list [][12]byte
	var ok bool
	cs.contStRWMutex.RLock()
	defer cs.contStRWMutex.RUnlock()
	if list, ok = cs.localContainerStorage[containerId]; !ok && (cs.containerStorage != nil) {
		if list, ok = cs.getContainer(containerId); !ok {
			log.Warnf("Memcache doesn't contain container %s, Current container id: %s", containerId, cs.currentContainerId)
			return nil
		}
	}
	if !ok {
		return nil
	} else if countIO {
		switch t := stats.(type) {
		case *ContainerCachingStatistics:
			t.ContainerStorageStats.StorageCount++
		case *SamplingContainerCachingStatistics:
			t.ContainerStorageStats.StorageCount++
		default:
			panic("unknown type")
		}

		if cs.traceChan != nil {
			cs.traceChan <- fmt.Sprintf("%v\t%v\n", streamId, containerId)
		}

		return list
	} else {
		return list
	}
}

// Flushes containers from localContainerIndex to memcache. The flushing starts at
// the given id and ends if it hits the first not existing id.
func (cs *ContainerStorage) FlushIDsFrom(from uint32) bool {
	log.Infof("Flushing containers starting from %v", from)
	cs.contStRWMutex.Lock()
	defer cs.contStRWMutex.Unlock()

	var numFlushed int
	numEntriesBefore := len(cs.localContainerStorage)

	for i := from; i <= cs.maxContainerId; i++ {
		if bm, ok := cs.localContainerStorage[i]; ok {
			if !cs.addContainer(i, bm) {
				log.Error("Couldn't flush container with id ", i)
				return false
			} else {
				numFlushed++
				delete(cs.localContainerStorage, i)
			}
		}
	}

	log.Infof("Flushed %v containers. numEntries before flushing: %v, after: %v", numFlushed, numEntriesBefore, len(cs.localContainerStorage))
	return true
}

// gets the container from memcache
func (cs *ContainerStorage) getContainer(idd uint32) ([][12]byte, bool) {
	var container [][12]byte
	var memcMapping *memcache.Item
	var memerr error
	id := strconv.FormatInt(int64(idd), 10)
	done := false
	toCounter := 0

	for !done {
		if memcMapping, memerr = cs.containerStorage.Get(id); memerr != nil {
			if memerr == memcache.ErrCacheMiss {
				log.Trace("get memcache cache miss: ", memerr)
				return nil, false
			} else {
				toCounter++
				if toCounter > 9 {
					log.Errorf("read from memcacheserver GetIndex: %s. tried %d times with id %d", memerr, toCounter, id)
					return nil, false
				}
			}
		} else {
			done = true
		}
	}
	dec := gob.NewDecoder(bytes.NewBuffer(memcMapping.Value))
	if decerr := dec.Decode(&container); decerr != nil {
		log.Error("decode error in GetIndex: ", decerr)
		return nil, false
	}

	return container, true
}

//Adds container to memcache
func (cs *ContainerStorage) addContainer(idd uint32, container [][12]byte) bool {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if encErr := enc.Encode(container); encErr != nil {
		log.Error("encode error addIndex:", encErr)
	}

	//try to add for max 10 times
	id := strconv.FormatInt(int64(idd), 10)
	done := false
	var memerr error
	for i := 0; i < 10; i++ {
		if memerr = cs.containerStorage.Set(&memcache.Item{Key: id, Value: buf.Bytes()}); memerr != nil {
			log.Warnf("write to memcacheserver addIndex: %s. Counter is at %d ", memerr, i)
		} else {
			done = true
			break
		}
	}

	if !done {
		log.Errorf("Write to memcacheserver failed for container %s : %s", idd, memerr)
	}

	return done
}
