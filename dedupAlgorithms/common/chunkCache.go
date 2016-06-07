package common

import "container/list"

type ChunkCacheStatistics struct {
	Hits     int
	Misses   int
	HitRatio float32
}

func (cs *ChunkCacheStatistics) Add(that ChunkCacheStatistics) {
	cs.Hits += that.Hits
	cs.Misses += that.Misses
}
func (cs *ChunkCacheStatistics) Finish() {
	if (cs.Hits + cs.Misses) == 0 {
		cs.HitRatio = 0.0
	} else {
		cs.HitRatio = float32(float64(cs.Hits) / float64((cs.Hits + cs.Misses)))
	}
}

func (cs *ChunkCacheStatistics) Reset() {
	cs.Hits = 0
	cs.Misses = 0
	cs.HitRatio = 0
}

type ChunkCache struct {
	maxSize    int
	cacheMap   map[[12]byte]*list.Element
	cacheList  *list.List
	Statistics ChunkCacheStatistics
}

func NewChunkCache(size int) *ChunkCache {
	cache := &ChunkCache{maxSize: size,
		cacheMap:  make(map[[12]byte]*list.Element, size),
		cacheList: list.New()}
	cache.cacheList.Init()
	return cache
}

func (c *ChunkCache) Contains(fp [12]byte) bool {
	e, exists := c.cacheMap[fp]
	if exists {
		c.cacheList.MoveToFront(e)
		c.Statistics.Hits++
	} else {
		c.Statistics.Misses++
	}
	return exists
}

func (c *ChunkCache) Update(fp [12]byte) {
	e, ok := c.cacheMap[fp]
	if ok { // cache hit
		c.cacheList.MoveToFront(e)
	} else if len(c.cacheMap) < c.maxSize { // cache is not full yet
		e := c.cacheList.PushFront(fp)
		c.cacheMap[fp] = e
	} else { // cache is full
		if e := c.cacheList.Back(); e != nil {
			delete(c.cacheMap, e.Value.([12]byte))
			e.Value = fp
			c.cacheList.MoveToFront(e)
			c.cacheMap[fp] = e
		}
	}
}

type UInt32Pair struct {
	key, value uint32
}

// Unsigned Integer cache
type UInt32Cache struct {
	maxSize   int
	CacheMap  map[uint32]*list.Element
	cacheList *list.List
	callback  func(uint32)
}

func NewUInt32Cache(size int, callback func(uint32)) *UInt32Cache {
	cache := &UInt32Cache{
		maxSize:   size,
		CacheMap:  make(map[uint32]*list.Element, size),
		cacheList: list.New(),
		callback:  callback}
	return cache
}

func (c *UInt32Cache) Get(key uint32) uint32 {
	return c.CacheMap[key].Value.(*UInt32Pair).value
}

func (c *UInt32Cache) Contains(key uint32) (uint32, bool) {
	e, exists := c.CacheMap[key]
	if exists {
		c.cacheList.MoveToFront(e)
		return e.Value.(*UInt32Pair).value, exists
	} else {
		return 0, exists
	}
}

func (c *UInt32Cache) Update(key uint32, newVal uint32) {
	e, ok := c.CacheMap[key]
	if ok { // cache hit
		c.cacheList.MoveToFront(e)
		e.Value.(*UInt32Pair).value = newVal
	} else if len(c.CacheMap) < c.maxSize { // cache is not full yet
		e := c.cacheList.PushFront(&UInt32Pair{key: key, value: newVal})
		c.CacheMap[key] = e
	} else { // cache is full
		if e := c.cacheList.Back(); e != nil {
			delete(c.CacheMap, e.Value.(*UInt32Pair).key)
			if c.callback != nil {
				c.callback(e.Value.(*UInt32Pair).key)
			}
			e.Value.(*UInt32Pair).key = key
			e.Value.(*UInt32Pair).value = newVal
			c.cacheList.MoveToFront(e)
			c.CacheMap[key] = e
		}
	}
}
