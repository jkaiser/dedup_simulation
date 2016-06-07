package common

import "testing"
import "encoding/binary"

func TestChunkCacheInsert(t *testing.T) {

	var maxSize int = 10
	var digest = [12]byte{}
	cache := NewChunkCache(maxSize)

	for i := 0; i < maxSize; i++ {
		digest[0] = byte(i) // works as long as maxSize is < 256
		cache.Update(digest)
		if !cache.Contains(digest) {
			t.Fatalf("int %v not in cache", i)
		}
	}

	for i := 0; i < maxSize; i++ {
		digest[0] = byte(i) // works as long as maxSize is < 256
		if !cache.Contains(digest) {
			t.Fatalf("int %v not in cache", i)
		}
	}
}

func TestChunkCacheDuplicateInsert(t *testing.T) {

	var maxSize int = 10
	var digest = [12]byte{}
	cache := NewChunkCache(maxSize)

	for i := 0; i < maxSize; i++ {
		digest[0] = byte(1) // works as long as maxSize is < 256
		cache.Update(digest)
		if !cache.Contains(digest) {
			t.Fatalf("int %v not in cache", i)
		}
	}

	digest[0] = byte(1) // works as long as maxSize is < 256
	if !cache.Contains(digest) {
		t.Fatalf("int %v not in cache", 1)
	}

	if len(cache.cacheMap) != 1 {
		t.Fatalf("internal map contains too many values. expected: %v, got: %v", 1, len(cache.cacheMap))
	}
}

func TestChunkCacheEvict(t *testing.T) {

	var maxSize int = 10
	var digest = [12]byte{}
	cache := NewChunkCache(maxSize)

	for i := 0; i < maxSize*2; i++ {
		binary.LittleEndian.PutUint32(digest[:], uint32(i))
		cache.Update(digest)
	}

	for i := 0; i < maxSize; i++ {
		binary.LittleEndian.PutUint32(digest[:], uint32(i))
		if cache.Contains(digest) {
			t.Fatalf("cache still has evicted element: %v", i)
		}
	}

	for i := maxSize; i < 2*maxSize; i++ {
		binary.LittleEndian.PutUint32(digest[:], uint32(i))
		if !cache.Contains(digest) {
			t.Fatalf("cache hasn't element: %v", i)
		}
	}

	if len(cache.cacheMap) != maxSize {
		t.Fatalf("internal map contains an unexpexted number of values. expected: %v, got: %v", maxSize, len(cache.cacheMap))
	}

	// test eviction of single value
	digest[0] = byte(maxSize)
	if !cache.Contains(digest) {
		t.Fatalf("cache hasn't element: %v", maxSize)
	}

	digest[0] = byte(100)
	cache.Update(digest)
	if !cache.Contains(digest) {
		t.Fatalf("cache hasn't evicted element: %v", maxSize)
	}
}
