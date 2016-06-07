/*
These benchmarks were part of the decision what to use as main key type in the hash tables (ChunkIndex, ...). As it turns out, the fixed arrays win. (Go 1.1.2, 2013-09-08)
*/
package dedupAlgorithms

import "testing"

func stringHash(numEntries int, b *testing.B) {

	b.StopTimer()
	var hashmap = make(map[string]int64, numEntries)
	var digest = [20]byte{'1', '2', '3', '4', '5', '6', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'h'}
	b.StartTimer()
	for i := 0; i < numEntries; i++ {
		digest[i%20]++
		hashmap[string(digest[:])] = int64(i)
	}
}

func arrayHash(numEntries int, b *testing.B) {

	b.StopTimer()
	var hashmap = make(map[[20]byte]int64, numEntries)
	var digest = [20]byte{'1', '2', '3', '4', '5', '6', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'h'}
	b.StartTimer()
	var hash = [20]byte{}
	for i := 0; i < numEntries; i++ {
		digest[i%20]++
		copy(hash[:], digest[:])
		hashmap[digest] = int64(i)
	}
}

func benchmarkStringHash(numEntries int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		stringHash(numEntries, b)
	}
}

func benchmarkArrayHash(numEntries int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		arrayHash(numEntries, b)
	}
}

func BenchmarkStringHash10(b *testing.B)   { benchmarkStringHash(10, b) }
func BenchmarkStringHash100(b *testing.B)  { benchmarkStringHash(100, b) }
func BenchmarkStringHash1K(b *testing.B)   { benchmarkStringHash(1000, b) }
func BenchmarkStringHash10K(b *testing.B)  { benchmarkStringHash(10000, b) }
func BenchmarkStringHash100K(b *testing.B) { benchmarkStringHash(100000, b) }

func BenchmarkArrayHash10(b *testing.B)   { benchmarkArrayHash(10, b) }
func BenchmarkArrayHash100(b *testing.B)  { benchmarkArrayHash(100, b) }
func BenchmarkArrayHash1K(b *testing.B)   { benchmarkArrayHash(1000, b) }
func BenchmarkArrayHash10K(b *testing.B)  { benchmarkArrayHash(10000, b) }
func BenchmarkArrayHash100K(b *testing.B) { benchmarkArrayHash(100000, b) }
