package dedupAlgorithms

import "testing"

func arraySetInsert(numEntries int, b *testing.B) bool {
	b.StopTimer()
	var numChunks int = 64
	arraySet := make([][20]byte, 0, numChunks)
	var digest = [20]byte{'1', '2', '3', '4', '5', '6', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'h'}
	b.StartTimer()

	for i := 0; i < numEntries; i++ {
		digest[i%20]++

		if i%numChunks == 0 {
			arraySet = arraySet[:0]
		}

		var exists bool = false
		for index, _ := range arraySet {
			if arraySet[index] == digest {
				exists = true
			}
		}

		if !exists {
			arraySet = append(arraySet, digest)
		}
	}

	return digest[0] == '0'
}

var foo bool

func benchmarkArraySetInsert(numEntries int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		foo = arraySetInsert(numEntries, b)
	}
}
func BenchmarkArraySetInsert10(b *testing.B)   { benchmarkArraySetInsert(10, b) }
func BenchmarkArraySetInsert100(b *testing.B)  { benchmarkArraySetInsert(100, b) }
func BenchmarkArraySetInsert1K(b *testing.B)   { benchmarkArraySetInsert(1000, b) }
func BenchmarkArraySetInsert10K(b *testing.B)  { benchmarkArraySetInsert(10000, b) }
func BenchmarkArraySetInsert100K(b *testing.B) { benchmarkArraySetInsert(100000, b) }

// map set
func mapSetInsert(numEntries int, b *testing.B) bool {
	b.StopTimer()
	var numChunks int = 64
	mapSet := make(map[[20]byte]bool, numChunks)
	var digest = [20]byte{'1', '2', '3', '4', '5', '6', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'h'}
	b.StartTimer()

	for i := 0; i < numEntries; i++ {
		digest[i%20]++

		if i%numChunks == 0 {
			mapSet = make(map[[20]byte]bool, numChunks)
		}

		_, exists := mapSet[digest]
		if !exists {
			mapSet[digest] = true
		}
	}
	return digest[0] == '0'
}

func benchmarkMapSetInsert(numEntries int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		foo = mapSetInsert(numEntries, b)
	}
}
func BenchmarkMapSetInsert10(b *testing.B)   { benchmarkMapSetInsert(10, b) }
func BenchmarkMapSetInsert100(b *testing.B)  { benchmarkMapSetInsert(100, b) }
func BenchmarkMapSetInsert1K(b *testing.B)   { benchmarkMapSetInsert(1000, b) }
func BenchmarkMapSetInsert10K(b *testing.B)  { benchmarkMapSetInsert(10000, b) }
func BenchmarkMapSetInsert100K(b *testing.B) { benchmarkMapSetInsert(100000, b) }
