package dedupAlgorithms

import "testing"

type Thinger interface {
	Size() int
}

type bigthing struct {
	Data []byte
}

func (t *bigthing) Size() int {
	return len(t.Data)
}

func NewThing() *bigthing {
	thing := new(bigthing)
	thing.Data = make([]byte, 100*1024)
	return thing
}

func mapInterfaceInsert(numEntries int, b *testing.B) bool {
	b.StopTimer()
	bthing := NewThing()
	b.StartTimer()

	m := make(map[int]Thinger, numEntries)

	for i := 0; i < numEntries; i++ {
		m[i] = bthing
	}

	return true
}

func mapInterfacePointerInsert(numEntries int, b *testing.B) bool {
	b.StopTimer()
	bthing := NewThing()
	var p Thinger = bthing
	b.StartTimer()

	m := make(map[int]*Thinger, numEntries)

	for i := 0; i < numEntries; i++ {
		m[i] = &p
	}

	return true
}

func mapIntegerInsert(numEntries int, b *testing.B) bool {

	m := make(map[int]int, numEntries)

	for i := 0; i < numEntries; i++ {
		m[i] = i
	}

	return true
}

func benchmarkInterfaceInsert(numEntries int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		foo = mapInterfaceInsert(numEntries, b)
	}
}
func BenchmarkInterfaceInsert10(b *testing.B)  { benchmarkInterfaceInsert(10, b) }
func BenchmarkInterfaceInsert100(b *testing.B) { benchmarkInterfaceInsert(100, b) }
func BenchmarkInterfaceInsert1K(b *testing.B)  { benchmarkInterfaceInsert(1000, b) }
func BenchmarkInterfaceInsert10K(b *testing.B) { benchmarkInterfaceInsert(10000, b) }

func benchmarkInterfacePointerInsert(numEntries int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		foo = mapInterfacePointerInsert(numEntries, b)
	}
}
func BenchmarkInterfacePointerInsert10(b *testing.B)  { benchmarkInterfacePointerInsert(10, b) }
func BenchmarkInterfacePointerInsert100(b *testing.B) { benchmarkInterfacePointerInsert(100, b) }
func BenchmarkInterfacePointerInsert1K(b *testing.B)  { benchmarkInterfacePointerInsert(1000, b) }
func BenchmarkInterfacePointerInsert10K(b *testing.B) { benchmarkInterfacePointerInsert(10000, b) }

func benchmarkIntegerInsert(numEntries int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		foo = mapIntegerInsert(numEntries, b)
	}
}
func BenchmarkIntegerInsert10(b *testing.B)  { benchmarkIntegerInsert(10, b) }
func BenchmarkIntegerInsert100(b *testing.B) { benchmarkIntegerInsert(100, b) }
func BenchmarkIntegerInsert1K(b *testing.B)  { benchmarkIntegerInsert(1000, b) }
func BenchmarkIntegerInsert10K(b *testing.B) { benchmarkIntegerInsert(10000, b) }
