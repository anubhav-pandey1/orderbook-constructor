package replay

import "testing"

var sinkDecision Decision

func BenchmarkTimestampStepPolicy(b *testing.B) {
	p := NewTimestampPolicy(TimestampStep, 100)
	p.AcceptSnapshot(Cursor{Timestamp: 1})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cursor := Cursor{Timestamp: int64(i+1)*100 + 1}
		sinkDecision = p.ClassifyUpdate(cursor)
		p.AcceptUpdate(cursor)
	}
}

func BenchmarkArrivalOrderPolicy(b *testing.B) {
	p := NewArrivalOrderPolicy()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cursor := Cursor{Timestamp: int64(i + 1)}
		sinkDecision = p.ClassifyUpdate(cursor)
		p.AcceptUpdate(cursor)
	}
}

func BenchmarkUpdateIDPolicy(b *testing.B) {
	p := NewUpdateIDPolicy()
	p.AcceptSnapshot(Cursor{FinalUpdateID: 1, HasUpdateID: true})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint64(i + 2)
		cursor := Cursor{FirstUpdateID: id, FinalUpdateID: id, HasUpdateID: true}
		sinkDecision = p.ClassifyUpdate(cursor)
		p.AcceptUpdate(cursor)
	}
}
