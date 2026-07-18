package syncx

import "testing"

func TestPolicies(t *testing.T) {
	p := NewTimestampPolicy(TimestampStep, 100)
	s := Cursor{Timestamp: 1000}
	if p.ClassifySnapshot(s).Action != Apply {
		t.Fatal()
	}
	p.AcceptSnapshot(s)
	n := Cursor{Timestamp: 1100}
	if p.ClassifyUpdate(n).Action != Apply {
		t.Fatal()
	}
	p.AcceptUpdate(n)
	if p.ClassifyUpdate(n).Reason != ReasonDuplicate {
		t.Fatal()
	}
	if p.ClassifyUpdate(Cursor{Timestamp: 1300}).Reason != ReasonGap {
		t.Fatal()
	}
	p.Invalidate()
	if p.ClassifySnapshot(Cursor{Timestamp: 1000}).Action != Discard {
		t.Fatal()
	}
	u := NewUpdateIDPolicy()
	a := Cursor{FinalUpdateID: 10, HasUpdateID: true}
	if u.ClassifySnapshot(a).Action != Apply {
		t.Fatal()
	}
	u.AcceptSnapshot(a)
	if u.ClassifyUpdate(Cursor{FirstUpdateID: 10, FinalUpdateID: 12, HasUpdateID: true}).Action != Apply {
		t.Fatal()
	}
	if u.ClassifyUpdate(Cursor{FirstUpdateID: 13, FinalUpdateID: 14, HasUpdateID: true}).Action != Resync {
		t.Fatal()
	}
}
