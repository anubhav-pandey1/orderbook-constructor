package replay

import "testing"

func TestPolicies(t *testing.T) {
	p := NewTimestampPolicy(TimestampStep, 100)
	s := Cursor{Timestamp: 1000}
	if got := p.ClassifySnapshot(s); got != (Decision{Action: Apply}) {
		t.Fatalf("timestamp snapshot decision=%+v", got)
	}
	p.AcceptSnapshot(s)
	n := Cursor{Timestamp: 1100}
	if got := p.ClassifyUpdate(n); got != (Decision{Action: Apply}) {
		t.Fatalf("timestamp next update decision=%+v", got)
	}
	p.AcceptUpdate(n)
	if got := p.ClassifyUpdate(n); got != (Decision{Action: Discard, Reason: ReasonDuplicate}) {
		t.Fatalf("timestamp duplicate decision=%+v", got)
	}
	if got := p.ClassifyUpdate(Cursor{Timestamp: 1300}); got != (Decision{Action: Resync, Reason: ReasonGap}) {
		t.Fatalf("timestamp gap decision=%+v", got)
	}
	p.Invalidate()
	if got := p.ClassifySnapshot(Cursor{Timestamp: 1000}); got != (Decision{Action: Discard, Reason: ReasonStale}) {
		t.Fatalf("timestamp stale recovery snapshot decision=%+v", got)
	}
	u := NewUpdateIDPolicy()
	a := Cursor{FinalUpdateID: 10, HasUpdateID: true}
	if got := u.ClassifySnapshot(a); got != (Decision{Action: Apply}) {
		t.Fatalf("update-id snapshot decision=%+v", got)
	}
	u.AcceptSnapshot(a)
	if got := u.ClassifyUpdate(Cursor{FirstUpdateID: 10, FinalUpdateID: 12, HasUpdateID: true}); got != (Decision{Action: Apply}) {
		t.Fatalf("update-id bridged range decision=%+v", got)
	}
	if got := u.ClassifyUpdate(Cursor{FirstUpdateID: 13, FinalUpdateID: 14, HasUpdateID: true}); got != (Decision{Action: Resync, Reason: ReasonGap}) {
		t.Fatalf("update-id gap decision=%+v", got)
	}
}
