package replay

import "testing"

func TestTimestampPolicyModesAndMissingCursor(t *testing.T) {
	step := NewTimestampPolicy(TimestampStep, 10)
	if d := step.ClassifySnapshot(Cursor{}); d.Action != Resync || d.Reason != ReasonMissingCursor {
		t.Fatalf("missing snapshot=%+v", d)
	}
	step.AcceptSnapshot(Cursor{Timestamp: 100})
	if d := step.ClassifyUpdate(Cursor{Timestamp: 109}); d.Action != Resync || d.Reason != ReasonGap {
		t.Fatalf("gap=%+v", d)
	}
	if d := NewTimestampPolicy(TimestampStep, 0).ClassifyUpdate(Cursor{Timestamp: 1}); d.Action != Resync || d.Reason != ReasonMissingCursor {
		t.Fatalf("zero step update=%+v", d)
	}
	mono := NewTimestampPolicy(TimestampMonotonic, 0)
	mono.AcceptSnapshot(Cursor{Timestamp: 100})
	if d := mono.ClassifyUpdate(Cursor{Timestamp: 999}); d.Action != Apply {
		t.Fatalf("monotonic jump=%+v", d)
	}
	mono.AcceptUpdate(Cursor{Timestamp: 999})
	if d := mono.ClassifyUpdate(Cursor{Timestamp: 998}); d.Action != Discard || d.Reason != ReasonStale {
		t.Fatalf("monotonic stale=%+v", d)
	}
	if d := mono.ClassifyUpdate(Cursor{Timestamp: 999}); d.Action != Discard || d.Reason != ReasonDuplicate {
		t.Fatalf("monotonic duplicate=%+v", d)
	}
}

func TestUpdateIDPolicyRangesAndInvalidCursors(t *testing.T) {
	p := NewUpdateIDPolicy()
	for _, c := range []Cursor{
		{},
		{FinalUpdateID: 1},
		{HasUpdateID: true},
	} {
		if d := p.ClassifySnapshot(c); d.Action != Resync || d.Reason != ReasonMissingCursor {
			t.Fatalf("snapshot cursor %+v decision %+v", c, d)
		}
	}
	p.AcceptSnapshot(Cursor{FinalUpdateID: 10, HasUpdateID: true})
	for _, c := range []Cursor{
		{FirstUpdateID: 0, FinalUpdateID: 11, HasUpdateID: true},
		{FirstUpdateID: 12, FinalUpdateID: 11, HasUpdateID: true},
		{FirstUpdateID: 11, FinalUpdateID: 11},
	} {
		if d := p.ClassifyUpdate(c); d.Action != Resync || d.Reason != ReasonMissingCursor {
			t.Fatalf("update cursor %+v decision %+v", c, d)
		}
	}
	if d := p.ClassifyUpdate(Cursor{FirstUpdateID: 9, FinalUpdateID: 11, HasUpdateID: true}); d.Action != Apply {
		t.Fatalf("bridged range=%+v", d)
	}
	p.AcceptUpdate(Cursor{FinalUpdateID: 11, HasUpdateID: true})
	if d := p.ClassifyUpdate(Cursor{FirstUpdateID: 12, FinalUpdateID: 12, HasUpdateID: true}); d.Action != Apply {
		t.Fatalf("next update=%+v", d)
	}
	if d := p.ClassifyUpdate(Cursor{FirstUpdateID: 13, FinalUpdateID: 14, HasUpdateID: true}); d.Action != Resync || d.Reason != ReasonGap {
		t.Fatalf("gap=%+v", d)
	}
	if d := p.ClassifyUpdate(Cursor{FirstUpdateID: 11, FinalUpdateID: 11, HasUpdateID: true}); d.Action != Discard || d.Reason != ReasonDuplicate {
		t.Fatalf("duplicate=%+v", d)
	}
	if d := p.ClassifyUpdate(Cursor{FirstUpdateID: 1, FinalUpdateID: 10, HasUpdateID: true}); d.Action != Discard || d.Reason != ReasonStale {
		t.Fatalf("stale=%+v", d)
	}
	p.Invalidate()
	if d := p.ClassifyUpdate(Cursor{FirstUpdateID: 12, FinalUpdateID: 12, HasUpdateID: true}); d.Action != Resync || d.Reason != ReasonMissingCursor {
		t.Fatalf("desynced update=%+v", d)
	}
}

func TestArrivalOrderPolicyAlwaysApplies(t *testing.T) {
	p := NewArrivalOrderPolicy()
	p.AcceptSnapshot(Cursor{})
	p.AcceptUpdate(Cursor{})
	p.Invalidate()
	if p.ClassifySnapshot(Cursor{}).Action != Apply || p.ClassifyUpdate(Cursor{}).Action != Apply {
		t.Fatal("arrival policy should always apply")
	}
}
