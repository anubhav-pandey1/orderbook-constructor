package book

import "testing"

func TestGenerationMembershipAndRebuild(t *testing.T) {
	s := newSideBook(Bid, 1)
	for p := Price(1); p <= 100; p++ {
		s.set(p, Quantity(p))
	}
	for p := Price(1); p <= 90; p++ {
		s.del(p)
	}
	for p := Price(80); p <= 90; p++ {
		s.set(p, Quantity(p+1000))
	}
	s.maybeRebuild()
	if s.prices.len() != len(s.levels) || s.staleCount != 0 {
		t.Fatalf("heap/live/stale=%d/%d/%d", s.prices.len(), len(s.levels), s.staleCount)
	}
	for p, current := range s.levels {
		found := false
		for _, e := range s.prices.data {
			if e.price == p && e.generation == current.generation {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing current generation for %d", p)
		}
	}
	for i := 1; i < len(s.prices.data); i++ {
		if s.prices.less(i, (i-1)/2) {
			t.Fatalf("heap violation %d", i)
		}
	}
}

func TestStaleRootGeneration(t *testing.T) {
	s := newSideBook(Bid, 2)
	s.set(100, 1)
	s.set(99, 2)
	old := s.levels[100].generation
	s.del(100)
	s.set(100, 9)
	if s.levels[100].generation == old {
		t.Fatal("generation reused")
	}
	p, q, ok := s.best()
	if !ok || p != 100 || q != 9 {
		t.Fatalf("best=%d/%d/%v", p, q, ok)
	}
}
