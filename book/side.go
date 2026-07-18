package book

// level is a stored aggregate quantity plus the generation stamped when the
// price was (re)inserted.
type level struct {
	quantity   Quantity
	generation uint64
}

// sideBook is one side of the book: an authoritative price->level map plus a
// price heap for amortized O(1) best-price access with lazy deletion.
type sideBook struct {
	side       Side
	levels     map[Price]level
	prices     priceHeap
	nextGen    uint64
	staleCount int // retained entries that do not match an active generation
}

func newSideBook(side Side, capHint int) sideBook {
	if capHint < 16 {
		capHint = 16
	}
	return sideBook{
		side:   side,
		levels: make(map[Price]level, capHint),
		prices: newPriceHeap(side == Bid, capHint),
	}
}

// set inserts or replaces a level. A new price pushes exactly one heap entry;
// a quantity update of an existing price touches only the map.
func (s *sideBook) set(p Price, q Quantity) {
	if lv, ok := s.levels[p]; ok {
		lv.quantity = q
		s.levels[p] = lv
		return
	}
	g := s.nextGen
	s.nextGen++
	s.levels[p] = level{quantity: q, generation: g}
	s.prices.push(heapEntry{price: p, generation: g})
}

// del removes a level. The heap entry is left for lazy cleanup. Deleting an
// absent price is a no-op. Returns whether a level was actually removed.
func (s *sideBook) del(p Price) bool {
	if _, ok := s.levels[p]; ok {
		delete(s.levels, p)
		s.staleCount++
		return true
	}
	return false
}

// best returns the current best price/qty, popping stale roots (deleted or
// superseded generation) along the way.
func (s *sideBook) best() (Price, Quantity, bool) {
	for {
		e, ok := s.prices.peek()
		if !ok {
			return 0, 0, false
		}
		if lv, exists := s.levels[e.price]; exists && lv.generation == e.generation {
			return e.price, lv.quantity, true
		}
		s.prices.pop()
		if s.staleCount > 0 {
			s.staleCount--
		}
	}
}

// maybeRebuild reclaims stale heap storage once the heap grows well beyond the
// live level count. Threshold is a tunable implementation constant.
func (s *sideBook) maybeRebuild() {
	if s.prices.len() > 2*len(s.levels)+64 {
		s.rebuild()
	}
}

func (s *sideBook) rebuild() {
	d := s.prices.data[:0]
	for p, lv := range s.levels {
		d = append(d, heapEntry{price: p, generation: lv.generation})
	}
	s.prices.data = d
	s.prices.heapify()
	s.staleCount = 0
}
