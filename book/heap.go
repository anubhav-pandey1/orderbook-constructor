package book

type heapEntry struct {
	price      Price
	generation uint64
}

type priceHeap struct {
	data []heapEntry
	max  bool
}

func newPriceHeap(max bool, capHint int) priceHeap {
	if capHint < 16 {
		capHint = 16
	}
	return priceHeap{data: make([]heapEntry, 0, capHint), max: max}
}

func (h *priceHeap) len() int { return len(h.data) }

func (h *priceHeap) less(i, j int) bool {
	if h.max {
		return h.data[i].price > h.data[j].price
	}
	return h.data[i].price < h.data[j].price
}

func (h *priceHeap) swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }

func (h *priceHeap) push(e heapEntry) {
	h.data = append(h.data, e)
	h.up(len(h.data) - 1)
}

func (h *priceHeap) peek() (heapEntry, bool) {
	if len(h.data) == 0 {
		return heapEntry{}, false
	}
	return h.data[0], true
}

func (h *priceHeap) pop() {
	n := len(h.data) - 1
	h.data[0] = h.data[n]
	h.data = h.data[:n]
	if n > 0 {
		h.down(0)
	}
}

func (h *priceHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			break
		}
		h.swap(i, parent)
		i = parent
	}
}

func (h *priceHeap) down(i int) {
	n := len(h.data)
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		c := l
		if r := l + 1; r < n && h.less(r, l) {
			c = r
		}
		if !h.less(c, i) {
			break
		}
		h.swap(i, c)
		i = c
	}
}

func (h *priceHeap) heapify() {
	for i := len(h.data)/2 - 1; i >= 0; i-- {
		h.down(i)
	}
}
