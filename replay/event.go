package replay

import "github.com/anubhav-pandey1/orderbook-constructor/book"

type EventKind uint8

const (
	SnapshotApplied EventKind = iota + 1

	IncrementalApplied

	BookInvalidated
)

type Event struct {
	NotificationID uint64
	Version        uint64
	SyncEpoch      uint64
	Kind           EventKind
	State          State
	Reason         Reason

	BidPx  book.Price
	AskPx  book.Price
	BidQty book.Quantity
	AskQty book.Quantity
	BidOK  bool
	AskOK  bool

	EventTS   int64
	DueNS     int64
	IngressNS int64
	ApplyNS   int64
}

func (e Event) Actionable() bool {
	return e.Kind != BookInvalidated && e.State == Synchronized
}
