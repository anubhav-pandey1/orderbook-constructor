package pipeline

import (
	"orderbook/book"
	"orderbook/internal/syncx"
)

type EventKind uint8

const (
	SnapshotApplied EventKind = iota + 1
	IncrementalApplied
	BookInvalidated
)

type Event struct {
	NotificationID, Version, SyncEpoch uint64
	Kind                               EventKind
	State                              syncx.State
	Reason                             syncx.Reason
	BidPx, AskPx                       book.Price
	BidQty, AskQty                     book.Quantity
	BidOK, AskOK                       bool
	EventTS, DueNS, IngressNS, ApplyNS int64
}

func (e Event) Actionable() bool { return e.Kind != BookInvalidated && e.State == syncx.Synchronized }
