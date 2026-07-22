package replay

import "github.com/anubhav-pandey1/orderbook-constructor/book"

// EventKind describes why a replay event was emitted.
type EventKind uint8

const (
	// SnapshotApplied reports an accepted snapshot.
	SnapshotApplied EventKind = iota + 1

	// IncrementalApplied reports an accepted incremental update.
	IncrementalApplied

	// BookInvalidated reports a transition out of synchronization.
	BookInvalidated
)

// Event is emitted after accepted book mutations and invalidations.
type Event struct {
	// NotificationID increments for each emitted event.
	NotificationID uint64
	// Version is the book version associated with the event.
	Version uint64
	// SyncEpoch increments after each accepted snapshot.
	SyncEpoch uint64
	// Kind classifies the event.
	Kind EventKind
	// State is the replay synchronization state after the event.
	State State
	// Reason explains invalidation events.
	Reason Reason

	// BidPx is the best bid when BidOK is true.
	BidPx book.Price
	// AskPx is the best ask when AskOK is true.
	AskPx book.Price
	// BidQty is the best bid quantity when BidOK is true.
	BidQty book.Quantity
	// AskQty is the best ask quantity when AskOK is true.
	AskQty book.Quantity
	// BidOK reports whether a best bid exists.
	BidOK bool
	// AskOK reports whether a best ask exists.
	AskOK bool

	// EventTS is the source feed timestamp.
	EventTS int64
	// DueNS is the replay due time in paced mode.
	DueNS int64
	// IngressNS is the replay clock time before book application.
	IngressNS int64
	// ApplyNS is the replay clock time after book application or invalidation.
	ApplyNS int64
}

// Actionable reports whether the event is an applied synchronized book update.
func (e Event) Actionable() bool {
	return e.Kind != BookInvalidated && e.State == Synchronized
}
