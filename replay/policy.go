package replay

// State describes replay synchronization state.
type State uint8

const (
	// Uninitialized means no accepted snapshot has established book state.
	Uninitialized State = iota

	// Synchronized means updates are being applied to an authoritative book.
	Synchronized

	// Desynchronized means an authoritative snapshot is required.
	Desynchronized
)

// Action describes what a Policy wants replay to do with a record.
type Action uint8

const (
	// Apply means the record should be applied to the book.
	Apply Action = iota + 1

	// Discard means the record should be ignored without invalidating the book.
	Discard

	// Resync means the book should be invalidated and a new snapshot requested.
	Resync
)

// Reason explains a discard or resynchronization decision.
type Reason uint8

const (
	// ReasonNone means no exceptional condition occurred.
	ReasonNone Reason = iota

	// ReasonStale means the record is older than the last accepted cursor.
	ReasonStale

	// ReasonDuplicate means the record duplicates the last accepted cursor.
	ReasonDuplicate

	// ReasonGap means the policy detected a missing cursor interval.
	ReasonGap

	// ReasonMissingCursor means required cursor data was absent or invalid.
	ReasonMissingCursor

	// ReasonCrossed means applying the record would lock or cross the book.
	ReasonCrossed

	// ReasonInvalidSnapshot means a snapshot failed book validation.
	ReasonInvalidSnapshot
)

// Cursor contains synchronization metadata from a feed record.
type Cursor struct {
	// Timestamp is the source record timestamp.
	Timestamp int64
	// FirstUpdateID is the first update ID covered by the record.
	FirstUpdateID uint64
	// FinalUpdateID is the final update ID covered by the record.
	FinalUpdateID uint64
	// HasUpdateID reports whether update ID fields are meaningful.
	HasUpdateID bool
}

// Decision is the result of classifying a record cursor.
type Decision struct {
	// Action is the replay action selected by the policy.
	Action Action
	// Reason explains Discard and Resync decisions.
	Reason Reason
}

// Policy classifies snapshots and updates and tracks accepted cursors.
type Policy interface {
	// ClassifySnapshot decides how to handle a snapshot cursor.
	ClassifySnapshot(Cursor) Decision
	// ClassifyUpdate decides how to handle an incremental update cursor.
	ClassifyUpdate(Cursor) Decision
	// AcceptSnapshot records an applied snapshot cursor.
	AcceptSnapshot(Cursor)
	// AcceptUpdate records an applied update cursor.
	AcceptUpdate(Cursor)
	// Invalidate clears synchronized state while preserving policy-specific history.
	Invalidate()
}

// TimestampMode configures timestamp-based gap detection.
type TimestampMode uint8

const (
	// TimestampStep requires each update timestamp to advance by the configured step.
	TimestampStep TimestampMode = iota + 1

	// TimestampMonotonic accepts any strictly increasing timestamp.
	TimestampMonotonic
)

type timestampPolicy struct {
	mode            TimestampMode
	step            int64
	last            int64
	hasLast, synced bool
}

// NewTimestampPolicy returns a policy that synchronizes by record timestamps.
func NewTimestampPolicy(mode TimestampMode, step int64) Policy {
	return &timestampPolicy{mode: mode, step: step}
}

func (p *timestampPolicy) ClassifySnapshot(c Cursor) Decision {
	if c.Timestamp <= 0 {
		return Decision{Action: Resync, Reason: ReasonMissingCursor}
	}
	if p.hasLast && c.Timestamp <= p.last {
		return staleTS(c.Timestamp, p.last)
	}
	return Decision{Action: Apply}
}

func (p *timestampPolicy) ClassifyUpdate(c Cursor) Decision {
	if c.Timestamp <= 0 || !p.synced {
		return Decision{Action: Resync, Reason: ReasonMissingCursor}
	}
	if c.Timestamp <= p.last {
		return staleTS(c.Timestamp, p.last)
	}
	if p.mode == TimestampMonotonic {
		return Decision{Action: Apply}
	}
	if p.mode == TimestampStep && p.step > 0 && c.Timestamp == p.last+p.step {
		return Decision{Action: Apply}
	}
	if p.mode != TimestampStep || p.step <= 0 {
		return Decision{Action: Resync, Reason: ReasonMissingCursor}
	}
	return Decision{Action: Resync, Reason: ReasonGap}
}

func staleTS(v, last int64) Decision {
	if v == last {
		return Decision{Action: Discard, Reason: ReasonDuplicate}
	}
	return Decision{Action: Discard, Reason: ReasonStale}
}

func (p *timestampPolicy) accept(c Cursor) {
	p.last = c.Timestamp
	p.hasLast = true
	p.synced = true
}

func (p *timestampPolicy) AcceptSnapshot(c Cursor) { p.accept(c) }
func (p *timestampPolicy) AcceptUpdate(c Cursor)   { p.accept(c) }
func (p *timestampPolicy) Invalidate()             { p.synced = false }

type updateIDPolicy struct {
	last            uint64
	hasLast, synced bool
}

// NewUpdateIDPolicy returns a policy that synchronizes by exchange update IDs.
func NewUpdateIDPolicy() Policy { return &updateIDPolicy{} }

func (p *updateIDPolicy) ClassifySnapshot(c Cursor) Decision {
	if !c.HasUpdateID || c.FinalUpdateID == 0 {
		return Decision{Action: Resync, Reason: ReasonMissingCursor}
	}
	if p.hasLast && c.FinalUpdateID <= p.last {
		return staleID(c.FinalUpdateID, p.last)
	}
	return Decision{Action: Apply}
}

func (p *updateIDPolicy) ClassifyUpdate(c Cursor) Decision {
	if !c.HasUpdateID || c.FirstUpdateID == 0 || c.FinalUpdateID < c.FirstUpdateID || !p.synced {
		return Decision{Action: Resync, Reason: ReasonMissingCursor}
	}
	if c.FinalUpdateID <= p.last {
		return staleID(c.FinalUpdateID, p.last)
	}
	next := p.last + 1
	if c.FirstUpdateID <= next && c.FinalUpdateID >= next {
		return Decision{Action: Apply}
	}
	return Decision{Action: Resync, Reason: ReasonGap}
}

func staleID(v, last uint64) Decision {
	if v == last {
		return Decision{Action: Discard, Reason: ReasonDuplicate}
	}
	return Decision{Action: Discard, Reason: ReasonStale}
}

func (p *updateIDPolicy) accept(c Cursor) {
	p.last = c.FinalUpdateID
	p.hasLast = true
	p.synced = true
}

func (p *updateIDPolicy) AcceptSnapshot(c Cursor) { p.accept(c) }
func (p *updateIDPolicy) AcceptUpdate(c Cursor)   { p.accept(c) }
func (p *updateIDPolicy) Invalidate()             { p.synced = false }

type arrivalOrderPolicy struct{}

// NewArrivalOrderPolicy returns a policy that applies records in arrival order.
func NewArrivalOrderPolicy() Policy { return arrivalOrderPolicy{} }

func (arrivalOrderPolicy) ClassifySnapshot(Cursor) Decision { return Decision{Action: Apply} }
func (arrivalOrderPolicy) ClassifyUpdate(Cursor) Decision   { return Decision{Action: Apply} }
func (arrivalOrderPolicy) AcceptSnapshot(Cursor)            {}
func (arrivalOrderPolicy) AcceptUpdate(Cursor)              {}
func (arrivalOrderPolicy) Invalidate()                      {}
