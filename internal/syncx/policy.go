package syncx

type State uint8

const (
	Uninitialized State = iota
	Synchronized
	Desynchronized
)

type Action uint8

const (
	Apply Action = iota + 1
	Discard
	Resync
)

type Reason uint8

const (
	ReasonNone Reason = iota
	ReasonStale
	ReasonDuplicate
	ReasonGap
	ReasonMissingCursor
	ReasonCrossed
	ReasonInvalidSnapshot
)

type Cursor struct {
	Timestamp                    int64
	FirstUpdateID, FinalUpdateID uint64
	HasUpdateID                  bool
}
type Decision struct {
	Action Action
	Reason Reason
}
type Policy interface {
	ClassifySnapshot(Cursor) Decision
	ClassifyUpdate(Cursor) Decision
	AcceptSnapshot(Cursor)
	AcceptUpdate(Cursor)
	Invalidate()
}
type TimestampMode uint8

const (
	TimestampStep TimestampMode = iota + 1
	TimestampMonotonic
)

type timestampPolicy struct {
	mode            TimestampMode
	step, last      int64
	hasLast, synced bool
}

func NewTimestampPolicy(mode TimestampMode, step int64) Policy {
	return &timestampPolicy{mode: mode, step: step}
}
func (p *timestampPolicy) ClassifySnapshot(c Cursor) Decision {
	if c.Timestamp <= 0 {
		return Decision{Resync, ReasonMissingCursor}
	}
	if p.hasLast && c.Timestamp <= p.last {
		return staleTS(c.Timestamp, p.last)
	}
	return Decision{Action: Apply}
}
func (p *timestampPolicy) ClassifyUpdate(c Cursor) Decision {
	if c.Timestamp <= 0 || !p.synced {
		return Decision{Resync, ReasonMissingCursor}
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
		return Decision{Resync, ReasonMissingCursor}
	}
	return Decision{Resync, ReasonGap}
}
func staleTS(v, last int64) Decision {
	if v == last {
		return Decision{Discard, ReasonDuplicate}
	}
	return Decision{Discard, ReasonStale}
}
func (p *timestampPolicy) accept(c Cursor)         { p.last = c.Timestamp; p.hasLast = true; p.synced = true }
func (p *timestampPolicy) AcceptSnapshot(c Cursor) { p.accept(c) }
func (p *timestampPolicy) AcceptUpdate(c Cursor)   { p.accept(c) }
func (p *timestampPolicy) Invalidate()             { p.synced = false }

type updateIDPolicy struct {
	last            uint64
	hasLast, synced bool
}

func NewUpdateIDPolicy() Policy { return &updateIDPolicy{} }
func (p *updateIDPolicy) ClassifySnapshot(c Cursor) Decision {
	if !c.HasUpdateID || c.FinalUpdateID == 0 {
		return Decision{Resync, ReasonMissingCursor}
	}
	if p.hasLast && c.FinalUpdateID <= p.last {
		return staleID(c.FinalUpdateID, p.last)
	}
	return Decision{Action: Apply}
}
func (p *updateIDPolicy) ClassifyUpdate(c Cursor) Decision {
	if !c.HasUpdateID || c.FirstUpdateID == 0 || c.FinalUpdateID < c.FirstUpdateID || !p.synced {
		return Decision{Resync, ReasonMissingCursor}
	}
	if c.FinalUpdateID <= p.last {
		return staleID(c.FinalUpdateID, p.last)
	}
	next := p.last + 1
	if c.FirstUpdateID <= next && c.FinalUpdateID >= next {
		return Decision{Action: Apply}
	}
	return Decision{Resync, ReasonGap}
}
func staleID(v, last uint64) Decision {
	if v == last {
		return Decision{Discard, ReasonDuplicate}
	}
	return Decision{Discard, ReasonStale}
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

func NewArrivalOrderPolicy() Policy                         { return arrivalOrderPolicy{} }
func (arrivalOrderPolicy) ClassifySnapshot(Cursor) Decision { return Decision{Action: Apply} }
func (arrivalOrderPolicy) ClassifyUpdate(Cursor) Decision   { return Decision{Action: Apply} }
func (arrivalOrderPolicy) AcceptSnapshot(Cursor)            {}
func (arrivalOrderPolicy) AcceptUpdate(Cursor)              {}
func (arrivalOrderPolicy) Invalidate()                      {}
