package rafts

import pb "continuedb/rafts/pb"

// unstable stores log entries which aren't persisted.
// Note: The whole storage will be invalid if the snapshot isn't nil.
type unstable struct {
	snapshot *pb.Snapshot
	ents []pb.Entry
	offset uint64
	logger Logger
}

func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.offset, true
	}
	return 0, false
}

func (u *unstable) maybeLastIndex() (uint64, bool) {
	if len(u.ents) > 0 {
		return u.offset + uint64(len(u.ents)) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Index, true
	}
	return 0, false
}

func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && i == u.snapshot.Index {
			return u.snapshot.Term, true
		}
		return 0, false
	}
	padding := i-u.offset
	if last, ok := u.maybeLastIndex(); !ok || last < padding {
		return 0, false
	}
	return u.ents[padding].Term, true
}

// entries return entries in [lo, hi).
func (u *unstable) entries(lo, hi, maxSize uint64) []pb.Entry {
	if lo >= hi {
		return nil
	}
	if lo-u.offset < 0 {
		lo = u.offset
	}
	if hi-u.offset > uint64(len(u.ents)) {
		hi = u.offset + uint64(len(u.ents))
	}
	return u.ents[lo-u.offset:hi-u.offset]
}

func (u *unstable) append(ents []pb.Entry) {
	first := ents[0].Index - 1
	switch {
	case first == u.offset + uint64(len(u.ents)) - 1:
		u.ents = append(u.ents, ents...)
	case first > u.offset + uint64(len(u.ents)) - 1:
		u.logger.Panicf("Can't append entries: missing index(%d, %d).", u.offset + uint64(len(u.ents)) - 1, first)
	case first < u.offset:
		u.ents = ents
		u.offset = first + 1
	default:
		u.logger.Infof("Truncate entries to index %d.", first)
		u.ents = append([]pb.Entry{}, u.ents[:first-u.offset+1]...)
		u.ents = append(u.ents, ents...)
	}
}

func (u *unstable) restore(snapshot *pb.Snapshot) {
	u.snapshot = snapshot
	u.offset = snapshot.Index + 1
	u.ents = nil
}

func (u *unstable) stableTo(i uint64) (*pb.Snapshot, []pb.Entry) {
	var (
		snap *pb.Snapshot
		ents []pb.Entry
	)
	if u.snapshot != nil && i >= u.snapshot.Index {
		snap = u.snapshot
		u.snapshot = nil
	}
	if i >= u.offset {
		ents = u.ents[:i-u.offset+1]
		u.ents = u.ents[i-u.offset+1:]
		u.offset = i + 1
	}
	return snap, ents
}

// raftLog is used to store the log entries for raft instance.
type raftLog struct {
	// stable storage.
	storage Storage
	// unstable storage
	unstable unstable
	// committed index.
	committed uint64
	// applied index.
	applied uint64

	logger Logger
}

func newRaftLog(storage Storage, logger Logger) *raftLog {
	rlog := &raftLog{
		storage: storage,
		logger: logger,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err) // TODO
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	rlog.unstable.offset = lastIndex + 1
	rlog.unstable.logger = logger
	rlog.committed = firstIndex - 1
	rlog.applied = firstIndex - 1
	return rlog
}

func (rl *raftLog) firstIndex() uint64 {
	if idx, ok := rl.unstable.maybeFirstIndex(); ok {
		return idx
	}
	idx, err := rl.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO
	}
	return idx
}

func (rl *raftLog) lastIndex() uint64 {
	if idx, ok := rl.unstable.maybeLastIndex(); ok {
		return idx
	}
	idx, err := rl.storage.LastIndex()
	if err != nil {
		panic(err) // TODO
	}
	return idx
}

func (rl *raftLog) term(idx uint64) (uint64, error) {
	if idx < rl.firstIndex() - 1 || idx > rl.lastIndex() {
		return 0, nil
	}
	if term, ok := rl.unstable.maybeTerm(idx); ok {
		return term, nil
	}
	term, err := rl.storage.Term(idx)
	if err != nil {
		return 0, err
	}
	return term, nil
}

func (rl *raftLog) matchTerm(idx, term uint64) bool {
	t, err := rl.term(idx)
	if err != nil {
		return false
	}
	return t == term
}

// maybeAppend accepts the last (idx, term) and will appends ents to raft log
// iff the given (idx, term) matches with someone already in the log.
// Return last index and the boolean value for success.
func (rl *raftLog) maybeAppend(idx, term, committed uint64, ents []pb.Entry) (uint64, bool) {
	lastidx := idx + uint64(len(ents))
	if rl.matchTerm(idx, term) {
		for i, ent := range ents {
			if !rl.matchTerm(ent.Index, ent.Term) {
				rl.logger.Infof("Find different terms at the same log index(idx: %d, term: %d -> %d).",
					ent.Index, rl.ignoreTermError(rl.term(ent.Index)), ent.Term)
				switch {
				case ent.Index < rl.committed:
					rl.logger.Errorf("Try to append different entries which are already committed, ignore this append.")
					return ent.Index-1, false
				default:
					rl.unstable.append(ents[i:])
				}
				rl.maybeCommit(min(lastidx, committed))
				break
			}
		}
		return lastidx, true
	}
	var hint uint64
	for i := rl.firstIndex(); i < rl.lastIndex() + 1; i++ {
		hint = i
		if t, err := rl.term(i); err != nil || t >= term {
			break
		}
	}
	return hint, false
}

func (rl *raftLog) append(ents ...pb.Entry) {
	if len(ents) == 0 {
		return
	}
	rl.unstable.append(ents)
}

func (rl *raftLog) maybeCommit(to uint64) bool {
	if to > rl.committed {
		rl.logger.Infof("Commit updated: %d -> %d", rl.committed, to)
		rl.committed = min(to, rl.lastIndex())
		snap, ents := rl.unstable.stableTo(rl.committed)
		if snap != nil {
			rl.storage.ApplySnapshot(snap)
		}
		if len(ents) > 0 {
			err := rl.storage.Append(ents)
			if err != nil {
				panic(err) // TODO
			}
		}
		return true
	}
	return false
}

func (rl *raftLog) appliedTo(to uint64) uint64 {
	if to < rl.applied {
		return rl.applied
	}
	rl.applied = min(rl.committed, to)
	return rl.applied
}

func (rl *raftLog) lastTerm() uint64 {
	term, err := rl.term(rl.lastIndex())
	if err != nil {
		panic(err) // TODO
	}
	return term
}

func (rl *raftLog) isUpToDate(idx, term uint64) bool {
	return term > rl.lastTerm() || (term == rl.lastTerm() && idx >= rl.lastIndex())
}

func (rl *raftLog) restore(s *pb.Snapshot) {
	rl.unstable.restore(s)
}

func (rl *raftLog) entries(from, maxSize uint64) ([]pb.Entry, error) {
	if from > rl.lastIndex() {
		return nil, nil
	}
	return rl.slice(from, rl.lastIndex() + 1, maxSize)
}

// slice returns entries in [from, to).
func (rl *raftLog) slice(from, to, maxSize uint64) ([]pb.Entry, error) {
	if from >= to {
		return nil, nil
	}
	if from < rl.firstIndex() || to > rl.lastIndex() + 1 {
		return nil, ErrOutOfBound
	}
	slasti, err := rl.storage.LastIndex()
	if err != nil {
		return nil, err
	}
	var ret []pb.Entry
	if slasti >= from {
		ret, err = rl.storage.Entries(from, min(slasti + 1, to), maxSize)
		if err != nil {
			return nil, err
		}
	}
	from = max(slasti + 1, from)
	ret = append(ret, rl.unstable.entries(from, to, maxSize)...)
	return limitSize(ret, maxSize), nil
}

func (rl *raftLog) snapshot() *pb.Snapshot {
	if rl.unstable.snapshot != nil {
		return rl.unstable.snapshot
	}
	snap, err := rl.storage.Snapshot()
	if err != nil {
		panic(err) // TODO
	}
	return snap
}

func (rl *raftLog) ignoreTermError(term uint64, err error) uint64 {
	if err != nil {
		rl.logger.Errorf("Ignore term error: %v", err)
		return 0
	}
	return term
}