package rafts

import (
	"errors"
	pb "continuedb/rafts/pb"
)

// Storage is the backend for applications to persist raft logs.
// We hold no assumption about the persisted entries but entries
// should be continuous from first index to last index.
type Storage interface {
	// InitialState return the initial configuration of the raft instance.
	InitialState() (pb.RaftMeta, *pb.Configuration, error)
	// FirstIndex return the first index the storage persists.
	FirstIndex() (uint64, error)
	// LastIndex return the last index the storage persists.
	LastIndex() (uint64, error)
	// Term return the term of the given index.
	Term(idx uint64) (uint64, error)
	// Entries return the array of log entries which index are in [lo, hi).
	Entries(lo, hi, maxsize uint64) ([]pb.Entry, error)
	// Append appends ents to the given storage. It'll cover the old if exists.
	Append(ents []pb.Entry) error
	// Snapshot return the snapshot.
	Snapshot() (*pb.Snapshot, error)
	// CreateSnapshot create a snapshot containing the cfg and data.
	CreateSnapshot(cfg *pb.Configuration, data []byte) error
	// ApplySnapshot applies the given snapshot.
	ApplySnapshot(snapshot *pb.Snapshot) error
}

var (
	ErrOutOfBound = errors.New("The log index is out of bound.")
	ErrCompacted = errors.New("The log has been compacted.")
	ErrMissIndex = errors.New("Missing index.")
	ErrUnavailable = errors.New("requested entry at index is unavailable")
)

// InMemStorage stores logs in memory.
type InMemStorage struct {
	snapshot pb.Snapshot
	meta     pb.RaftMeta
	logs     []pb.Entry
	logger   Logger
}

func NewInMemStorage(logger Logger) *InMemStorage {
	if logger == nil {
		logger = defaultLogger
	}
	s := &InMemStorage{
		logs: make([]pb.Entry, 0),
		logger: logger,
	}
	// dummy entry
	var ent pb.Entry
	s.logs = append(s.logs, ent)
	return s
}

func (ms *InMemStorage) InitialState() (pb.RaftMeta, *pb.Configuration, error) {
	return ms.meta, ms.snapshot.Conf, nil
}

func (ms *InMemStorage) SetMeta(state pb.RaftMeta) {
	ms.meta = state
}

func (ms *InMemStorage) FirstIndex() (uint64, error) {
	return ms.firstIndex(), nil
}

func (ms *InMemStorage) firstIndex() uint64 {
	// logs[0] is a dummy entry containing info about the last (term, index) of the snapshot.
	return ms.logs[0].Index + 1
}

func (ms *InMemStorage) LastIndex() (uint64, error) {
	return ms.lastIndex(), nil
}

func (ms *InMemStorage) lastIndex() uint64 {
	return ms.logs[0].Index + uint64(len(ms.logs)) - 1
}

func (ms *InMemStorage) Term(idx uint64) (uint64, error) {
	off := ms.logs[0].Index
	if idx < off {
		return 0, ErrCompacted
	}
	return ms.logs[idx-off].Term, nil
}

func (ms *InMemStorage) Entries(lo, hi, maxsize uint64) ([]pb.Entry, error) {
	if lo < ms.firstIndex() || hi  > ms.lastIndex() + 1 {
		return nil, ErrOutOfBound
	}
	off := ms.logs[0].Index
	return limitSize(ms.logs[lo-off:hi-off], maxsize), nil
}

func (ms *InMemStorage) Snapshot() (*pb.Snapshot, error) {
	return &ms.snapshot, nil
}

func (ms *InMemStorage) CreateSnapshot(cfg *pb.Configuration, data []byte) error {
	term, err := ms.Term(ms.lastIndex())
	if err != nil {
		return err
	}
	ms.snapshot = pb.Snapshot{
		Conf: cfg,
		Index: ms.lastIndex(),
		Term: term,
		Data: data,
	}
	return nil
}

func (ms *InMemStorage) ApplySnapshot(snapshot *pb.Snapshot) error {
	ms.snapshot = *snapshot
	ms.logs = append([]pb.Entry(nil), pb.Entry{Index: snapshot.Index, Term: snapshot.Term})
	return nil
}

func (ms *InMemStorage) Append(ents []pb.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	first := ms.logs[0].Index
	last := ents[0].Index + uint64(len(ents)) - 1
	if first > last {
		return nil
	}
	off := ents[0].Index - ms.logs[0].Index
	switch {
	case off < uint64(len(ms.logs)):
		ms.logs = append([]pb.Entry{}, ms.logs[:off]...)
		ms.logs = append(ms.logs, ents...)
	case off == uint64(len(ms.logs)):
		ms.logs = append(ms.logs, ents...)
	default:
		ms.logger.Errorf("Can't append ents: missing index (%d, %d)",
			ms.lastIndex(), ents[0].Index)
		return ErrMissIndex
	}
	return nil
}