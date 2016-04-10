package	rafts

import (
	"math/rand"
	"time"
	"errors"
	"sort"
	pb "continuedb/rafts/pb"
)

var (
	// DefaultHeartbeatTick is the default heartbeat interval.
	DefaultHeartbeatTick = 1
	// DefaultElectionTick is the default election interval.
	DefaultElectionTick = 10
	// ErrNotLeader occurs when clients try to propose a request on a
	// non-leader raft instance.
	ErrNotLeader = errors.New("This instance isn't the raft leader.")
	// ErrNoId occurs when there's no id set.
	ErrNoneId = errors.New("Id can't be none.")
	// ErrNoInputChannel occurs when there's no inc set.
	ErrNoInputChannel = errors.New("Inc can't be none.")
	// ErrNoOutputChannel occurs when there's no outc set.
	ErrNoOutputChannel = errors.New("Outc can't be none.")
	// ErrAlreadyStarted occurs when try to start/restart an already started instance.
	ErrAlreadyStarted = errors.New("The instance has already started.")

	noLeadTypeSet = []pb.MessageType{
		pb.MessageType_MsgAppResp,
		pb.MessageType_MsgHeartbeatResp,
		pb.MessageType_MsgVote,
		pb.MessageType_MsgVoteResp,
	}
)

type uint64Slice []uint64
func (s uint64Slice) Len() int { return len(s) }
func (s uint64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s uint64Slice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type AdvanceFunc func(pb.Message)

type ProposeUnit struct {
	Type pb.EntryType
	Data []byte
	Ret chan bool
}

// Committed is the committed entries for application to apply.
type Committed struct {
	// From specifies the start index
	From uint64
	// Ents contains entries that are committed but not applied.
	Ents []pb.Entry
}

func (cmt *Committed) IsEmpty() bool {
	return len(cmt.Ents) == 0
}

// StateType is the role of a raft instance.
type StateType int

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// None for no current leader.
const None = 0

type ProgressState int
const (
	StateProbe ProgressState = iota
	StateReplica
	StateSnapshot
)

type Progress struct {
	match, next  uint64
	pendingSnapshot uint64
	alive bool
	paused bool
	state ProgressState
}

func (p *Progress) pause() { p.paused = true }
func (p *Progress) resume() { p.paused = false }
func (p *Progress) isPaused() bool { return p.paused }

func (p *Progress) maybeUpdate(to uint64) bool {
	var ret bool = false
	if to > p.match {
		p.match = to
		p.resume()
		ret = true
	}
	if p.next < to + 1 {
		p.next = to + 1
	}
	return ret
}

func (p *Progress) decreaseTo(to uint64) {
	if to < p.match {
		return
	}
	p.next = to
	if p.next < 1 {
		p.next = 1
	}
	p.resume()
}

func (p *Progress) maybeSnapshotEnd() bool {
	return p.state == StateSnapshot && p.match > p.pendingSnapshot
}

func (p *Progress) becomeProbe() {
	p.state = StateProbe
}

func (p *Progress) becomeReplica() {
	p.state = StateReplica
}

func (p *Progress) becomeSnapshot(si uint64) {
	p.state = StateSnapshot
	p.pendingSnapshot = si
	p.paused = true
}

// RaftConfig is the configuration for the raft instance.
type RaftConfig struct {
	Id uint64
	Storage Storage

	// Tickc for the custom logical clock.
	Tickc <-chan time.Time
	// Inc for the incoming raft messages.
	Inc chan pb.Message
	// Outc for the send-out raft messages.
	Outc chan pb.Message

	HeartbeatTick int
	ElectionTick  int

	MaxSizePerMsg uint64

	Logger Logger
}

func (c *RaftConfig) checkAndInitDefault() error {
	if c.Logger == nil {
		c.Logger = defaultLogger
	}
	if c.Id == 0 {
		return ErrNoneId
	}
	if c.Storage == nil {
		return errors.New("Storage can't be nil.")
	}
	if c.Tickc == nil {
		c.Tickc = defaultTickc
	}
	if c.Inc == nil {
		return ErrNoInputChannel
	}
	if c.Outc == nil {
		return ErrNoOutputChannel
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = DefaultHeartbeatTick
	}
	if c.ElectionTick == 0 {
		c.ElectionTick = DefaultElectionTick
	}
	return nil
}

// Raft is a instance of the node which implement the raft consensus
// algorithm. It should be hooked on the main grpc server at startup.
type Raft struct {
	pb.RaftMeta

	id uint64

	state StateType

	rand *rand.Rand

	maxSizePerMsg uint64

	// heartbeatElapsed and heartbeatTimeout is used for leaders to send
	// heartbeat periodically. A heartbeat message will be sent if
	// heartbeatElapsed >= heartbeatTimeout.
	heartbeatElapsed int
	heartbeatTimeout int

	// electionElapsed and electionTimeout is used as to check whether
	// the current term is expired. Current term is considered expired
	// if electionElapsed is great than some rand number which is
	// within [electionTimeout, 2 * electionTimeout - 1].
	electionElapsed int
	electionTimeout int

	// Raft leader.
	leader uint64
	// The followers' state in the view of leader.
	peers map[uint64]*Progress
	// Record the ones which instances vote for.
	votes map[uint64]bool
	// tickc is the signal channel of the logical clock.
	tickc    <-chan time.Time
	// tick is invoked once we get a value from tickc.
	tick     func()
	// advance is invoked once the raft instance receives a message.
	advance  AdvanceFunc

	logs     *raftLog
	// pendingConf marks whether we have a unapplied ConfChange.
	// We allow only one pending ConfChange when running.
	pendingConf bool

	inc, outc      chan pb.Message

	committedc chan Committed
	applyc chan uint64

	// propc is used for client to propose request.
	// It'll be set to nil when the instance thinks itself has been stopped.
	propc    chan ProposeUnit
	// confc is used for client to apply a committed ConfChange msg
	confc chan pb.ConfChange
	// stopc is used for noticing main loop to stop.
	stopc    chan struct{}
	// stopped will be closed once the instance is really stop.
	stoppedc chan struct{}

	logger   Logger
}

func NewRaft(config *RaftConfig) (*Raft, error) {
	err := config.checkAndInitDefault()
	if err != nil {
		return nil, err
	}
	r := &Raft{
		id: config.Id,
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		maxSizePerMsg: config.MaxSizePerMsg,
		heartbeatTimeout: config.HeartbeatTick,
		electionTimeout: config.ElectionTick,
		leader: None,
		peers: make(map[uint64]*Progress),
		votes: make(map[uint64]bool),
		tickc: config.Tickc,
		logs: newRaftLog(config.Storage, config.Logger),
		committedc: make(chan Committed),
		applyc: make(chan uint64),
		propc: make(chan ProposeUnit),
		confc: make(chan pb.ConfChange),
		inc: config.Inc,
		outc: config.Outc,
		stopc: make(chan struct{}),
		logger: config.Logger,
	}
	r.Commit = r.logs.committed
	return r, nil
}

func (r *Raft) hasLeader() bool { return r.leader != None }

func (r *Raft) isElectionTimeout() bool {
	d := r.electionElapsed - r.electionTimeout
	if d > 0 {
		return d > r.rand.Int() % r.electionTimeout
	}
	return false;
}

func (r *Raft) IsLeader() bool { return r.propc != nil && r.leader == r.id }

// Propose is used for clients to propose requests. ErrNotLeader
// will return if the instance isn't leader in the raft group.
func (r *Raft) Propose(data []byte) (bool, error) {
	if r.stoppedc != nil {
		ret := make(chan bool)
		r.logger.Infof("Raft instance(id: %d) propose data(%v).", r.id, data)
		r.propc <- ProposeUnit{Type: pb.EntryType_EntryNormal, Data: data, Ret: ret}
		ok := <- ret
		return ok, nil
	}
	r.logger.Infof("Raft instance(id: %d) can't propose anything since it's not running.", r.id)
	return false, ErrNotLeader
}

func (r *Raft) ProposeConfChange(cc pb.ConfChange) (bool, error) {
	if r.stoppedc != nil {
		ret := make(chan bool)
		r.logger.Infof("Raft instance(id: %d) propose conf change(%v).", r.id, cc)
		data, err := cc.Marshal()
		if err != nil {
			return false, err
		}
		r.propc <- ProposeUnit{Type: pb.EntryType_EntryConfChange, Data: data, Ret: ret}
		ok := <-ret
		return ok, nil
	}
	r.logger.Infof("Raft instance(id: %d) can't propose anything since it's not running.", r.id)
	return false, ErrNotLeader
}

func (r *Raft) GetCommitted() <-chan Committed {
	return r.committedc
}

func (r *Raft) AppliedTo(to uint64) {
	r.applyc <- to
}

// ApplyConfChange applies a committed ConfChange. It should be called on each raft instance.
// And it's the application's responsibility to guarantee the `cc` is committed.
// The instance will stop if it's removed from the raft group.
func (r *Raft) ApplyConfChange(cc pb.ConfChange) {
	r.confc <- cc
}

// prelude is the common should be run both start and restart.
func (r *Raft) prelude() error {
	if r.stoppedc != nil {
		return ErrAlreadyStarted
	}
	r.stoppedc = make(chan struct{})
	return nil
}

func (r *Raft) Start(peers []uint64) error {
	err := r.prelude()
	if err != nil {
		return err
	}
	var ereturn error
	for _, p := range peers {
		cc := pb.ConfChange{
			Type: pb.ConfChangeType_ConfChangeAddNode,
			Node: p,
		}
		d, err := cc.Marshal()
		if err != nil {
			ereturn = err
			r.logger.Errorf("Add node(%d) failed.", p)
			continue
		}
		ent := pb.Entry{
			Type: pb.EntryType_EntryConfChange,
			Index: r.logs.lastIndex() + 1,
			Term: 1,
			Data: d,
		}
		r.logs.append(ent)
		r.addNode(p)
	}
	r.logger.Infof("Raft instance(id: %d) started.", r.id)
	r.becomeFollower(1, None)
	go r.mainLoop()
	return ereturn
}

func (r *Raft) Restart() error {
	err := r.prelude()
	if err != nil {
		return err
	}
	if meta, cfg, err := r.logs.storage.InitialState(); err == nil {
		r.loadMeta(meta)
		if cfg != nil {
			for _, p := range cfg.Nodes {
				r.addNode(p)
			}
		}
	} else {
		r.logger.Panicf("Can't restart the raft instance: occurs error(%v) when load initial state.", err)
	}
	r.logger.Infof("Raft instance(id: %d) restarted.", r.id)
	r.becomeFollower(r.Term, None)
	go r.mainLoop()
	return nil
}

func (r *Raft) Stop() error {
	// We shouldn't remove the raft instance from the configuration, otherwise we'd have
	// too few instance for election soon.
	r.logger.Infof("Stopping the raft instance(id: %d).", r.id)
	r.stopc <- struct{}{}
	<-r.stoppedc
	close(r.stoppedc)
	r.stoppedc = nil
	r.logger.Infof("The raft instance(id: %d) already stopped.", r.id)
	return nil
}

func (r *Raft) IsStopped() bool {
	return r.stoppedc == nil
}

func (r *Raft) mainLoop() {
	var (
		cmt Committed
		err error
	)
	for {
		select {
		case <-r.stopc:
			r.stoppedc <- struct{}{}
			return
		case <-r.tickc:
			r.tick()
		case r.committedc <- cmt:
			cmt.From = max(r.logs.applied + 1, r.logs.firstIndex())
			cmt.Ents, err = r.logs.slice(cmt.From, r.Commit + 1, noLimit)
			if err != nil {
				r.logger.Panicf("Raft instance(id: %d) can't get Committed: %v", r.id, err)
			}
		case to := <-r.applyc:
			r.logger.Infof("Raft instance(id: %d) applied index to %d.", r.id, to)
			r.logs.appliedTo(to)
		case cc := <-r.confc:
			r.logger.Infof("Raft instance(id: %d) applied ConfChange(%v).", r.id, cc)
			if cc.Node == 0 {
				r.pendingConf = false
				continue
			}
			switch cc.Type {
			case pb.ConfChangeType_ConfChangeAddNode:
				r.addNode(cc.Node)
			case pb.ConfChangeType_ConfChangeRemoveNode:
				r.removeNode(cc.Node)
				r.Stop()
			}
		case unit := <-r.propc:
			if unit.Type == pb.EntryType_EntryConfChange {
				if r.pendingConf {
					// Simply drop the proposal
					r.logger.Infof("Raft instance(id: %d) dropped the ConfChange proposal(%v)" +
						" since we have a pending one.", r.id, unit)
					unit.Ret <- false
					continue
				}
				r.pendingConf = true
			}
			unit.Ret <- true
			ent := pb.Entry{
				Type: unit.Type,
				Index: r.logs.lastIndex() + 1,
				Term: r.Term,
				Data: unit.Data,
			}
			msg := pb.Message{
				Type: pb.MessageType_MsgProp,
				From: r.id, To: r.id,
				Term: r.Term,
				Entries: append([]pb.Entry{}, ent),
			}
			r.advance(msg)
		case msg := <-r.inc:
			if r.Term > msg.Term {
				r.logger.Infof("Raft instance(id: %d) received a out of date message(%v), ignore it.", r.id, msg)
				continue
			}
			r.logger.Infof("Raft instance(id: %d) received message: %v", r.id, msg)
			if r.Term < msg.Term {
				lead := msg.From
				if oneOf(msg.Type, noLeadTypeSet) {
					lead = None
				}
				r.becomeFollower(msg.Term, lead)
			}
			r.advance(msg)
			if r.logs.committed > r.Commit {
				r.logger.Infof("Raft instance(id: %d) updated commit from %d to %d.", r.id, r.Commit, r.logs.committed)
				r.Commit = r.logs.committed
			}
		}
	}
}

func (r *Raft) loadMeta(meta pb.RaftMeta) {
	r.logs.committed = meta.Commit
	r.Term = max(meta.Term, 1)
	r.Vote = meta.Vote
	r.Commit = meta.Commit
}

func (r *Raft) addNode(id uint64) {
	if _, ok := r.peers[id]; !ok {
		r.peers[id] = &Progress{match: 0, next: r.logs.lastIndex() + 1, state: StateProbe, alive: true}
	}
	r.pendingConf = false
}

func (r *Raft) removeNode(id uint64) {
	if _, ok := r.peers[id]; ok {
		delete(r.peers, id)
	}
	r.pendingConf = false
}

func (r *Raft) becomeFollower(term, leader uint64) {
	r.state = StateFollower
	r.reset(term)
	r.leader = leader
	r.tick = r.tickElection
	r.advance = r.advanceFollower
	r.logger.Infof("Raft instance(id: %d) become follower(leader: %d) at term %d.", r.id, r.leader, r.Term)
}

func (r *Raft) becomeCandidate() {
	r.state = StateCandidate
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	r.advance = r.advanceCandidate
	r.Vote = r.id
	r.votes[r.id] = true
	r.logger.Infof("Raft instance(id: %d) become candidate at term %d.", r.id, r.Term)
}

func (r *Raft) becomeLeader() {
	r.state = StateLeader
	r.reset(r.Term + 1)
	r.leader = r.id
	r.tick = r.tickHeartbeat
	r.advance = r.advanceLeader
	r.logger.Infof("Raft instance(id: %d) become leader at term %d.", r.id, r.Term)
}

func (r *Raft) reset(term uint64) {
	if term > r.Term {
		r.Term = term
		r.Vote = None
	}
	r.leader = None
	r.electionElapsed = 0
	r.heartbeatTimeout = 0
	r.pendingConf = false
	r.votes = make(map[uint64]bool)
	for id := range r.peers {
		r.peers[id] = &Progress{match: 0, next: r.logs.lastIndex() + 1, alive: true}
		if id == r.id {
			r.peers[id].match = r.logs.lastIndex()
		}
	}
}

func (r *Raft) sendTo(id uint64, msg pb.Message) {
	if _, ok := r.peers[id]; !ok {
		r.logger.Infof("Raft instance(id: %d) try to send to an unknown id(%d), ignore it.", r.id, id)
		return
	}
	msg.From = r.id
	msg.To = id
	msg.Term = r.Term
	msg.Commit = min(r.peers[id].match, r.Commit)
	r.logger.Infof("Raft instance(id: %d) send msg(%v) to %d.", r.id, msg, id)
	r.outc <- msg
}

func (r *Raft) broadcast(msg pb.Message) {
	for id := range r.peers {
		if id == r.id {
			continue
		}
		r.sendTo(id, msg)
	}
}

func (r *Raft) q() int { return len(r.peers) / 2 + 1 }

func (r *Raft) poll(id uint64, vote bool) (cnt int) {
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = vote
	}
	for _, v := range(r.votes) {
		if v {
			cnt++
		}
	}
	return cnt
}

func (r *Raft) Campaign() {
	r.becomeCandidate()
	if r.q() <= r.poll(r.id, true) {
		r.becomeLeader()
	}
	msg := pb.Message{
		Type: pb.MessageType_MsgVote,
		PrevTerm: r.logs.lastTerm(),
		PrevIndex: r.logs.lastIndex(),
	}
	r.broadcast(msg)
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.isElectionTimeout() {
		r.Campaign()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		for _, p := range r.peers {
			p.alive = false
		}
		msg := pb.Message{Type: pb.MessageType_MsgHeartbeat}
		r.broadcast(msg)
	}
}

func (r *Raft) advanceLeader(m pb.Message) {
	switch m.Type {
	case pb.MessageType_MsgAppResp:
		if m.Reject {
			r.peers[m.From].decreaseTo(m.PrevIndex)
			if r.peers[m.From].state == StateReplica {
				r.peers[m.From].becomeProbe()
			}
			r.sendAppend(m.From)
		} else if r.peers[m.From].maybeUpdate(m.PrevIndex) {
			p := r.peers[m.From]
			switch {
			case p.state == StateProbe:
				p.becomeReplica()
			case p.state == StateSnapshot && p.maybeSnapshotEnd():
				p.becomeProbe()
			}
			r.maybeCommit()
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgHeartbeatResp:
		r.peers[m.From].alive = true
	case pb.MessageType_MsgProp:
		r.logger.Infof("Raft instance(id: %d) receivd proposal(%v).", r.id, m)
		if _, ok := r.peers[r.id]; !ok {
			r.logger.Infof("Raft instance(id: %d) isn't included in the raft group, ignore proposal.", r.id)
			return
		}
		r.logs.append(m.Entries...)
		r.bcastAppend()
	case pb.MessageType_MsgVote:
		msg := pb.Message{Type: pb.MessageType_MsgVoteResp, Reject: true}
		r.sendTo(m.From, msg)
	}
}

func (r *Raft) advanceFollower(m pb.Message) {
	switch m.Type {
	case pb.MessageType_MsgProp:
		r.logger.Infof("Raft instance(id: %d) is not leader, ignore proposal.", r.id)
	case pb.MessageType_MsgApp:
		r.electionElapsed = 0
		r.handleAppend(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.leader = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnap:
		r.electionElapsed = 0
		r.handleSnapshot(m)
	case pb.MessageType_MsgVote:
		msg := pb.Message{Type: pb.MessageType_MsgVoteResp}
		if (r.Vote == None || r.Vote == m.From) && r.logs.isUpToDate(m.PrevIndex, m.PrevTerm) {
			r.electionElapsed = 0
			r.logger.Infof("Raft instance(id: %d) voted for %d.", r.id, m.From)
			r.Vote = m.From
			msg.Reject = false
		} else {
			r.logger.Infof("Raft instance(id: %d) rejected to vote for %d", r.id, m.From)
			msg.Reject = true
		}
		r.sendTo(m.From, msg)
	}
}

func (r *Raft) advanceCandidate(m pb.Message) {
	switch m.Type {
	case pb.MessageType_MsgProp:
		r.logger.Infof("Raft instance(id: %id) is not leader, ignore proposal.", r.id)
	case pb.MessageType_MsgApp:
		r.becomeFollower(m.Term, m.From)
		r.handleAppend(m)
	case pb.MessageType_MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgVote:
		// m.From must reject our vote request if it's a candidate.
		r.poll(m.From, false)
		msg := pb.Message{Type: pb.MessageType_MsgVoteResp, Reject: true}
		r.sendTo(m.From, msg)
	case pb.MessageType_MsgVoteResp:
		gr := r.poll(m.From, !m.Reject)
		switch r.q() {
		case gr:
			r.becomeLeader()
			r.bcastAppend()
		case len(r.votes) - gr:
			r.becomeFollower(r.Term, None)
		}
	}
}

func (r *Raft) handleHeartbeat(m pb.Message) {
	r.logs.maybeCommit(m.Commit)
	msg := pb.Message{Type: pb.MessageType_MsgHeartbeatResp}
	r.sendTo(m.From, msg)
}

func (r *Raft) handleAppend(m pb.Message) {
	idx, ok := r.logs.maybeAppend(m.PrevIndex, m.PrevTerm, m.Commit, m.Entries)
	if !ok {
		r.logger.Infof("Raft instance(id: %d) reject the app msg(%v).", r.id, m)
	}
	msg := pb.Message{Type: pb.MessageType_MsgAppResp, Reject: !ok, PrevIndex: idx}
	r.sendTo(m.From, msg)
}

func (r *Raft) handleSnapshot(m pb.Message) {
	if r.maybeRestore(m.Snap) {
		r.logger.Infof("Raft instance(id: %d) restore snapshot(Term: %d, Index: %d)", r.id, m.Snap.Term, m.Snap.Index)
	} else {
		r.logger.Infof("Raft instance(id: %d) ignore snapshot(Term %d, Index: %d)", r.id, m.Snap.Term, m.Snap.Index)
	}
	msg := pb.Message{
		Type: pb.MessageType_MsgAppResp,
		Reject: false,
		PrevIndex: r.logs.lastIndex(),
		PrevTerm: r.logs.lastTerm(),
	}
	r.sendTo(m.From, msg)
}

func (r *Raft) maybeRestore(s *pb.Snapshot) bool {
	if s.Index <= r.logs.committed {
		return false
	}
	if r.logs.matchTerm(s.Index, s.Term) {
		r.logs.maybeCommit(s.Index)
		return false
	}
	r.logs.restore(s)
	r.peers = make(map[uint64]*Progress)
	for _, n := range s.Conf.Nodes {
		match, next := uint64(0), r.logs.lastIndex() + 1
		if n == r.id {
			match = r.logs.lastIndex()
		}
		r.peers[n] = &Progress{match: match, next: next, state: StateProbe, alive: true}
	}
	return true
}

func (r *Raft) maybeCommit() bool {
	matches := make(uint64Slice, 0, len(r.peers))
	for i := range r.peers {
		matches = append(matches, r.peers[i].match)
	}
	sort.Sort(sort.Reverse(matches))
	to := matches[r.q()-1]
	return r.logs.maybeCommit(to)
}

func (r *Raft) bcastAppend() {
	for id := range r.peers {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// sendAppend sends apppend message to `to`.
func (r *Raft) sendAppend(to uint64) {
	p, ok := r.peers[to]
	if !ok || p.isPaused() || !p.alive {
		return
	}
	var msg pb.Message
	term, errt := r.logs.term(p.next-1)
	ents, erre := r.logs.entries(p.next, r.maxSizePerMsg)

	if erre != nil || errt != nil {
		// we can't send app msg, send snapshot
		msg.Type = pb.MessageType_MsgSnap
		msg.Snap = r.logs.snapshot()
		p.becomeSnapshot(msg.Snap.Index)
	} else {
		if p.state == StateProbe && len(ents) != 0 {
			ents = append([]pb.Entry{}, ents[0])
			p.pause()
		}
		msg.Type = pb.MessageType_MsgApp
		msg.PrevIndex = p.next-1
		msg.PrevTerm = term
		msg.Entries = ents
	}
	r.sendTo(to, msg)
}