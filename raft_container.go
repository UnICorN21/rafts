package rafts

import (
	pb "continuedb/rafts/pb"
	"sync"
	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"continuedb/util"
	"time"
	"continuedb/rpc"
)

const (
	// defaultPipeBufferSize is the default buffer size of the pipe to avoid
	// some unnecessary wait on the variable.
	defaultPipeBufferSize = 64
)

var (
	retryOptions = &util.RetryOption{MaxBackoff: 30 * time.Second}
)

func NewContainerMeta(id uint64, host, port string) *pb.NodeMeta {
	return &pb.NodeMeta{id, host+port}
}

type remoteContainer struct {
	meta *pb.NodeMeta
	conn *grpc.ClientConn
	client pb.RaftClient
}

type Router interface {
	// Lookup looks up the node id that holds the given raft instance.
	// Return nil if lookup failed.
	Lookup(id uint64) *pb.NodeMeta
	// SetRoute set route for raft rid to node nid.
	SetRoute(rid uint64, meta *pb.NodeMeta)
}

type localRaft struct {
	*Raft
	removed chan struct{}
}

// RaftContainer is the container to hold raft instances.
// It's tightly bounded with grpc specified in pb directory.
// A server node may have lots of raft instances, but it's allowed to
// have one RaftContainer at most. RaftContainer is responsible for fake
// raft heartbeats and dispatch msgs to different node.
type RaftContainer struct {
	ctx *rpc.Context

	started bool
	pipe   chan pb.Message
	stopc  chan struct{}

	router Router
	// mu for protecting the following.
	mu     sync.Mutex
	// locals maps the raft id to the local raft instance.
	locals map[uint64]*localRaft
	nodes  map[uint64]*remoteContainer
}

// RaftContainer should implement RaftServer interface.
var _ pb.RaftServer = &RaftContainer{}

func NewRaftContainer(ctx *rpc.Context, router Router, grpcServer *grpc.Server) *RaftContainer {
	rc := &RaftContainer{
		ctx: ctx,
		pipe: make(chan pb.Message, defaultPipeBufferSize),
		stopc: make(chan struct{}),
		router: router,
		locals: make(map[uint64]*localRaft),
		nodes: make(map[uint64]*remoteContainer),
	}
	retryOptions.Stopper = rc.stopc
	pb.RegisterRaftServer(grpcServer, rc)

	return rc
}

func (rc *RaftContainer) Add(r *Raft) {
	if r == nil {
		return
	}

	rc.mu.Lock()
	if _, ok := rc.locals[r.id]; ok {
		return
	}
	local := &localRaft{
			Raft: r,
			removed: make(chan struct{}),
	}
	rc.locals[r.id] = local
	rc.mu.Unlock()

	if rc.started {
		go rc.redirect(local)
	}
}

func (rc *RaftContainer) Remove(id uint64) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if l, ok := rc.locals[id]; ok {
		l.removed <- struct{}{}
		delete(rc.locals, id)
	}
}

func (rc *RaftContainer) Start() {
	for _, l := range rc.locals {
		go rc.redirect(l)
	}
	rc.started = true
	go func() {
		for {
			select {
			case m := <-rc.pipe:
				rc.send(m)
			case <-rc.stopc:
				return
			}
		}
	}()
}

func (rc *RaftContainer) Stop() {
	rc.stopc <- struct{}{}
	rc.started = false
}

func (rc *RaftContainer) Pipe(context context.Context, m *pb.Message) (*pb.Response, error) {
	resp := &pb.Response{}
	if r, ok := rc.locals[m.To]; ok {
		if r.IsStopped() {
			resp.State = pb.MessageState_StateRaftStopped
		} else {
			select {
			case r.inc <- *m:
				resp.State = pb.MessageState_StateOk
			default:
				resp.State = pb.MessageState_StateFailed
			}
		}
	} else {
		resp.State = pb.MessageState_StateRaftNotExist
		if meta := rc.router.Lookup(m.To); meta.Id != rc.ctx.NodeMeta.Id {
			resp.Container = meta
		}
	}
	return resp, nil
}

func (rc *RaftContainer) redirect(l *localRaft) {
	for {
		select {
		case msg := <-l.outc:
			rc.pipe <- msg
		case <-l.removed:
			return
		case <-l.stoppedc:
			return
		case <-rc.stopc:
			return
		}
	}
}

// send sends message to the corresponding raft instance.
// It guarantees the message will be received and handled exactly once
// unless the raft instance which will receive had been already stopped.
func (rc *RaftContainer) send(m pb.Message) {
	// if id is on the local machine
	// NOTE: In fact, we rarely send local messages.
	if r, ok := rc.locals[m.To]; ok {
		r.inc <- m
		return
	}
	for r := util.NewRetry(retryOptions); r.Next(); {
		c := rc.router.Lookup(m.To)
		if c != nil {
			log.Infof("RaftContainer(%d) tried to send msg to container(%d).", rc.ctx.NodeMeta.Id, c.Id)

			if _, ok := rc.nodes[c.Id]; !ok {
				conn, err := rc.ctx.GRPCDial(c.Addr)
				if err != nil {
					// make next retry if we meet an error
					continue
				}
				rc.nodes[c.Id] = &remoteContainer{
					meta: c,
					conn: conn,
					client: pb.NewRaftClient(conn),
				}
			}

			resp, err := rc.nodes[c.Id].client.Pipe(context.Background(), &m)
			if err != nil {
				log.Errorf("Error while sending msg(%v) to node(%d): %v", m, c.Id, err)
				continue
			}
			switch resp.State {
			case pb.MessageState_StateOk:
				return
			case pb.MessageState_StateRaftStopped:
				log.Infof("Raft instance(%d) already stopped.", m.To)
				return
			case pb.MessageState_StateRaftNotExist:
				if resp.Container != nil {
					rc.router.SetRoute(m.To, resp.Container)
					r.Reset()
				}
			case pb.MessageState_StateFailed:
			}
		} else {
			log.Infof("RaftContainer(%d) didn't found the location of raft(%d).", rc.ctx.NodeMeta.Id, m.To)
		}
	}
}