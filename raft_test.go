package rafts

import (
	"testing"
	"time"
	"fmt"
	"continuedb/rafts/simulation"
	pb "continuedb/rafts/pb"
	"math/rand"
	"sync"
)

const (
	NUM_OF_PEERS = 3
	NUM_OF_AUX = 1
	NUM_OF_PROPOSAL = 21
)

// TestRaft tests the normal runtime of a raft group.
// It's always pass currently. One should check the output log
// to make sure everything is done.
func TestRaft(t *testing.T) {
	var (
		rafts []*Raft
		peers []uint64
		auxr []*Raft
		aux []uint64
		// startup stores flag for raft bootstrap: 0 for start, 1 for restart.
		startup []float32
		stopc chan struct{}
		random *rand.Rand
	)
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
	stopc = make(chan struct{})
	hub := simulation.NewHub(false, true)
	for i := 0; i < NUM_OF_PEERS; i++ {
		peers = append(peers, uint64(i+1))
		startup = append(startup, random.Float32())
	}
	for i := 0; i < NUM_OF_AUX; i++ {
		aux = append(aux, uint64(len(peers) + i + 1))
	}
	for i, id := range peers {
		inc := make(chan pb.Message)
		outc := make(chan pb.Message)
		storage := NewInMemStorage(nil)

		if startup[i] > 0.5 {
			// restore snapshot to storage
			snapshot := &pb.Snapshot{
				Conf: &pb.Configuration{
					Nodes: peers,
				},
				Index: uint64(len(peers)),
				Term: 1,
			}
			storage.ApplySnapshot(snapshot)
			meta := pb.RaftMeta{Term: 2, Vote: 0, Commit: 1}
			storage.SetMeta(meta)
		}

		config := &RaftConfig{Id: id, Inc: inc, Outc: outc, Storage: storage, MaxSizePerMsg: 100000}
		r, err := NewRaft(config)
		if err != nil {
			panic(err)
		}
		rafts = append(rafts, r)

		hub.Register(id, inc, outc)
	}

	for _, id := range aux {
		inc := make(chan pb.Message)
		outc := make(chan pb.Message)
		config := &RaftConfig{Id: id, Inc: inc, Outc: outc, Storage: NewInMemStorage(nil)}
		n, err := NewRaft(config)
		if err != nil {
			fmt.Printf("Error when create tmp raft instance: %v\n", err)
		}
		auxr = append(auxr, n)

		hub.Register(id, inc, outc)
	}

	hub.Run()
	for i, r := range rafts {
		if startup[i] > 0.5 {
			r.Restart()
		} else {
			r.Start(peers)
		}
	}
	go func() {
		// cnt counts for proposals
		var (
			mu sync.Mutex
			cnt int
		)
		for {
			select {
			case <-stopc:
				fmt.Printf("Stopped")
				for _, r := range rafts {
					r.Stop()
				}
				hub.Stop()
				return
			default:
				time.Sleep(20 * time.Millisecond)
				Tick()
				go func() {
					mu.Lock()
					defer mu.Unlock()
					if _, ok := hasLeader(rafts); cnt >= NUM_OF_PROPOSAL || !ok {
						return
					}
					for _, n := range rafts {
						cmt := <-n.GetCommitted()
						if !cmt.IsEmpty() {
							for _, e := range cmt.Ents {
								if e.Type == pb.EntryType_EntryConfChange {
									var cc pb.ConfChange
									cc.Unmarshal(e.Data)
									n.ApplyConfChange(cc)
								}
							}
							n.AppliedTo(cmt.From + uint64(len(cmt.Ents)) - 1)
						}
					}
					if cnt == 2 {
						ld, _ := hasLeader(rafts)
						fmt.Printf("ld.pendingConf = %v\n", ld.pendingConf)
						ld.ProposeConfChange(pb.ConfChange{Type: pb.ConfChangeType_ConfChangeAddNode, Node: aux[0]})
						auxr[0].Start(append(peers, aux[0]))
					}
					if cnt == 8 {
						ld, _ := hasLeader(rafts)
						fmt.Printf("ld.pendingConf = %v\n", ld.pendingConf)
						ld.ProposeConfChange(pb.ConfChange{Type: pb.ConfChangeType_ConfChangeRemoveNode, Node: aux[0]})
					}
					if random.Float32() < .7 {
						pr := randr(random, rafts)
						pr.Propose([]byte{byte(cnt)})
						cnt++
					}
					if random.Float32() < .4 {
						sr := randr(random, rafts)
						sr.Stop()
						if random.Float32() < .5 {
							s := &pb.Snapshot{}
							sr.logs.storage.ApplySnapshot(s)
							sr.logs.restore(s)
							sr.logs.storage.(*InMemStorage).SetMeta(pb.RaftMeta{})
						}
						time.Sleep(10 * time.Millisecond)
						sr.Restart()
					}
				}()
			}
		}
	}()
	go func () {
		time.Sleep(12000 * time.Millisecond)
		fmt.Printf("Stop")
		stopc <- struct{}{}
	}()
	<-stopc
}

func hasLeader(rafts []*Raft) (*Raft, bool) {
	for _, r := range rafts {
		if r.IsLeader() {
			return r, true
		}
	}
	return nil, false
}

func randr(r *rand.Rand, rafts []*Raft) *Raft {
	return rafts[int(r.Float32() * float32(len(rafts)))]
}

