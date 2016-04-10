package simulation

import (
	"testing"
	"fmt"
	"time"
	"math/rand"
	"sync"
	pb "continuedb/rafts/pb"
)

type StubNode struct {
	id uint64
	in, out chan pb.Message
	done chan struct{}
	mu sync.Mutex
	mailbox []pb.Message
}

func newStubNode(id uint64) *StubNode {
	return &StubNode{
		id: id,
		in: make(chan pb.Message),
		out: make(chan pb.Message),
		done: make(chan struct{}),
	}
}

func (sn *StubNode) mailboxSize() int {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	return len(sn.mailbox)
}

func (sn *StubNode) send(msg pb.Message, to uint64) {
	msg.From = sn.id
	msg.To = to
	sn.out <- msg
}

func (sn *StubNode) run() {
	go func(){
		for {
			select {
			case msg := <-sn.in:
				fmt.Printf("Node(%d) receives msg: %v\n", sn.id, msg)
				sn.mu.Lock()
				sn.mailbox = append(sn.mailbox, msg)
				sn.mu.Unlock()
				var m pb.Message
				switch msg.Type {
				case pb.MessageType_MsgApp:
					m.Type = pb.MessageType_MsgAppResp
				case pb.MessageType_MsgHeartbeat:
					m.Type = pb.MessageType_MsgHeartbeatResp
				case pb.MessageType_MsgVote:
					m.Type = pb.MessageType_MsgVoteResp
				default:
					continue
				}
				sn.send(m, msg.From)
			case <-sn.done:
				close(sn.in)
				close(sn.out)
				fmt.Printf("Node(%d) stopped.\n", sn.id)
				return
			}
		}
	}()
}

func (sn *StubNode) stop() {
	sn.done <- struct{}{}
}

func TestHub(t *testing.T) {
	h := NewHub(false, false)

	nodes := make([]*StubNode, 0, 5)
	for i := 0; i < 5; i++ {
		nodes = append(nodes, newStubNode(uint64(i)))
		h.Register(uint64(i), nodes[i].in, nodes[i].out)
		nodes[i].run()
	}

	h.Run()

	testMessageSR(t, nodes)

	go func(){
		time.Sleep(500 * time.Millisecond)
		h.Stop()
	}()

	<-h.done
}

func testMessageSR(t *testing.T, nodes []*StubNode) {
	msg := pb.Message{Type: pb.MessageType_MsgApp}
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	sender := nodes[0]
	mts := make([]int, len(nodes))
	for i := range mts {
		if i == 0 {
			mts[i] = 0
		} else {
			mts[i] = rand.Intn(6)
		}
	}
	for i, n := range nodes {
		if n.id == sender.id {
			continue
		}
		fmt.Printf("Need to send %d msgs to %d\n", mts[i], n.id)
		for j := 0; j < mts[i]; j++ {
			sender.send(msg, n.id)
		}
	}
	// Wait a little to leave time for transmission.
	time.Sleep(500 * time.Microsecond)
	var sum int
	for i, n := range nodes {
		if n.id == sender.id {
			continue
		}
		sum += mts[i]
		if mts[i] != n.mailboxSize() {
			t.Fatalf("Node(%d) missing msg(recv: %d, want: %d).\n", n.id, n.mailboxSize(), mts[i])
		} else {
			t.Logf("Node(%d) receives %d msgs.\n", n.id, n.mailboxSize())
		}
	}
	if sum != sender.mailboxSize() {
		t.Fatalf("Node(%d) missing msg(recv: %d, want: %d).\n", sender.id, sender.mailboxSize(), sum)
	} else {
		t.Logf("Node(%d) receives %d msgs.\n", sender.id, sender.mailboxSize())
	}
	for _, n := range nodes {
		n.stop()
	}
}
