package simulation

import (
	"reflect"
	"fmt"
	"sync"
	"math/rand"
	"time"
	pb "continuedb/rafts/pb"
)

type Hub struct {
	running bool

	mu sync.Mutex
	ins map[int]uint64
	incases []reflect.SelectCase
	outs map[uint64]chan pb.Message
	done chan struct{}

	real bool
	droprate, delayrate float32
	maxdelay int

	silent bool
}

func NewHub(real, silent bool) *Hub {
	return &Hub{
		ins: make(map[int]uint64),
		incases: make([]reflect.SelectCase, 0),
		outs: make(map[uint64]chan pb.Message),
		done: make(chan struct{}),

		real: real,
		droprate: 0.1,
		delayrate: 0.3,
		maxdelay: 200,

		silent: silent,
	}
}

func (h *Hub) SetRealArgs(delayrate, droprate float32, maxdelay int) bool {
	if h.running {
		return false
	}
	h.delayrate, h.droprate = delayrate, droprate
	h.maxdelay = maxdelay
	return true
}

func (h *Hub) Register(id uint64, out, in chan pb.Message) {
	cs := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(in)}
	h.incases = append(h.incases, cs)
	h.ins[len(h.incases)-1] = id
	h.outs[id] = out
}

func (h *Hub) Run() {
	h.running = true
	go func(){
		for {
			select {
			case <-h.done:
				fmt.Printf("Hub stopped.\n")
				return
			default:
				h.mu.Lock()
				ch, val, ok := reflect.Select(h.incases)
				id, iok := h.ins[ch]
				h.mu.Unlock()
				if !iok {
					return
				}
				if !ok {
					if !h.silent {
						fmt.Printf("Node(%d) dropped.\n", id)
					}
					h.mu.Lock()
					delete(h.ins, ch)
					h.incases[ch].Chan = reflect.ValueOf(nil)
					delete(h.outs, id)
					h.mu.Unlock()
				} else {
					go func() {
						msg := val.Interface().(pb.Message)
						if !h.silent {
							fmt.Printf("Hub: transimiting msg: %v\n", msg)
						}
						if h.real {
							if rand.Float32() < h.droprate {
								return
							}
							if rand.Float32() < h.delayrate {
								d := rand.Intn(h.maxdelay)
								time.Sleep(time.Duration(d))
							}
						}
						h.outs[msg.To] <- msg
					}()
				}
			}
		}
	}()
}

func (h *Hub) Stop() {
	h.done <- struct{}{}
	h.running = false
}
