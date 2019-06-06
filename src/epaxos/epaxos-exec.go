package epaxos

import (
	//"log"
	"epaxosproto"
	"genericsmrproto"
	"sort"
	"state"
	"time"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

type Exec struct {
	r *Replica
}

type SCComponent struct {
	nodes []*Instance
	color int8
}

func (e *Exec) executeCommand(replica int32, instance int32) bool {
	if e.r.InstanceSpace[replica][instance] == nil {
		return false
	}
	inst := e.r.InstanceSpace[replica][instance]
	if inst.Status == epaxosproto.EXECUTED {
		return true
	}
	if inst.Status != epaxosproto.COMMITTED {
		return false
	}

	if !e.findSCC(inst) {
		return false
	}

	return true
}

var stack []*Instance = make([]*Instance, 0, 100)

func (e *Exec) findSCC(root *Instance) bool {
	index := 1
	//find SCCs using Tarjan's algorithm
	stack = stack[0:0]
	return e.strongconnect(root, &index)
}

func (e *Exec) strongconnect(v *Instance, index *int) bool {
	v.Index = *index
	v.Lowlink = *index
	*index = *index + 1

	l := len(stack)
	if l == cap(stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, stack)
		stack = newSlice
	}
	stack = stack[0 : l+1]
	stack[l] = v

	for q := int32(0); q < int32(e.r.N); q++ {
		inst := v.Deps[q]
		for i := e.r.ExecedUpTo[q] + 1; i <= inst; i++ {
			for e.r.InstanceSpace[q][i] == nil || e.r.InstanceSpace[q][i].Cmds == nil || v.Cmds == nil {
				time.Sleep(1000 * 1000)
			}

			if e.r.InstanceSpace[q][i].Status == epaxosproto.EXECUTED {
				continue
			}

			for e.r.InstanceSpace[q][i].Status != epaxosproto.COMMITTED {
				time.Sleep(1000 * 1000)
			}

			// Works because we're only batching 1 command at a time
			// Idea: Skip through the log entries that don't conflict with the command
			//       we're trying to execute. Still have to wait on edges that conflict
			//       that cascade down the chain.
			if !state.CONFLICT_FUNC(&v.Cmds[0], &e.r.InstanceSpace[q][i].Cmds[0]) {
				continue
			}

			w := e.r.InstanceSpace[q][i]
			if w.Index == 0 {
				if !e.strongconnect(w, index) {
					for j := l; j < len(stack); j++ {
						stack[j].Index = 0
					}
					stack = stack[0:l]
					return false
				}
				if w.Lowlink < v.Lowlink {
					v.Lowlink = w.Lowlink
				}
			} else {
				if w.Index < v.Lowlink {
					v.Lowlink = w.Index
				}
			}
		}
	}

	if v.Lowlink == v.Index {
		//found SCC
		list := stack[l:len(stack)]

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for w.Cmds == nil {
				time.Sleep(1000 * 1000)
			}
			for idx := 0; idx < len(w.Cmds); idx++ {
				val := w.Cmds[idx].Execute(e.r.State)
				if w.lb != nil && w.lb.clientProposals != nil {
					var delt int64 = 0;//time.Now().UnixNano() - e.r.startTimes[w.lb.clientProposals[idx].CommandId]
					//log.Printf("Executed command %d in %fms\n", w.lb.clientProposals[idx].CommandId, float64(delt) / 1000000.0)
					if e.r.Dreply {
						e.r.ReplyProposeTS(
							&genericsmrproto.ProposeReplyTS{
								TRUE,
								w.lb.clientProposals[idx].CommandId,
								val,
								delt},
							w.lb.clientProposals[idx].Reply)
					}
				}
			}
			w.Status = epaxosproto.EXECUTED
		}
		stack = stack[0:l]
	}
	return true
}

func (e *Exec) inStack(w *Instance) bool {
	for _, u := range stack {
		if w == u {
			return true
		}
	}
	return false
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].Seq < na[j].Seq
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}
