package state

import (
	//"fmt"
	//"code.google.com/p/leveldb-go/leveldb"
	//"encoding/binary"
)

type Operation uint8

const (
	NONE Operation = iota
	PUT
	GET
	FAST_GET
)

type Key int64
type Value int64
const NIL Value = 0

type State struct {
	Store map[Key]Value
}

func InitState() *State {
	return &State{make(map[Key]Value)}
}

type ConflictFunction func(gamma *Command, delta *Command) (bool)

type Command struct {
	Op Operation
	K Key
	V Value
	ConflictF ConflictFunction
}

var CONFLICT_FUNC ConflictFunction = Conflict

func Conflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		if gamma.Op == PUT || delta.Op == PUT {
			return true
		}
	}
	return false
}

// Conflicts only on strong reads (GETs) on the same key
// All other operations commute
func InventoryConflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		if gamma.Op == GET || delta.Op == GET {
			return true
		}
	}

	return false
}

func ConflictBatch(batch1 []Command, batch2 []Command) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			batch1[i].ConflictF = CONFLICT_FUNC
			if batch1[i].ConflictF(&batch1[i], &batch2[j]) {
				return true
			}
		}
	}
	return false
}

func (c *Command) Execute(st *State) Value {
	switch c.Op {
		case PUT:
			st.Store[c.K] = c.V
			return c.V

		case GET:
		case FAST_GET:
			if val, present := st.Store[c.K]; present {
				return val
			}

		default:
			break
	}

	return NIL
}