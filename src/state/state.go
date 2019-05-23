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
	INCREMENT
	READ
	FAST_READ
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
type ExecuteFunction func(gamma *Command, st *State) (Value)

type Command struct {
	Op Operation
	K Key
	V Value
}

// Set these to the functions you want to run
var CONFLICT_FUNC ConflictFunction = DefaultConflict
var EXECUTE_FUNC ExecuteFunction = DefaultExecute

func ConflictBatch(batch1 []Command, batch2 []Command) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			if CONFLICT_FUNC(&batch1[i], &batch2[j]) {
				return true
			}
		}
	}
	return false
}

func (c *Command) Execute(st *State) Value {
	return EXECUTE_FUNC(c, st)
}

func DefaultConflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		if gamma.Op == PUT || delta.Op == PUT {
			return true
		}
	}
	return false
}

func DefaultExecute(gamma *Command, st *State) Value {
	switch gamma.Op {
		case PUT:
			st.Store[gamma.K] = gamma.V
			return gamma.V

		case GET:
			if val, present := st.Store[gamma.K]; present {
				return val
			}

		default:
			break
	}

	return NIL
}

// Conflicts only on strong reads (GETs) on the same key
// All other operations commute
func InventoryConflict(gamma Command, delta Command) bool {
	if gamma.K == delta.K {
		if gamma.Op == READ || delta.Op == READ {
			return true
		}
	}

	return false
}

func InventoryExecute(gamma Command, st State) Value {
	switch gamma.Op {
		case INCREMENT:
			if val, present := st.Store[gamma.K]; present {
				st.Store[gamma.K] = gamma.V + val
			} else {
				st.Store[gamma.K] = gamma.V
			}

			return st.Store[gamma.K]

		case READ:
		case FAST_READ:
			if val, present := st.Store[gamma.K]; present {
				return val
			}

		default:
			break
	}

	return NIL
}