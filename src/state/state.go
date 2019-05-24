package state

import (
	//"fmt"
	//"code.google.com/p/leveldb-go/leveldb"
	//"encoding/binary"
	"log"
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

type Application uint8
const (
	DEFAULT Application = iota
	INVENTORY
	FACEBOOK
)

type Key int64
type Value int64
const NIL Value = 0

type State struct {
	Store map[Key]Value
	FBStore map[Key][]Value
}

var CONFLICT_FUNC ConflictFunction
var EXECUTE_FUNC ExecuteFunction

func InitState(app Application) *State {
	switch app {
		case DEFAULT:
			log.Printf("Using Default Application")
			CONFLICT_FUNC = DefaultConflict
			EXECUTE_FUNC = DefaultExecute
			break
		case INVENTORY:
			log.Printf("Using Inventory Application")
			CONFLICT_FUNC = DefaultConflict
			EXECUTE_FUNC = InventoryExecute
			break
		case FACEBOOK:
			log.Printf("Using Facebook Application")
			break
	}

	return &State{make(map[Key]Value), make(map[Key][]Value)}
}

type ConflictFunction func(gamma *Command, delta *Command) (bool)
type ExecuteFunction func(gamma *Command, st *State) (Value)
type Command struct {
	Op Operation
	K Key
	V Value
}

// Set these to the functions you want to run
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
		if gamma.Op == INCREMENT || delta.Op == INCREMENT {
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

// Conflicts only when a read and an increment are occuring on the same
// key
func InventoryConflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		if (gamma.Op == READ && delta.Op == INCREMENT)  || 
			 (gamma.Op == INCREMENT && delta.Op == READ) {
			return true
		}
	}

	return false
}

func InventoryExecute(gamma *Command, st *State) Value {
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