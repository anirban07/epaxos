package state

import (
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
	LIKE
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
const OK Value = 1

type State struct {
	Store map[Key]Value
	FBStore map[Key]map[Value]bool
}

var CONFLICT_FUNC ConflictFunction
var EXECUTE_FUNC ExecuteFunction
var OP_CONFLICT_FUNC OperationConflict

func InitState(app Application) *State {
	switch app {
		case DEFAULT:
			log.Printf("Using Default Application")
			CONFLICT_FUNC = DefaultConflict
			OP_CONFLICT_FUNC = DefaultOperationConflict
			EXECUTE_FUNC = DefaultExecute
			break
		case INVENTORY:
			log.Printf("Using Inventory Application")
			CONFLICT_FUNC = DefaultConflict
			OP_CONFLICT_FUNC = DefaultOperationConflict
			EXECUTE_FUNC = InventoryExecute
			break
		case FACEBOOK:
			log.Printf("Using Facebook Application")
			CONFLICT_FUNC = DefaultConflict
			OP_CONFLICT_FUNC = DefaultOperationConflict
			EXECUTE_FUNC = FacebookExecute
			break
	}

	return &State{make(map[Key]Value), make(map[Key]map[Value]bool)}
}

type ConflictFunction func(gamma *Command, delta *Command) bool
type ExecuteFunction func(gamma *Command, st *State) Value
type OperationConflict func(op1 Operation, op2 Operation) bool
type Command struct {
	CommandId int32
	Op Operation
	K  Key
	V  Value
}

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

func DefaultOperationConflict(op1 Operation, op2 Operation) bool {
	return op1 == PUT || op2 == PUT
}

func DefaultConflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		return DefaultOperationConflict(gamma.Op, delta.Op)
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
func InventoryOperationConflict(op1 Operation, op2 Operation) bool {
	return (op1 == READ && op2 == INCREMENT) || (op1 == INCREMENT && op2 == READ)
}

func InventoryConflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		return InventoryOperationConflict(gamma.Op, delta.Op)
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

func FacebookOperationConflict(op1 Operation, op2 Operation) bool {
	return (op1 == LIKE && op2 == READ) || (op1 == READ && op2 == LIKE)
}

func FacebookConflict(gamma *Command, delta *Command) bool {
	if gamma.K == delta.K {
		return FacebookOperationConflict(gamma.Op, delta.Op)
	}

	return false
}

func FacebookExecute(gamma *Command, st *State) Value {
	switch gamma.Op {
		case LIKE:
			if innermap, present := st.FBStore[gamma.K]; present {
				if _, present := innermap[gamma.V]; !present {
					st.FBStore[gamma.K][gamma.V] = true
				}
			} else {
				st.FBStore[gamma.K] = make(map[Value]bool)
				st.FBStore[gamma.K][gamma.V] = true
			}

			return OK;
		case FAST_READ:
		case READ:
			if innermap, present := st.FBStore[gamma.K]; present {
				return Value(len(innermap))
			}

		default:
			break
	}

	return NIL
}
