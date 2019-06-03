package state

//"fmt"
//"code.google.com/p/leveldb-go/leveldb"
//"encoding/binary"

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

// TODO: Make function pointer part of the struct
type State struct {
	Store map[Key]Value
}

func InitState() *State {
	return &State{make(map[Key]Value)}
}

type ConflictFunction func(gamma *Command, delta *Command) bool
type ExecuteFunction func(gamma *Command, st *State) Value
type OperationConflict func(op1 Operation, op2 Operation) bool
type Command struct {
	Op Operation
	K  Key
	V  Value
}

// Set these to the functions you want to run
var CONFLICT_FUNC ConflictFunction = DefaultConflict
var EXECUTE_FUNC ExecuteFunction = InventoryExecute
var OP_CONFLICT_FUNC OperationConflict = InventoryOperationConflict

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
	return op1 == INCREMENT || op2 == INCREMENT
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
