package kvraft

const (
	OK = "OK"
	// ErrNoKey       = "ErrNoKey"

	ErrWrongLeader = "ErrWrongLeader"

	ErrTimeout = "ErrTimeout"
)

type Err string

type OpType int

const (
	OP_GET OpType = iota
	OP_PUT
	OP_APPEND
)

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	ClientKey string // half part of Idempotent Key using for duplicate detection
	RequestId int64  // half part of Idempotent Key using for duplicate detection
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientKey string // half part of Idempotent Key using for duplicate detection
	RequestId int64  // half part of Idempotent Key using for duplicate detection
}

type GetReply struct {
	Err   Err
	Value string
}
