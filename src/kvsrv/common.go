package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key        string
	Value      string
	ClientKey  string // half part of Idempotent Key using for duplicate detection
	RequestKey int64  // half part of Idempotent Key using for duplicate detection
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
