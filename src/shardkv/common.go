package shardkv

// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type OpType int

// all events that may occur in a replica group(raft cluster)
// also all types of log-entries appended into raft log by leader node
const (
	OP_GET        OpType = iota // Event: leader node got GET request sended by client
	OP_PUT                      // Event: leader node got PUT request sended by client
	OP_APPEND                   // Event: leader node got APPEND request sended by client
	OP_CONF_SHARD               // Event: leader node detected sharding configuration changed, may transmit shard
	OP_RECV_SHARD               // Event: leader node received new shard from other peer replica group(raft cluster)

	OP_SNAPSHOT
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientKey string // half part of Idempotent Key using for duplicate detection
	RequestId int64  // half part of Idempotent Key using for duplicate detection
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	ClientKey string // half part of Idempotent Key using for duplicate detection
	RequestId int64  // half part of Idempotent Key using for duplicate detection
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	SID     int                   // shard's id
	From    string                // gid that sending the shard
	Version int                   // configuration's version/num
	Shard   map[string]string     // shard's content
	Cstate  map[string]Clerkstate // clerk's state about this shard
}

type GetShardReply struct {
	Err Err
}

type DelShardArgs struct {
	To      int // gid of replica group which the shard has been sent to
	Version int // config version number
	Shard   int // shard id
}

type Result struct {
	Out string
	Err string
}

type ReConfigLog struct {
	Shard   int // shard's id
	To      int // gid that receive the shard
	ToSvrs  []string
	Version int // configuration's version/num
}
