package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const TIMEOUT_PERIOD = 200 // millisecond

type Op struct {
	Typ       OpType
	Key       string
	Value     string
	ClientKey string
	RequestId int64
}

type KVServer struct {
	mu   sync.Mutex
	me   int
	rf   *raft.Raft
	dead int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	pending map[string]chan string

	/* state machine */
	sm *stateMachine
}

// k/v state machine
type stateMachine struct {
	// mutex         sync.Mutex // no concurrency on cache and idempotentMap
	cache         map[string]string     // state for storing key-value pairs
	idempotentMap map[string]clerkstate // state for detecting consecutive duplicated requests
}

// clerk's state saved in kv server
type clerkstate struct {
	/* lastReqId represents last request's id from the clerk
	 * using to guarantee limited Idenpotence in the case that clerk sends request one by one
	 * consecutive duplicated request will be idempotent
	 */
	lastReqId int64
	result    string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Typ:       OP_GET,
		Key:       args.Key,
		ClientKey: args.ClientKey,
		RequestId: args.RequestId,
	}
	// send log entry to raft
	isLeader, done, cancel := kv.addLogEntry(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for getting majority agreement
	select {
	case ret := <-done:
		reply.Err = OK
		reply.Value = ret
	case <-time.After(TIMEOUT_PERIOD * time.Millisecond): // timeout
		cancel()
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Typ:       OP_PUT,
		Key:       args.Key,
		Value:     args.Value,
		ClientKey: args.ClientKey,
		RequestId: args.RequestId,
	}

	// send log entry to raft
	isLeader, done, cancel := kv.addLogEntry(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for getting majority agreement
	select {
	case <-done:
		reply.Err = OK
	case <-time.After(TIMEOUT_PERIOD * time.Millisecond): // timeout
		cancel()
		reply.Err = ErrTimeout
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Typ:       OP_APPEND,
		Key:       args.Key,
		Value:     args.Value,
		ClientKey: args.ClientKey,
		RequestId: args.RequestId,
	}
	// send log entry to raft
	isLeader, done, cancel := kv.addLogEntry(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for getting majority agreement
	select {
	case <-done:
		reply.Err = OK
	case <-time.After(TIMEOUT_PERIOD * time.Millisecond): // timeout
		cancel()
		reply.Err = ErrTimeout
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// Mux:
// add all kinds of commands into Multiplexer(raft log + kv.pending)
func (kv *KVServer) addLogEntry(op Op) (isLeader bool, done chan string, cancel func()) {

	/* Must lock "rf.Start()" and "kv.pending[tag]=done" in the same critical regeion
	 * to guarantee atomicity of this two operations!!!
	 * If not, Dmux will dispatch results after "rf.Start()", then will see partial
	 * state about Mux queue
	 *
	 * be careful about deadlock, because of acquiring 2 locks
	 */
	kv.mu.Lock()
	// send op request as log entry into raft
	_, _, isLeader = kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return
	}

	// kv.mu.Lock()
	// update Dmux/Dispatch rules
	done = make(chan string, 1)
	tag := muxIdentifier(op.ClientKey, op.RequestId)
	kv.pending[tag] = done

	kv.mu.Unlock()

	cancel = func() {
		/* remove the reference to the done channel
		 * let it can be recycling by GC
		 */
		kv.mu.Lock()
		kv.pending[tag] = nil
		kv.mu.Unlock()
	}

	return
}

// D-Mux:
// - 1.dispatch results from raft layer to RPC handler by looping applyCh(MQ)
// - 2.execute commands in log entries had committed by raft
// - 3.if no RPC handler waits for the output, then only execute but not dispatch
func (kv *KVServer) consumeTicker(applyCh chan raft.ApplyMsg) {
	for !kv.killed() {

		for msg := range applyCh {
			if msg.CommandValid {
				// acquire command
				op, ok := msg.Command.(Op)
				if !ok {
					panic("unknown type of Command ApplyMsg")
				}
				// execute command
				ret := kv.sm.exeCmd(op)
				// dispatch result
				kv.dispatch(op, ret)

			} else if msg.SnapshotValid {

			} else {
				panic("unknown type of ApplyMsg")
			}
		}
	}
}

func (kv *KVServer) dispatch(op Op, out string) {
	tag := muxIdentifier(op.ClientKey, op.RequestId)

	kv.mu.Lock()
	done := kv.pending[tag]
	delete(kv.pending, tag)
	kv.mu.Unlock()

	if done == nil {
		/* 1.rpc handler cancel waiting this output event
		 * 2.current kvserver not leader, not serve for clerk
		 */
		return
	}

	done <- out // send result into buffered channel instead of non-buffer channel
}

// send the command op to state machine to execute.
// consecutive duplicated OP_GET command: subsequent req will be returned prevoius result,
// but other type commands will be discarded
func (sm *stateMachine) exeCmd(op Op) (ret string) {

	// limited idempotent checking
	// detecting consecutive duplicated request, but discontinuous, can't
	cs := sm.idempotentMap[op.ClientKey]
	if cs.lastReqId == op.RequestId {
		if op.Typ == OP_GET {
			ret = cs.result
			return
		} else {
			return // consecutive duplicate request
		}
	}

	// run command
	switch op.Typ {
	case OP_GET:
		ret = sm.cache[op.Key]
	case OP_PUT:
		sm.cache[op.Key] = op.Value
	case OP_APPEND:
		sm.cache[op.Key] += op.Value
	default:
		panic("unknown command type")
	}

	// maintain state of clerk
	sm.idempotentMap[op.ClientKey] = clerkstate{
		lastReqId: op.RequestId,
		result:    ret,
	}
	return
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	applyCh := make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, applyCh)

	kv.pending = make(map[string]chan string)

	// create state machine
	kv.sm = &stateMachine{
		cache:         make(map[string]string),
		idempotentMap: make(map[string]clerkstate),
	}

	// comsume message queue applyCh
	go kv.consumeTicker(applyCh)

	return kv
}

// get muxing identifier, using to Mux/Dmux
func muxIdentifier(clerkId string, reqId int64) string {
	return fmt.Sprintf("%s:%v", clerkId, reqId)
}
