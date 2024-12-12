package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	Typ       OpType
	Key       string
	Value     string
	ClientKey string
	RequestId int64

	NewShard GetShardArgs

	ReConfig ReConfigLog
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	dead         int32 // set by Kill()
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int                // replica group id
	ctrlerClient *shardctrler.Clerk // clerk of shard-controler

	pending   map[string]chan Result // Mux-Dmux queue
	commitIdx int

	/* state machine of each shard */
	sh2sm [shardctrler.NShards]*stateMachine
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := &Op{
		Typ:       OP_GET,
		Key:       args.Key,
		ClientKey: args.ClientKey,
		RequestId: args.RequestId,
	}

	ret := kv.executeOp(op)
	reply.Value = ret.Out
	reply.Err = Err(ret.Err)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	typ := OP_PUT
	if args.Op == "Append" {
		typ = OP_APPEND
	}
	op := &Op{
		Typ:       typ,
		Key:       args.Key,
		Value:     args.Value,
		ClientKey: args.ClientKey,
		RequestId: args.RequestId,
	}

	ret := kv.executeOp(op)
	reply.Err = Err(ret.Err)
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	/* repulse request with old version shard, if add this kind of request into raft layer, may cause deadlock
	 * StateMachine(gid=101,shard=6,version=3)
	 * Raft Log: .. [OP_CONF_SHARD] ... [OP_RECV_SHARD]
	 *					    \______________    |
	 *				         ______________\__/
	 *				       /                \
	 * Raft Log:.. [OP_CONF_SHARD] ... [OP_RECV_SHARD]
	 * StateMachine(gid=102,shard=6,version=6)
	 *
	 * sm101 will send shard6-3 to sm102, at the same time sm102 send shard6-6 to sm101
	 * if two state machines append event OP_RECV_SHARD to their raft log's end, then OP_RECV_SHARD
	 * must wait util precede OP_CONF_SHARD task finishs transmitting shard, deadlock.
	 *
	 * if request's version is older than the current version of state machine, no need append log,
	 * the shard in request must not be the waiting shard of state machine.
	 * if request's version is bigger, it's possible that state machine's version reach that version,
	 * because there could be some un-executed operations in the log end.
	 */
	if args.Version < kv.sh2sm[args.SID].getShardVersion() {
		reply.Err = OK
		return
	}

	op := &Op{
		Typ:       OP_RECV_SHARD,
		NewShard:  *args,
		ClientKey: fmt.Sprintf("gid%v", args.From), // for idempotent
		RequestId: int64(args.Version),             // for idempotent
	}

	ret := kv.executeOp(op)
	reply.Err = Err(ret.Err)

	return
}

func (kv *ShardKV) sendGetShard(svrname string, args *GetShardArgs, reply *GetShardReply) (ok bool) {
	server := kv.make_end(svrname)
	ok = server.Call("ShardKV.GetShard", args, reply)
	return
}

// append log and wait raft-agreement
// if code=0, request is ok and done
// if code=1, current server does not hold leader raft node
// if code=2, getting agreement from raft nodes is timeout
func (kv *ShardKV) executeOp(op *Op) (ret Result) {
	isLeader, done := kv.appendLog(op)
	if !isLeader {
		ret.Err = ErrWrongLeader
		return
	}

	select {
	case ret = <-done:
		// nothing
	case <-time.After(150 * time.Millisecond):
		// cancel wait
		kv.mu.Lock()
		tag := muxIdentifier(op.ClientKey, op.RequestId)
		kv.pending[tag] = nil
		kv.mu.Unlock()
	}
	return
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) appendLog(op *Op) (isLeader bool, done chan Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// send op request as log entry into raft
	_, _, isLeader = kv.rf.Start(*op)
	if !isLeader {
		return
	}

	// update Dmux/Dispatch rules
	done = make(chan Result, 1)
	tag := muxIdentifier(op.ClientKey, op.RequestId)
	kv.pending[tag] = done

	return
}

// dispatch execution result of log-entry/event to RPC handler waiting for the result
func (kv *ShardKV) dispatch(op *Op, ret Result) {
	// Dmux by unique mux identifier/tag
	tag := muxIdentifier(op.ClientKey, op.RequestId)

	kv.mu.Lock()
	done := kv.pending[tag]
	delete(kv.pending, tag)
	kv.mu.Unlock()

	if done == nil {
		return
	}

	done <- ret
}

func (kv *ShardKV) applyLogTicker() {
	for !kv.killed() {
		for msg := range kv.applyCh {
			if msg.CommandValid {
				op, ok := msg.Command.(Op)
				if !ok {
					panic("applyLogTicker: unknown type of Command ApplyMsg")
				}
				shard := -1
				if op.Typ == OP_CONF_SHARD {
					shard = op.ReConfig.Shard
				} else if op.Typ == OP_RECV_SHARD {
					shard = op.NewShard.SID
				} else {
					shard = key2shard(op.Key)
				}
				// divide msg into message queue of each shard
				kv.sh2sm[shard].submitEntry(createSMOP(deepCopyOp(&op), nil))
				// update server status
				kv.mu.Lock()
				kv.commitIdx = msg.CommandIndex
				kv.mu.Unlock()

			} else if msg.SnapshotValid {
				// dispatch snapshot to corressponding state machine
				kv.dispatchSnapshot(msg.Snapshot)

			} else {
				panic("ShardKV applyLogTicker error")
			}
		}
	}
}

// poll latest sharded configurations from Shard-Controler cluster
func (kv *ShardKV) leaderPollConfigTicker() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for shard, sm := range kv.sh2sm {
			ss, _, version := sm.getShardStatus()
			/* query next version configuration from configuration controler rather than latest version!
			 * if pull latest config periodically, puller may missing some config with no latest version,
			 * then it will skip some synchronized operation with other peer nodes.
			 * In short, each nodes may see different configuration and different config-changes
			 */
			newCfg := kv.ctrlerClient.Query(version + 1)
			// determine if configuration changed
			if version >= newCfg.Num {
				continue
			}
			DPrintf("%p Event:(gid=%v,shd=%v,ver=%v,ss=%v); next(ver=%v,owner=%v)\n", sm, kv.gid, shard, version, ss, newCfg.Num, newCfg.Shards[shard])
			// generate event
			kv.rf.Start(Op{
				Typ: OP_CONF_SHARD,
				ReConfig: ReConfigLog{
					Shard:   shard,
					To:      newCfg.Shards[shard],
					ToSvrs:  newCfg.Groups[newCfg.Shards[shard]],
					Version: newCfg.Num,
				},
			})
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// create snapshot periodically if log size grows up to maxraftstate
func (kv *ShardKV) snapshotTicker(persister *raft.Persister, maxraftstate int) {
	for !kv.killed() {
		if 0 < maxraftstate && maxraftstate <= persister.RaftStateSize() {
			index, snap := kv.snapshot()
			if index > 0 {
				kv.rf.Snapshot(index, snap)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// create a snapshot
func (kv *ShardKV) snapshot() (int, []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	arr := [shardctrler.NShards][]byte{}
	for shard, sm := range kv.sh2sm {
		arr[shard] = sm.snapshot()
	}

	e.Encode(arr)
	e.Encode(kv.commitIdx)

	return kv.commitIdx, w.Bytes()
}

// dispatch snapshot to state machine
func (kv *ShardKV) dispatchSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var arr [shardctrler.NShards][]byte
	var commitIdx int

	if d.Decode(&arr) != nil ||
		d.Decode(&commitIdx) != nil {
		panic("dispatchSnapshot Decode error")
	} else {
		kv.mu.Lock()
		if kv.commitIdx < commitIdx {
			for shard := range kv.sh2sm {
				if kv.sh2sm[shard] == nil {
					panic("ShardKV nil dispatchSnapshot")
				}
				kv.sh2sm[shard].submitEntry(createSMOP(&Op{Typ: OP_SNAPSHOT}, arr[shard]))
			}
			kv.commitIdx = commitIdx
		} else if kv.commitIdx > commitIdx {
			// occur when this server's raft-node lag behind leader raft-node too much
		}
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.make_end = make_end
	kv.gid = gid

	// init clerk of Shard-Controler and query latest sharded configuration
	kv.ctrlerClient = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.pending = make(map[string]chan Result)

	// create new kv server
	for shard := range kv.sh2sm {
		kv.sh2sm[shard] = createStateMachine(kv.gid, shard, kv.dispatch, kv.sendGetShard)
	}
	// restore state machine
	kv.dispatchSnapshot(persister.ReadSnapshot())

	// comsume message queue applyCh
	go kv.applyLogTicker()

	if maxraftstate != -1 {
		// create snapshot periodically
		go kv.snapshotTicker(persister, maxraftstate)
	}

	// query latest sharded configuration periodically
	go kv.leaderPollConfigTicker()

	return kv
}

// get muxing identifier, using to Mux/Dmux
func muxIdentifier(clerkId string, reqId int64) string {
	return fmt.Sprintf("%s:%v", clerkId, reqId)
}
