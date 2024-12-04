package shardctrler

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	dead    int32 // set by Kill()
	applyCh chan raft.ApplyMsg

	pending map[string]chan Config

	sm *statemachine
}

type statemachine struct {
	mu      sync.Mutex
	configs []Config              // indexed by config num
	cstates map[string]Clerkstate // clerk's state that is used to detect consecutive duplicate request

	commitIdx int // index of last applied log entry
}

// clerk's state saved in kv server
type Clerkstate struct {
	LastReqId int
	Result    *Config
}

type Op struct {
	Typ     OpType
	Servers map[int][]string // JoinArgs (need Deep-Copy)
	GIDs    []int            // LeaveArgs(need Deep-Copy)
	Shard   int              // MoveArgs
	GID     int              // MoveArgs
	Num     int              // QueryArgs

	ClerkKey  string
	RequestId int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := &Op{
		Typ:       OP_JOIN,
		Servers:   deepCopyMap(args.Servers),
		ClerkKey:  args.ClerkKey,
		RequestId: args.RequestId,
	}

	isLeader, isOk, _ := sc.executeOp(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	if isOk {
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := &Op{
		Typ:       OP_LEAVE,
		GIDs:      deepCopyIntArr(args.GIDs),
		ClerkKey:  args.ClerkKey,
		RequestId: args.RequestId,
	}

	isLeader, isOk, _ := sc.executeOp(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	if isOk {
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := &Op{
		Typ:       OP_MOVE,
		Shard:     args.Shard,
		GID:       args.GID,
		ClerkKey:  args.ClerkKey,
		RequestId: args.RequestId,
	}

	isLeader, isOk, _ := sc.executeOp(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	if isOk {
		reply.Err = OK
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := &Op{
		Typ:       OP_QUERY,
		Num:       args.Num,
		ClerkKey:  args.ClerkKey,
		RequestId: args.RequestId,
	}

	isLeader, isOk, cfg := sc.executeOp(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	if isOk {
		reply.Err = OK
		reply.Config = cfg
	}
}

// append log and wait raft-agreement
func (sc *ShardCtrler) executeOp(op *Op) (isLeader, isOk bool, cfg Config) {
	isLeader, done := sc.appendLog(op)
	if !isLeader {
		return
	}

	select {
	case cfg = <-done:
		isOk = true
	case <-time.After(100 * time.Millisecond):
		// cancel wait
		sc.mu.Lock()
		tag := getMuxTag(op.ClerkKey, op.RequestId)
		sc.pending[tag] = nil
		sc.mu.Unlock()
	}
	return
}

// handler append log into raft
func (sc *ShardCtrler) appendLog(op *Op) (isLeader bool, done chan Config) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	_, _, isLeader = sc.rf.Start(*op)
	if !isLeader {
		return
	}

	tag := getMuxTag(op.ClerkKey, op.RequestId)
	done = make(chan Config, 1)
	sc.pending[tag] = done

	return
}

func (sc *ShardCtrler) dispatchOp(op *Op, cfg *Config) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	tag := getMuxTag(op.ClerkKey, op.RequestId)
	done := sc.pending[tag]

	if done == nil {
		return
	}

	if cfg == nil {
		done <- Config{}
	} else {
		done <- *cfg
	}
}

func (sc *ShardCtrler) applyLog() {
	for !sc.killed() {
		for msg := range sc.applyCh {
			if msg.CommandValid {
				// consume log entry from raft
				op, ok := msg.Command.(Op)
				if !ok {
					panic("applyLog error: interface assertion")
				}
				cfg := sc.sm.executeLog(&op, msg.CommandIndex)
				sc.dispatchOp(&op, cfg)
			} else if msg.SnapshotValid {
				// consume snapshot
				sc.sm.applySnap(msg.Snapshot)
			} else {
				panic("applyLog error msg")
			}
		}
	}
}

func (sc *ShardCtrler) snapshotTicker(persister *raft.Persister) {
	for !sc.killed() {
		if 1000 <= persister.RaftStateSize() {
			index, snap := sc.sm.snapshot()
			if index > 0 {
				sc.rf.Snapshot(index, snap)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	cfgs := make([]Config, 1)
	cfgs[0].Groups = map[int][]string{}

	sc.sm = &statemachine{
		configs: cfgs,
		cstates: make(map[string]Clerkstate),
	}

	sc.sm.applySnap(persister.ReadSnapshot())

	sc.pending = make(map[string]chan Config)

	go sc.applyLog()

	go sc.snapshotTicker(persister)

	return sc
}

// re-balance the number of shards on each replica group cluster
// must hold the lock sm.mu before calling
// requirement: addGids not intersect with delGids; gid > 0
//
// must be deterministic, to keep the result the same as the result of all other peers
// In Go, map iteration order is not deterministic.
func (sm *statemachine) rebalance(addGids, delGids []int) (ret [NShards]int) {
	oldCfg := sm.configs[len(sm.configs)-1]
	newCnt := len(oldCfg.Groups) + len(addGids) - len(delGids) // replica group cluster number
	if newCnt == 0 {
		return
	}
	x := NShards / newCnt   // average number of shards in each replica group cluster
	y := NShards - x*newCnt // the number of replica group cluster that can hold x+1 shards

	delMap := getMapByArr(delGids) // deleted gid
	moveShards := []int{}          // shards need movement
	gid2shds := map[int][]int{}    // old gid -> shards
	for gid := range oldCfg.Groups {
		gid2shds[gid] = nil
	}
	for shd, gid := range oldCfg.Shards {
		if gid == 0 || delMap[gid] {
			moveShards = append(moveShards, shd)
			delete(gid2shds, gid)
		} else {
			gid2shds[gid] = append(gid2shds[gid], shd)
		}
	}

	moreShdGids := []int{} // gid that must transmit shards
	lessShdGids := addGids // gid that maybe receive shards
	for gid, shds := range gid2shds {
		if len(shds) > x {
			moreShdGids = append(moreShdGids, gid)
		} else {
			lessShdGids = append(lessShdGids, gid)
		}
	}
	sort.Slice(moreShdGids, func(i, j int) bool {
		return moreShdGids[i] < moreShdGids[j]
	})
	sort.Slice(lessShdGids, func(i, j int) bool {
		return lessShdGids[i] < lessShdGids[j]
	})

	// get must-move shards
	newGid2shds := map[int][]int{}
	for _, gid := range moreShdGids {
		shds := gid2shds[gid]
		if len(shds) > x+1 {
			if y > 0 {
				newGid2shds[gid] = shds[len(shds)-x-1:]
				moveShards = append(moveShards, shds[:len(shds)-x-1]...)
				y--
			} else {
				newGid2shds[gid] = shds[len(shds)-x:]
				moveShards = append(moveShards, shds[:len(shds)-x]...)
			}
		} else if len(shds) == x+1 {
			if y > 0 {
				newGid2shds[gid] = shds
				y--
			} else {
				newGid2shds[gid] = shds[1:]
				moveShards = append(moveShards, shds[0])
			}
		} else {
			panic("gid2moreShds error")
		}
	}

	// re-assign must-move shards
	for _, gid := range lessShdGids {
		shds := gid2shds[gid]
		newGid2shds[gid] = shds

		if len(moveShards) == 0 {
			continue
		}
		var less int
		if y > 0 {
			less = x + 1 - len(shds)
			y--
		} else {
			less = x - len(shds)
		}
		// assert less <= len(moveShards)
		newGid2shds[gid] = append(newGid2shds[gid], moveShards[:less]...)
		moveShards = moveShards[less:]
	}

	for gid, shds := range newGid2shds {
		if len(shds) == 0 {
			continue
		}
		ret = mapshard(ret, shds, gid)
	}

	return
}

func mapshard(shards [NShards]int, arr []int, gid int) [NShards]int {
	for _, shd := range arr {
		shards[shd] = gid
	}
	return shards
}

// state machine execute log entry
func (sm *statemachine) executeLog(op *Op, idx int) (ret *Config) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// detect consecutive duplicate request
	cstate := sm.cstates[op.ClerkKey]
	if cstate.LastReqId == op.RequestId {
		if op.Typ == OP_QUERY {
			return cstate.Result
		}
		return nil
	}

	sm.commitIdx = idx

	// execute log operation
	preCfg := sm.configs[len(sm.configs)-1]
	switch op.Typ {
	case OP_JOIN:
		if len(op.Servers) == 0 {
			break
		}
		groups := deepCopyMap(preCfg.Groups)
		gids := make([]int, 0)
		for gid, servers := range op.Servers {
			if _, ok := groups[gid]; !ok {
				gids = append(gids, gid) // new replica group id
				groups[gid] = servers
			}
		}

		cfg := Config{
			Num:    preCfg.Num + 1,
			Shards: sm.rebalance(gids, nil),
			Groups: groups,
		}
		sm.configs = append(sm.configs, cfg)

	case OP_LEAVE:
		if len(op.GIDs) == 0 {
			break
		}
		groups := deepCopyMap(preCfg.Groups)
		for _, gid := range op.GIDs {
			delete(groups, gid)
		}
		cfg := Config{
			Num:    preCfg.Num + 1,
			Shards: sm.rebalance(nil, op.GIDs),
			Groups: groups,
		}
		sm.configs = append(sm.configs, cfg)

	case OP_MOVE:
		shards := preCfg.Shards
		shards[op.Shard] = op.GID
		cfg := Config{
			Num:    preCfg.Num + 1,
			Shards: shards,
			Groups: deepCopyMap(preCfg.Groups),
		}
		sm.configs = append(sm.configs, cfg)

	case OP_QUERY:
		idx := op.Num
		if idx <= -1 || len(sm.configs) <= idx {
			idx = len(sm.configs) - 1
		}
		ret = &sm.configs[idx]

	default:
		panic("executeLog error")
	}

	sm.cstates[op.ClerkKey] = Clerkstate{
		LastReqId: op.RequestId,
		Result:    ret,
	}
	return
}

// create a snapshot for state machine
func (sm *statemachine) snapshot() (int, []byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.cstates)
	e.Encode(sm.commitIdx)

	return sm.commitIdx, w.Bytes()
}

// restore a snapshot for state machine
func (sm *statemachine) applySnap(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var configs []Config
	var cstates map[string]Clerkstate
	var commitIdx int

	if d.Decode(&configs) != nil || d.Decode(&cstates) != nil ||
		d.Decode(&commitIdx) != nil {
		panic("applySnap Decode error")
	} else {
		sm.mu.Lock()
		if sm.commitIdx < commitIdx {
			sm.configs = configs
			sm.cstates = cstates
			sm.commitIdx = commitIdx
		}
		sm.mu.Unlock()
	}
}
