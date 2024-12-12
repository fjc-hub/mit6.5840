package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"6.5840/labgob"
)

/* state machine for a SHARD.
 * 1.for keeping all state machine in different nodes the same,
 *   the result must be DETERMINISTIC while appling agreed log on it.
 * 2.all kinds of status updations in state machine must submit raft to get agreement.
 */
type stateMachine struct {
	mutex          sync.Mutex
	caches         map[string]string     // state for storing key-value pairs
	idempotentMap  map[string]Clerkstate // state for detecting consecutive duplicated requests
	version        int                   // sharding config's version
	shardStatus    ShardStatus
	moveTarget     int // gid that the shard need move to, valid if shardStatus == SS_SAVED
	moveTargetSvrs []string

	mq *MQ // message queue that saved unfinished operation log entries. guarantee Exactly-Once

	transCond *sync.Cond // condition variable: wait for the event that state machine need transmit shard
	finisCond *sync.Cond // condition variable: wait for the event that state machine finished shard-transmit

	/* immutable */
	gid, shard int
	// rpc GetShard
	sendGetShard func(svrname string, args *GetShardArgs, reply *GetShardReply) bool
	// send result of log entry to rpc handler, always kv.dispatch()
	dispatch func(op *Op, ret Result)
}

// shard state machine status
type ShardStatus int

const (
	SS_UNOWN   = iota // this shard is not responsible by state machine (shard unsaved, unowned)
	SS_WAITING        // this shard need move in the state machine before serving for clerks (shard unsaved, owned)

	SS_SAVED // this shard is transmitting to other (shard saved, unowned)
	SS_OWNED // this shard can be manipulate by clerks (shard saved, owned)
)

// shard state machine's operation log
type SMOP struct {
	Op
	Snapshot []byte
}

// clerk's state saved in server
type Clerkstate struct {
	/* LastReqId represents last request's id from the clerk
	 * using to guarantee limited Idenpotence in the case that clerk sends request one by one
	 * consecutive duplicated request will be idempotent
	 */
	LastReqId int64
	Result    string
}

func createStateMachine(gid, shard int, dispatch func(op *Op, ret Result),
	sendGetShard func(svrname string, args *GetShardArgs, reply *GetShardReply) bool) *stateMachine {
	sm := &stateMachine{
		gid:          gid,
		shard:        shard, // for debug
		sendGetShard: sendGetShard,
		dispatch:     dispatch,
	}
	sm.transCond = sync.NewCond(&sm.mutex)
	sm.finisCond = sync.NewCond(&sm.mutex)

	sm.caches = map[string]string{}
	sm.idempotentMap = map[string]Clerkstate{}
	sm.version = 0
	sm.shardStatus = SS_UNOWN
	sm.moveTarget = -1
	sm.mq = createMq([]byte{})

	go sm.applyLogTicker()
	go sm.transShardTicker()
	return sm
}

func (sm *stateMachine) submitEntry(smop *SMOP) {
	sm.mq.push(smop)
}

// consume log entries in message queue
func (sm *stateMachine) applyLogTicker() {
	for {
		sm.finisCond.L.Lock()
		for sm.shardStatus == SS_SAVED {
			sm.finisCond.Wait() // wait for shard-transmit finished
		}
		sm.finisCond.L.Unlock()

		// DPrintf("applyLogTicker(gid=%v,shd=%v,ver=%v,ss=%v) op == nil\n", sm.gid, sm.shard, sm.version, sm.shardStatus)
		op := sm.mq.pop()
		if op == nil {
			sm.mq.wait()
			continue
		}
		// cope with log-entry/event
		switch op.Typ {
		case OP_RECV_SHARD:
			// install new shard
			ret := sm.installShard(&op.Op)
			// dispatch result to RPC handler
			sm.dispatch(&op.Op, Result{Err: ret})

		case OP_CONF_SHARD:
			// change sharding config, may move shard
			sm.changeConfig(&op.Op)
			/* finisCond.Wait() can not be there, because crash and reboot will start from mq.pop(),
			   then will execute operation while transmitting. */

		case OP_SNAPSHOT:
			sm.applySnap(op.Snapshot)

		case OP_GET, OP_PUT, OP_APPEND:
			// execute lightweigh command
			ret := sm.exeCmd(&op.Op)
			// dispatch result to RPC handler
			sm.dispatch(&op.Op, ret)

		default:
			panic("applyLogTicker switch error")
		}
	}
}

// receive a shard from peer replica group and install into the state machine
// - 1.only install the shard with the same version as state machine waiting for.
// - 2.reply OK to lower version RPC to let send quit sending, because sender maybe
// crash and restart, then redo operation log.
// - 3.must be idempotent for every shard's version, all servers in the sender replica
// group will transmit the same shard to waiting group(there will be multiple identical logs)
func (sm *stateMachine) installShard(op *Op) (err string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	newShards := op.NewShard

	// compare the version of two state machines
	if newShards.Version > sm.version { // receiver lags behind sender, later re-send again
		return
	} else if newShards.Version < sm.version {
		return OK
	}

	err = OK
	// detecting consecutive duplicated request, but discontinuous, can't
	if sm.idempotentMap[op.ClientKey].LastReqId == op.RequestId {
		return // consecutive duplicate request
	} else {
		// maintain clerk's status
		sm.idempotentMap[op.ClientKey] = Clerkstate{
			LastReqId: op.RequestId,
		}
	}

	// transition shard status
	if sm.shardStatus == SS_WAITING {
		sm.shardStatus = SS_OWNED
	} else if sm.shardStatus == SS_OWNED {
		return // has installed
	} else {
		panic(fmt.Sprintf("installShard invalid shardStatus(%v)", sm.shardStatus))
	}

	// accept the new shard
	sm.caches = newShards.Shard
	sm.idempotentMap = newShards.Cstate

	return
}

// execute Get/Put/Append log entry from raft after got majority agreement
func (sm *stateMachine) exeCmd(op *Op) (ret Result) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	// check if the op operate key that is in the shard that is responsible by current replica group
	if sm.shardStatus == SS_UNOWN || sm.shardStatus == SS_SAVED {
		ret.Err = ErrWrongGroup
		return
	}

	if sm.shardStatus == SS_WAITING {
		ret.Err = "Waiting"
		return
	}

	// detecting consecutive duplicated request, but discontinuous, can't
	cs := sm.idempotentMap[op.ClientKey]
	if cs.LastReqId == op.RequestId {
		if op.Typ == OP_GET {
			ret.Out = cs.Result
			ret.Err = OK
			return
		} else {
			ret.Err = OK
			return // consecutive duplicate request
		}
	}

	// run command
	switch op.Typ {
	case OP_GET:
		ret.Out = sm.caches[op.Key]
	case OP_PUT:
		sm.caches[op.Key] = op.Value
	case OP_APPEND:
		sm.caches[op.Key] += op.Value
	default:
		panic("unknown command type")
	}
	ret.Err = OK

	// maintain state of clerk
	sm.idempotentMap[op.ClientKey] = Clerkstate{
		LastReqId: op.RequestId,
		Result:    ret.Out,
	}
	return
}

// - 1.execute config-changed log entry from raft after got majority agreement.
//
// - 2.must guarantee Crash-Recovery for this heavyweight operation,
// because this operation with RPC has not an entire lock to mutex snapshot() to gaurantee Atomicity
// so can keep the snapshot in disk recoverable
func (sm *stateMachine) changeConfig(op *Op) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	reConfig := op.ReConfig

	// check if log entry is valid yet
	if sm.version >= reConfig.Version {
		return
	}

	DPrintf("%p changeConfig(gid=%v,shd=%v,ver=%v,ss=%v) -> (gid=%v,shd=%v,ver=%v)\n", sm, sm.gid, sm.shard, sm.version, sm.shardStatus, reConfig.To, sm.shard, reConfig.Version)
	defer func(ss ShardStatus) {
		DPrintf("%p changeConfig(gid=%v,shd=%v,ver=%v,ss=%v) -> (gid=%v,shd=%v,ver=%v,ss=%v)\n", sm, sm.gid, sm.shard, sm.version, ss, reConfig.To, sm.shard, reConfig.Version, sm.shardStatus)
	}(sm.shardStatus)

	// transition status of state machine
	switch sm.shardStatus {
	case SS_UNOWN:
		if reConfig.To == sm.gid {
			if reConfig.Version == 1 {
				sm.shardStatus = SS_OWNED
			} else {
				sm.shardStatus = SS_WAITING
			}
		}
		sm.version = reConfig.Version

	case SS_WAITING:
		// do nothing and not block subsequent operations

	case SS_SAVED:
		panic("changeConfig: state machine in SS_SAVED can not redo operation log")

	case SS_OWNED:
		if reConfig.To != sm.gid {
			sm.shardStatus = SS_SAVED // stop serving for clerks
			sm.moveTarget = reConfig.To
			sm.moveTargetSvrs = reConfig.ToSvrs
			sm.transCond.Broadcast() // wakeup background ticker
		}
		sm.version = reConfig.Version

	default:
		panic("changeConfig switch error")
	}

	return
}

// send shard by RPC and will retry util successfully
// if Crashed during RPC, retry after restart state machine
func (sm *stateMachine) transShardTicker() {
	for {
		sm.transCond.L.Lock()
		for sm.shardStatus != SS_SAVED {
			sm.transCond.Wait() // unlock and suspend
		}

		DPrintf("%p transShardTicker(gid=%v,shd=%v,ver=%v,ss=%v) move=%v\n", sm, sm.gid, sm.shard, sm.version, sm.shardStatus, sm.moveTarget)

		tg := sm.moveTarget
		oldss := sm.shardStatus

		servers := deepCopyStrArr(sm.moveTargetSvrs)
		args := &GetShardArgs{
			SID:     sm.shard,
			From:    fmt.Sprintf("%p", sm), // make all server in the replica group distinct
			Version: sm.version,
			Shard:   deepCopyMap2(sm.caches),
			Cstate:  deepCopyMap3(sm.idempotentMap),
			// Shard:  sm.caches,        // no need deep-copy
			// Cstate: sm.idempotentMap, // because no one read/write this 2 maps during rpc
		}
		sm.transCond.L.Unlock()

		if len(servers) == 0 {
			panic("transShardTicker: len(servers) == 0")
		}

		// transmit shard. exactly-once
		isSuccess := false
		for !isSuccess {
			for _, svrname := range servers {
				reply := GetShardReply{}
				if ok := sm.sendGetShard(svrname, args, &reply); !ok {
					continue
				}
				if reply.Err == OK {
					isSuccess = true
					break
				}
			}
			// retry util transmit successfully
			time.Sleep(80 * time.Millisecond)
		}

		// reclaim the space of transmitted shard
		sm.mutex.Lock()
		if sm.version != args.Version { // for debug
			panic(fmt.Sprintf("sm.version != args.Version (gid=%v shd=%v) (tar=%v ver=%v ss=%v)->(tar=%v ver=%v ss=%v)\n", sm.gid, sm.shard, tg, args.Version, oldss, sm.moveTarget, sm.version, sm.shardStatus))
		}
		if sm.shardStatus != SS_SAVED { // for debug
			panic(fmt.Sprintf("sm.shardStatus(%v) != SS_SAVED\n", sm.shardStatus))
		}

		DPrintf("%p transShardTicker(gid=%v,shd=%v,ver=%v,ss=%v) move=%v successfully successfully\n", sm, sm.gid, sm.shard, sm.version, sm.shardStatus, sm.moveTarget)

		/* assert sm.shardStatus == SS_SAVED and sm.moveTarget and sm.version not changed.
		 * no need Double-Check after re-acquire lock:
		 *	while sm.shardStatus == SS_SAVED, state machine will stop consuming any operation log.
		 * Stop The World, so no updation during RPC
		 */
		sm.caches = map[string]string{}
		sm.idempotentMap = map[string]Clerkstate{}
		sm.shardStatus = SS_UNOWN
		sm.moveTarget = -1
		sm.moveTargetSvrs = []string{}
		sm.mutex.Unlock()

		sm.finisCond.Broadcast() // wakeup
	}
}

func (sm *stateMachine) getShardStatus() (ShardStatus, int, int) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	return sm.shardStatus, sm.moveTarget, sm.version
}

func (sm *stateMachine) getShardVersion() int {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	return sm.version
}

// create a snapshot
func (sm *stateMachine) snapshot() []byte {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sm.caches)
	e.Encode(sm.idempotentMap)
	e.Encode(sm.version)
	e.Encode(sm.shardStatus)
	e.Encode(sm.moveTarget)
	e.Encode(sm.moveTargetSvrs)
	e.Encode(sm.mq.snapshot())

	return w.Bytes()
}

// restore state machine's status to snapshot
func (sm *stateMachine) applySnap(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var caches map[string]string
	var idempotentMap map[string]Clerkstate
	var version int
	var shardStatus ShardStatus
	var moveTarget int
	var moveTargetSvrs []string
	var mqSnapshot []byte

	if d.Decode(&caches) != nil || d.Decode(&idempotentMap) != nil ||
		d.Decode(&version) != nil || d.Decode(&shardStatus) != nil ||
		d.Decode(&moveTarget) != nil || d.Decode(&moveTargetSvrs) != nil ||
		d.Decode(&mqSnapshot) != nil {
		panic("stateMachine applySnap Decode error")
	} else {
		sm.mutex.Lock()
		sm.caches = caches
		sm.idempotentMap = idempotentMap
		sm.version = version
		sm.shardStatus = shardStatus
		sm.moveTarget = moveTarget
		sm.moveTargetSvrs = moveTargetSvrs
		sm.mq = createMq(mqSnapshot)
		DPrintf("%p applySnap(gid=%v,shd=%v,ver=%v,ss=%v)\n", sm, sm.gid, sm.shard, sm.version, sm.shardStatus)
		sm.mutex.Unlock()

		/* wakeup suspended tasks/goroutines!!!
		 *	apply snapshot may cause state machine skip some log entries. if state machine skip
		 * the SS_OWNED operation that wakeup transCond, then the background transmitting task
		 * will wait forever, Lost-Wakeup
		 */
		sm.transCond.Broadcast()
		sm.finisCond.Broadcast()
	}
}
