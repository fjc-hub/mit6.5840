package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// server's status
type ServerState int

const (
	SFOLLOWER ServerState = iota
	SCANDIDATE
	SLEADER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// snapshot information
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	mq *MQ

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int // candidateId that received vote in current term (or -1 if none), leader/candidate's = me
	status      ServerState

	expiredTime int64 // election timer used to trigger new election periodically when no leader

	/*
		logs queue Partition:
								 rf.LastApplied                           rf.CommitIndex
		|-|------Committed, Applied------|----------Committed, Unapplied----------|-----Uncommitted-----|
		[0]     [1, rf.LastApplied]		     [rf.LastApplied+1, rf.CommitIndex]	    [rf.CommitIndex+1,)

		1.committed logs represent itself has replicated on majority of raft servers
		2.log entry 0 is a dummy-element, don't contains it while creating snapshot
	*/
	logs        []LogEntry // using []*LogEntry will send error log when underlying LogEntry structure change
	le          int        // length of logs, init 1
	CommitIndex int        // index of highest log entry known to be committed
	LastApplied int        // index of highest log entry known to be applied/installed

	appendCond *sync.Cond // bind to the event that leader got new command from upper clients
	applyCond  *sync.Cond // bind to the apply log entries event

	// leader special state
	NextIndex  []int // leader considers that node[i] lost(may not lose) logs[NextIndex[i], le]
	MatchIndex []int // leader considers that node[i] has replicated/save(may not commit) logs[0,MatchIndex[i]]

	// snapshot
	snapshot      []byte // a prefix of logs is compacted as a snapshot of the upper state-machine
	snapshotTerm  int    // the term number of last included log entry in snapshot
	snapshotIndex int    // the index of last included log entry in snapshot
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.lock()
	defer rf.unlock()
	return rf.currentTerm, rf.status == SLEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// Needed to Persist:
//   - logs: recover upper volatile state-machine's status after malfunction by replaying logs
//   - snapshot: a snapshot of upper state-machine and used to compact logs
//   - currentTerm + votedFor: incorporate into unique vote. maybe a vote gives to many node if don't persist
//   - snapshotTerm + snapshotIndex: snapshot information
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshotTerm)
	e.Encode(rf.snapshotIndex)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if len(data) > 0 {
		// recover raft states
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		var logs []LogEntry
		var currentTerm int
		var votedFor int
		var snapshotTerm int
		var snapshotIndex int
		if d.Decode(&logs) != nil ||
			d.Decode(&currentTerm) != nil ||
			d.Decode(&votedFor) != nil ||
			d.Decode(&snapshotTerm) != nil ||
			d.Decode(&snapshotIndex) != nil {
			panic("readPersist Decode states error")
		} else {
			rf.le = len(logs)
			rf.logs = logs
			rf.currentTerm = currentTerm
			rf.votedFor = votedFor
			rf.snapshotTerm = snapshotTerm
			rf.snapshotIndex = snapshotIndex
		}
	}

	// start from snapshot
	rf.LastApplied = rf.snapshotIndex
	rf.CommitIndex = rf.snapshotIndex

	if len(snapshot) > 0 {
		// recover snapshot
		rf.snapshot = snapshot
		rf.mq.push(&ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  rf.snapshotTerm,
			SnapshotIndex: rf.snapshotIndex,
		})
	}
}

// the upper service says it has created a snapshot that has
// all info up to and including index. this means the
// upper service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	rf.lock()
	defer rf.unlock()

	defer func() {
		DPrintf("%v End Snapshot index(%v) %v\n", rf.me, index, Raft2string(rf))
	}()

	if rf.LastApplied < index || rf.snapshotIndex > index {
		// this part of entries (logs[rf.LastApplied+1:index] ) has not been committed
		panic("Snapshot: fast fail")
	}

	off := rf.offset(index)

	// overwrite old snapshot (if existed)
	rf.snapshot = snapshot
	rf.snapshotTerm = rf.logs[off].Term
	rf.snapshotIndex = index

	// trim log array
	rf.logs = append(rf.logs[:1], rf.logs[off+1:]...)
	rf.le = len(rf.logs)

	rf.persist()
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler. candidate server invoke this to call others for gathering votes
// SFOLLOWER server should reset the election-timeout when be called by candidates,
// to avoid this server initiates a new election
//
// must idempotent for <candidate, term> to avoid candidate gets more than 1 vote from 1 server
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock()
	defer rf.unlock()

	if args.Term < rf.currentTerm {
		// reject stale term RPC request and re-send caller new term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		// detected new term from certain candidate
		rf.recvNewTerm(args.Term)
	}

	// compare the latest log of caller and callee, if caller's newer ,grant vote
	// vote at most 1 in a term, implement it by <rf.currentTerm, rf.votedFor>
	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) &&
		(rf.lastEntryTerm() < args.LastLogTerm ||
			(rf.lastEntryTerm() == args.LastLogTerm && rf.lastEntryIndex() <= args.LastLogIndex)) {

		// update raft's <currentTerm, voteFor> pair to follow the candidate
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.status = SFOLLOWER // leader and candidate node will not vote to any other candidates if it has higher term
		rf.persist()

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		// reject vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int
	Entries      []LogEntry // logs if can be appended
	PrevLogTerm  int        // term of the log preceding Entries[0] in caller's logs queue
	PrevLogIndex int        // index of the log preceding Entries[0] in caller's logs queue

	// Leader’s commitIndex.
	// used to tell follower commit all logs before it. attached ACK in subsequent request like TCP second handshake
	LeaderCommit int
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool

	// optimization: accelerate to get longest identical prefix between leader logs array and follower logs array
	XTerm  int // term in the conflicting entry (if exists)
	XIndex int // index of first entry with that term (if exists)
	XLen   int // log length
}

// AppendEntries RPC handler: remotely invoked by Leader to replicate log entries, also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock()
	defer rf.unlock()

	var rfPrevTerm int
	if args.Term < rf.currentTerm {
		// reject stale term RPC request and re-send the caller new term
		goto reject_append
	} else if args.Term > rf.currentTerm || rf.status == SCANDIDATE {
		// detected new term from certain leader
		// or a candidate discovers current leader in the same term
		rf.recvNewTerm(args.Term)
	}
	// flush election timer
	rf.expiredTime = randTimestamp()

	// assert: rf.status == SFOLLOWER

	// get the term of the log entry with index args.PrevLogIndex in follower's log
	if args.PrevLogIndex >= rf.realLogLen() {
		goto reject_append
	} else if 0 < args.PrevLogIndex && args.PrevLogIndex < rf.snapshotIndex {
		// compacted log entries can not get their terms
		goto reject_append
	} else {
		rfPrevTerm = rf.term(args.PrevLogIndex)
	}
	// check if logs in args can be appended/overwritten
	if rfPrevTerm != args.PrevLogTerm {
		// not identical prefix between between leader logs array and follower logs array
		// so can't append/overwrite leader's suffix as follower's suffix
		reply.XTerm = rfPrevTerm
		start := rf.offset(args.PrevLogIndex)
		for i := start; i >= 0; i-- {
			if rf.logs[i].Term != rfPrevTerm {
				reply.XIndex = rf.index(i + 1)
				break
			}
		}
		goto reject_append
	}

	// accept to append/overwrite new log
	if len(args.Entries) > 0 {
		off := rf.offset(args.PrevLogIndex)
		// cut off the part of follower's log that is inconsistent with the leader log
		lmt := min(len(args.Entries), rf.le-off-1)
		diff := -1 // the first entry that inconsistent with leader's log
		for i := 0; i < lmt; i++ {
			if args.Entries[i].Term != rf.logs[i+off+1].Term {
				diff = i + off + 1
				break
			}
		}

		if diff > 0 {
			/* cut off inconsistent log entries, if already detected inconsistent part now:
			 * x = inconsistent; ? = unknown;
			 *  0      off off+1   [args.Entries]              rf.le
			 * |-|--------|------------------x??????????|????????|         Follower's
			 * 	                           diff
			 */
			rf.logs = append(rf.logs[:off+1], args.Entries...)
		} else {
			/* joint new log for follower, if not detected inconsistent part now
			 * constitute a new log for follower:
			 *  0      off off+1   [args.Entries]         [tail]
			 * |-|--------|-----------------------------|?????????????     Leader's
			 * |-|--------|-----------------------------|--------| 	       Follower's
			 * |-|--------|-------------------------|	       	           Follower's
			 */
			tail := []LogEntry{}
			if t := off + len(args.Entries) + 1; t < len(rf.logs) {
				tail = rf.logs[t:]
			}
			rf.logs = append(append(rf.logs[:off+1], args.Entries...), tail...) // do nothing if no difference
		}
		rf.le = len(rf.logs)
		rf.persist()

		rf.CommitIndex = min(args.LeaderCommit, rf.lastEntryIndex())
	} else {
		// heartbeat
		// follower's Log[:args.PrevLogIndex] is identical to leader's Log[:args.PrevLogIndex]
		rf.CommitIndex = min(args.LeaderCommit, args.PrevLogIndex)
	}

	// apply committed log entries if rf.CommitIndex changed
	if rf.LastApplied < rf.CommitIndex {
		rf.applyCond.Broadcast() // wakeup
	}

	reply.XLen = rf.realLogLen()
	reply.Term = rf.currentTerm
	reply.Success = true
	return

reject_append:
	reply.XLen = rf.realLogLen()
	reply.Term = rf.currentTerm
	reply.Success = false
	return
}

// InstallSnapshot RPC arguments structure.
type InstallSnapshotArgs struct {
	Term int // leader's currentTerm

	// snapshot's states
	LastIncludedTerm  int
	LastIncludedIndex int
	Snapshot          []byte // snapshot
}

// InstallSnapshot RPC reply structure.
type InstallSnapshotReply struct {
	Term    int // callee's currentTerm
	Success bool
}

// InstallSnapshot RPC handler: remotely invoked by Leader to send chunks of a snapshot to a follower.
// Leaders always send chunks in order, and follower will receive chunks in order, no need to assemble chunks
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock()
	defer rf.unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		// detect higher term
		rf.recvNewTerm(args.Term)
	}
	// flush election timer
	rf.expiredTime = randTimestamp()

	reply.Term = args.Term

	idx, term := args.LastIncludedIndex, args.LastIncludedTerm

	// check if the snapshot from leader is fresh enough
	if idx < rf.snapshotIndex {
		// older snapshot
		reply.Success = false
		return
	} else if idx == rf.snapshotIndex {
		// the same snapshot as follower itself
		reply.Success = true
		return
	} else if idx >= rf.realLogLen() {
		// follower's log lags behind the snapshot
		rf.logs = rf.logs[:1]
		rf.le = len(rf.logs)
		rf.LastApplied = idx
		rf.CommitIndex = idx
	} else {
		// trim follower's log and update relerant states
		rf.logs = append(rf.logs[:1], rf.logs[rf.offset(idx)+1:]...)
		rf.le = len(rf.logs)
		rf.LastApplied = max(rf.LastApplied, idx)
		rf.CommitIndex = max(rf.CommitIndex, idx)
	}

	// apply committed log entries if rf.CommitIndex changed
	if rf.LastApplied < rf.CommitIndex {
		rf.applyCond.Broadcast() // wakeup
	}

	// overwrite old snapshot
	rf.snapshot = args.Snapshot
	rf.snapshotTerm = term
	rf.snapshotIndex = idx

	// persist updated raft states and the new snapshot
	rf.persist()

	// install the new snapshot into upper state-machine through message queue
	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  term,
		SnapshotIndex: idx,
	}
	rf.mq.push(msg)

	reply.Success = true
	return
}

// code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// code to send a AppendEntries RPC to a server.
// server is the index of the target server in rf.peers[].
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	if len(args.Entries) > 0 {
		DPrintf("sendAppendEntries %v->%v term=%v args(%v,%v,%v) log=%v\n", rf.me, server, rf.currentTerm,
			args.PrevLogTerm, args.PrevLogIndex, len(args.Entries), serializeLogs(args.Entries))
	}
	defer func() {
		if len(args.Entries) > 0 {
			if ok {
				DPrintf("sendAppendEntries %v->%v term=%v args(%v,%v,%v) reply(%v)\n", rf.me, server,
					rf.currentTerm, args.PrevLogTerm, args.PrevLogIndex, len(args.Entries), obj2json(reply))
			} else {
				DPrintf("sendAppendEntries %v->%v term=%v args(%v,%v,%v) timeout\n", rf.me, server,
					rf.currentTerm, args.PrevLogTerm, args.PrevLogIndex, len(args.Entries))
			}
		}
	}()

	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// code to send a InstallSnapshot RPC to a server.
// server is the index of the target server in rf.peers[].
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("sendSnapshot %v->%v args(%v)\n", rf.me, server, insArgs(args))
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	DPrintf("sendSnapshot %v->%v args(%v) reply(%v)\n", rf.me, server, insArgs(args), obj2json(reply))
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.lock()
	defer rf.unlock()
	isLeader := rf.status == SLEADER
	term := rf.currentTerm
	if !isLeader {
		return -1, term, isLeader
	}

	rf.logs = append(rf.logs, LogEntry{Term: term, Command: command})
	index := rf.realLogLen()
	rf.le = len(rf.logs)
	rf.persist()

	rf.appendCond.Broadcast() // notify background goroutine that the event appeared

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// check periodically if need initiate new election by rf.expiredTime
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Check if a leader election should be started.
		rf.lock()
		if (rf.status == SFOLLOWER || rf.status == SCANDIDATE) && rf.expiredTime <= time.Now().UnixMilli() {
			// transition to candidate status
			rf.status = SCANDIDATE
			rf.votedFor = rf.me
			rf.currentTerm++
			rf.persist()
			rf.expiredTime = randTimestamp() // reset election timer after quit this election

			var (
				term         = rf.currentTerm // need Double-Check after RPC
				lastLogIndex = rf.lastEntryIndex()
				lastLogTerm  = rf.lastEntryTerm()
			)
			rf.unlock()

			// no waiting, if so, election in current term maybe delay next election
			go rf.leaderElection(term, lastLogIndex, lastLogTerm)

		} else {
			rf.unlock()
		}

		time.Sleep(50 * time.Millisecond) // check timer periodically
	}
}

// initiate a new election to try to transition to Leader
// possible results during election:
//   - 1.Transition to Leader in currentTerm: when rf got majority of vote of currentTerm
//   - 2.Transition to Follower: if receive RPC message with higher term when election period
//   - 3.Keep on Candidate: none of this has happened
func (rf *Raft) leaderElection(term, lastLogIndex, lastLogTerm int) {

	args := RequestVoteArgs{
		Term: term, CandidateId: rf.me,
		LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}

	// request other nodes for votes in parallel
	var votes atomic.Int64
	var canStop atomic.Bool // election can be stopped
	votes.Store(int64(len(rf.peers)) / 2)
	canStop.Store(false)
	for peer := range rf.peers {
		if canStop.Load() { // loop fast quit
			break
		}
		if peer == rf.me {
			continue // avoid RPC itself
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply) // will retry if fails within default timeout
			if !ok {
				return
			}

			rf.lock()
			// Double-Check if raft is still candidate
			// Double-Check if currentTerm is the same as original term before sending RPC
			// 		log will not change if keeping candidate in the same term after RPC
			if rf.status != SCANDIDATE || rf.currentTerm != term {
				// something changed, invalidate reply
				rf.unlock()
				return
			}

			// check if detected higher term
			if reply.Term > term {
				rf.recvNewTerm(reply.Term)
				rf.unlock()
				return
			}

			// count Agreement Vote
			if reply.VoteGranted {
				votes.Add(-1)
			}

			// check if raft gets majority of vote
			if votes.Load() <= 0 {
				// transition to Leader
				rf.status = SLEADER
				// reinitialize Leader's states of followers
				for i := range rf.peers {
					rf.NextIndex[i] = rf.realLogLen()
					rf.MatchIndex[i] = 0
				}
				// start timer goroutine in background to send leader's heartbeat
				go rf.replicateLogs() // register replication task
				rf.unlock()
				canStop.Store(true) // stop election
				return
			}
			rf.unlock()
		}(peer)
	}
}

// wait the event that there are apply-needed committed log entris is true
// util true, apply/issue them into rf.mq
//
// wakeup this task by condition variable rf.applyCond, when committed entries in log
// using Broadcast() not Singal(),
// because Singal may only wakeup a non-waiting goroutine the waits for the lock
func (rf *Raft) applyLogEntriesTicker() {
	for !rf.killed() {

		// the lock applyCond.L is the same as the lock protecting rf.LastApplied/CommitIndex
		rf.applyCond.L.Lock()
		for rf.LastApplied == rf.CommitIndex { // Spurious-Wakeup solved: for loop the event
			/**
			 * Lost-Wakeup solved:
			 *   releasing applyCond.L lock and suspending/sleeping goroutine is atomically execute in Wait()
			 *   namely, no wakeup() can be called between Event-Checking and Sleeping.
			 */
			rf.applyCond.Wait()
		}

		arr := make([]*ApplyMsg, 0)
		for ; rf.LastApplied+1 <= rf.CommitIndex; rf.LastApplied++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.offset(rf.LastApplied+1)].Command,
				CommandIndex: rf.LastApplied + 1,
			}
			arr = append(arr, &msg)
		}

		if len(arr) > 0 {
			// sending into mq without blocking and notify ccomsumers
			rf.mq.push(arr...)
		}

		rf.applyCond.L.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.status = SFOLLOWER
	rf.expiredTime = time.Now().UnixMilli() + rand.Int63n(500)
	rf.logs = make([]LogEntry, 1)
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))

	rf.logs[0] = LogEntry{Term: 0} // dummy entry
	rf.le = 1

	rf.mq = createMq()

	rf.appendCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)

	// start ticker goroutine to start elections
	go rf.ticker()

	// send committed log entries into message queue
	go rf.applyLogEntriesTicker()

	// send message from message queue to state machine
	go rf.send2StateMachine(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	return rf
}

/*==============================================================================================*/
/*=================================↓↓↓ Raft's Utils Function ↓↓↓================================*/
/*==============================================================================================*/

// If raft server receives a RPC request/response whose term > rf.currentTerm,
// then set currentTerm = T, convert to follower
//
// Before call this, must have held the lock
func (rf *Raft) recvNewTerm(term int) {
	rf.currentTerm = term
	rf.status = SFOLLOWER
	rf.votedFor = -1
	rf.persist()
}

// get the log entry's index in logs array by its real index in the system
//
// Before call this, must have held the lock if caller wants to access safely
func (rf *Raft) offset(index int) int {
	if index == 0 {
		return 0
	}
	return index - rf.snapshotIndex
}

// get the log entry's term in logs array by its real index in the system
// index >= rf.snapshotIndex or index == 0
//
// Before call this, must have held the lock if caller wants to access safely
func (rf *Raft) term(index int) int {
	if index == rf.snapshotIndex {
		return rf.snapshotTerm
	} else if index == 0 {
		return 0
	}
	return rf.logs[index-rf.snapshotIndex].Term
}

// get the real index in the system array by its the log entry's index in logs
//
// Before call this, must have held the lock if caller wants to access safely
func (rf *Raft) index(offset int) int {
	if offset == 0 {
		return 0
	}
	return offset + rf.snapshotIndex
}

// get the whole log's length with compacted log entries in snapshot
//
// Before call this, must have held the lock if caller wants to access safely
func (rf *Raft) realLogLen() int {
	return rf.le + rf.snapshotIndex
}

// get the term of last entry in log
//
// Before call this, must have held the lock if caller wants to access safely
func (rf *Raft) lastEntryTerm() int {
	if rf.le == 1 {
		return rf.snapshotTerm // all entries may be compacted
	}
	return rf.logs[rf.le-1].Term
}

// get the index of last entry in log
//
// Before call this, must have held the lock if caller wants to access safely
func (rf *Raft) lastEntryIndex() int {
	if rf.le == 1 {
		return rf.snapshotIndex // all entries may be compacted
	}
	return rf.le - 1 + rf.snapshotIndex
}

// get the term of the entry before idx by offset on rf.logs. require off > 0
//
// Before call this, must have held the lock if caller wants to access safely
func (rf *Raft) prevEntryTerm(off int) int {
	if off == 1 {
		return rf.snapshotTerm
	}
	return rf.logs[off-1].Term
}

// get the index of the entry before idx by offset on rf.logs. require off > 0
//
// Before call this, must have held the lock if caller wants to access safely
func (rf *Raft) prevEntryIndex(off int) int {
	return off - 1 + rf.snapshotIndex
}

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	rf.mu.Unlock()
}
