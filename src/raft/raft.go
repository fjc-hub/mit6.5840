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
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
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

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int // candidateId that received vote in current term (or -1 if none), leader/candidate's = me
	status      ServerState

	expiredTime int64 // election timer used to trigger new election periodically when no leader

	/*
		logs queue Partition:

								rf.LastApplied                           rf.CommitIndex
		|------Committed, Applied------|----------Committed, Unapplied----------|-----Uncommitted-----|
			  [1, rf.LastApplied]		   [rf.LastApplied+1, rf.CommitIndex]	  [rf.CommitIndex+1,)

		committed logs represent itself has replicated on majority of raft servers
	*/
	logs        []*LogEntry
	le          int   // length of logs, init 1
	CommitIndex int   // index of highest log entry known to be committed
	LastApplied int   // index of highest log entry known to be applied/installed
	NextIndex   []int // leader considers that node[i] lost(may not lose) logs[NextIndex[i], le]
	MatchIndex  []int // leader considers that node[i] has replicated/save(may not commit) logs[0,MatchIndex[i]]
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == SLEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []*LogEntry
	var currentTerm int
	var votedFor int
	if d.Decode(&logs) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil {
		panic("readPersist Decode error")
	} else {
		rf.le = len(logs)
		rf.logs = logs
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
		(rf.logs[rf.le-1].Term < args.LastLogTerm ||
			(rf.logs[rf.le-1].Term == args.LastLogTerm && rf.le-1 <= args.LastLogIndex)) {

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
	Entries      []*LogEntry // logs if can be appended
	PrevLogTerm  int         // term of the log preceding Entries[0] in caller's logs queue
	PrevLogIndex int         // index of the log preceding Entries[0] in caller's logs queue

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	// check if logs in args can be appended/overwritten
	if t := args.PrevLogIndex; !(0 <= t && t < rf.le) || rf.logs[t].Term != args.PrevLogTerm {
		// not identical prefix between between leader logs array and follower logs array
		// so can't append/overwrite leader's suffix as follower's suffix
		reply.XLen = len(rf.logs)
		if 0 <= t && t < rf.le {
			reply.XTerm = rf.logs[t].Term
			for i := t; i >= 0; i-- {
				if rf.logs[i].Term != rf.logs[t].Term {
					reply.XIndex = i + 1
					break
				}
			}
		}
		goto reject_append
	}
	if rf.CommitIndex > args.LeaderCommit {
		// new Leader may have more smaller CommitIndex than old Leader, but no matter.
		// because new leader saved all committed logs and it won't????
	} else if rf.CommitIndex < args.LeaderCommit {
		rf.CommitIndex = min(args.LeaderCommit, rf.le-1)
	}

	// accept to append/overwrite new logs
	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...) // save logs down to uncommited region
		rf.le = len(rf.logs)
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	return

reject_append:
	reply.Term = rf.currentTerm
	reply.Success = false
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.status == SLEADER
	term := rf.currentTerm
	if !isLeader {
		return -1, term, isLeader
	}

	rf.logs = append(rf.logs, &LogEntry{Term: term, Command: command})
	index := rf.le
	rf.le++
	rf.persist()

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
		rf.mu.Lock()
		if (rf.status == SFOLLOWER || rf.status == SCANDIDATE) && rf.expiredTime <= time.Now().UnixMilli() {
			// transition to candidate status
			rf.status = SCANDIDATE
			rf.votedFor = rf.me
			rf.currentTerm++
			rf.persist()
			rf.expiredTime = randTimestamp() // reset election timer after quit this election

			var (
				term         = rf.currentTerm        // need Double-Check after RPC
				lastLogIndex = rf.le - 1             // need Double-Check after RPC
				lastLogTerm  = rf.logs[rf.le-1].Term // need Double-Check after RPC
			)
			rf.mu.Unlock()

			// no waiting, if so, election in current term maybe delay next election
			go rf.leaderElection(term, lastLogIndex, lastLogTerm)

		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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

			rf.mu.Lock()
			// Double-Check if raft is still candidate
			// Double-Check if currentTerm is the same as original term before sending RPC
			// Double-Check lastLogIndex and lastLogIndex
			if rf.status != SCANDIDATE || rf.currentTerm != term ||
				lastLogIndex != rf.le-1 || lastLogTerm != rf.logs[rf.le-1].Term {
				// something changed, invalidate reply
				rf.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				votes.Add(-1) // count Agreement Vote
			}
			// check if raft gets majority of vote
			if votes.Load() <= 0 {
				// transition to Leader
				rf.status = SLEADER
				// reinitialize Leader's states of followers
				for i := range rf.peers {
					rf.NextIndex[i] = rf.le
					rf.MatchIndex[i] = 0
				}
				// start timer goroutine in background to send leader's heartbeat
				go rf.replicateLogs() // register replication task
				rf.mu.Unlock()
				canStop.Store(true) // stop election
				return
			}
			rf.mu.Unlock()
		}(peer)
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
	rf.le = 1
	rf.logs = make([]*LogEntry, 1)
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))

	rf.logs[0] = &LogEntry{Term: 0} // dummy entry

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.apply2StateMachine(applyCh)

	return rf
}
