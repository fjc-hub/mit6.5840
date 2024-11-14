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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int // candidateId that received vote in current term (or -1 if none), leader/candidate's = me
	status      ServerState

	expiredTime int64 // election timer used to trigger new election periodically when no leader

	le   int // length of logs
	logs []*LogEntry
}

type LogEntry struct {
	Term int
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
		(rf.le == 0 ||
			(rf.logs[rf.le-1].Term < args.LastLogTerm &&
				rf.logs[rf.le-1].Term == args.LastLogTerm && rf.le-1 <= args.LastLogIndex)) {
		// update raft's <currentTerm, voteFor> pair to follow the candidate
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.status = SFOLLOWER // leader and candidate node will not vote to any other candidates

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
	Term     int
	LeaderId int
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler: remotely invoked by Leader to replicate log entries, also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// reject stale term RPC request and re-send the caller new term
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		// detected new term from certain leader by receiving RPC from new term leader
		rf.recvNewTerm(args.Term)
	}
	// flush election timer
	rf.expiredTime = randTimestamp()

	reply.Term = args.Term
	reply.Success = true
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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
			rf.leaderElection()
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// heartbeat timer of Leader raft, exit if raft quits from leader status
func (rf *Raft) hbTimer(term int) {

	rf.heartbeat(term) // upon raft become leader does

	for !rf.killed() {

		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		if rf.status == SLEADER {
			rf.mu.Unlock()

			rf.heartbeat(term)
		} else {
			rf.mu.Unlock()
			break
		}
	}
}

// send heartbeat to other peer servers in parallel if rf is Leader.
//   - function 1: using for suppressing other server trigger election
//   - function 2: if peer raft return higher term, leader raft need transition back follower raft
//
// do not acquire mutex rf.mu before calling it, for avoiding deadlock between raft nodes
func (rf *Raft) heartbeat(term int) {
	// var wg sync.WaitGroup  better not use it, because takes so long to wait all timeout RPC in Bad Case
	args := AppendEntriesArgs{Term: term, LeaderId: rf.me}
	for peer := range rf.peers {
		if peer == rf.me {
			continue // avoid RPC itself
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply) // will retry if fails within default timeout
			if !ok {
				return
			}
			// check whether replied raft holds newer/higher term than rf.me
			// if true, convert from <leader, currentTerm> to <follower, new currentTerm>. Double-Check need!!!
			rf.mu.Lock()
			if rf.currentTerm < reply.Term && rf.status == SLEADER {
				rf.recvNewTerm(args.Term)
			}
			rf.mu.Unlock()
		}(peer)
	}
}

// initiate a new election to try to transition to Leader
// result during election:
//
//	1.Transition to Leader in currentTerm: when rf got majority of vote of currentTerm
//	2.Transition to Follower: if receive RPC message with higher term when election period
//	3.Keep on Candidate: none of this has happened
//
// must acquire mutex rf.mu before calling it, and will unlock it
func (rf *Raft) leaderElection() {
	// transition to candidate node
	rf.status = SCANDIDATE
	rf.votedFor = rf.me
	rf.currentTerm++
	defer func() {
		rf.expiredTime = randTimestamp() // reset election timer after quit this election
	}()

	var (
		// invariant can be shared safely
		term         = rf.currentTerm
		args         = RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.le, LastLogTerm: rf.getLastLogTerm()}
		target int64 = int64(len(rf.peers)+1) / 2 // target vote
	)

	rf.mu.Unlock() // unlock mutex when request others for votes

	// request other nodes for votes in parallel
	var votes atomic.Int64
	var canStop atomic.Bool // election can be stopped
	votes.Store(1)
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

			if reply.Term > term {
				// check term consistency: detected new Term from new leaders or new candidates
				rf.mu.Lock()
				if rf.currentTerm < reply.Term { // Double-Check
					rf.recvNewTerm(reply.Term) // transition to follower node
					rf.mu.Unlock()
					canStop.Store(true) // stop election
					return
				}
				rf.mu.Unlock()
				return
			} else if reply.Term == term && reply.VoteGranted {
				votes.Add(1) // count vote of the term

				// check if rf gets majority of vote
				rf.mu.Lock()
				// Double-Check if rf.currentTerm is the same as original term
				if t := votes.Load(); t >= target && rf.status == SCANDIDATE && rf.currentTerm == term {
					// transition to Leader
					rf.status = SLEADER
					// start timer goroutine in background to send leader's heartbeat
					go rf.hbTimer(rf.currentTerm) // register time task
					rf.mu.Unlock()
					canStop.Store(true) // stop election
					return
				}
				rf.mu.Unlock()
				return
			}
		}(peer)
	}
}

func (rf *Raft) getLastLogTerm() int {
	if rf.le == 0 {
		return -1
	}
	return rf.logs[rf.le-1].Term
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
	rf.le = 0
	rf.logs = make([]*LogEntry, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// randomized election timeout
func randTimestamp() int64 {
	return time.Now().UnixMilli() + 200 + (rand.Int63() % 300)
}
