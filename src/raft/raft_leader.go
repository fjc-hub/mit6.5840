package raft

import (
	"sort"
	"time"
)

// Automatic Failure Recovery manifests in 2 aspect:
// 1.First, a leader crashed, the Rafts will elect a new leader, it proceeds the work of
// old crashed leader to replicate its logs to majority of followers.
// 2.Second, leader has resposibility to send missing logs of follower's log queue,
// even the follower lose its all logs, leader still need re-transmit all logs

// spawn log-sync goroutine for every other peer nodes
func (rf *Raft) replicateLogs() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// construct single goroutine for every single peer node
		// replicate logs or send heartbeats, periodically
		go func(peer int) {
			for !rf.killed() {
				rf.mu.Lock()
				if rf.killed() || rf.status != SLEADER {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				go rf.syncLogOrHeartbeat(peer) // Don't wait, run background.otherwise maybe delay next heartbeat

				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}

	// maintain Leader's CommitIndex in background
	go rf.commitLogs()
}

// Leader replication Task: replicates its logs into majority of Followers, not all nodes is required.
// Workflow as below:
// - 1.Leader sends a follower a set of logs that it thinks the follower lost.
// - 2.If the follower rejects to append/overwrite the logs, leader re-send append RPC with more logs to the follower.
// - 3.If the follower accepts this logs, it appends/overwrites the logs down its local logs and replies leader with OK.
// - 4.After getting accepted reply,Leader will increase these logs'agreement vote counters by 1.
// - 5.Leader will communicates with all nodes by the same upper flow in parallel goroutines.
// - 6.When Leader detects certain logs'Vote Counter are larger than Majority, Leader can confirm
// -   that majority of followers have saved these logs in their local logs, so leader commits them
// -   by moving its CommitIndex point on log array.
// - 7.But now other followers don't know that leader has gotten majority of votes and committed these logs,
// -   they still wait for ACK to commit these logs from leader.
// - 8.Leader attaches this commit-ACK on next append-request RPC(include heartbeat) for saving net bandwith.
// - 9.When Followers accept append-request, they will use commit-ACK attached in RPC to keep pace with Leader.
//
// The Advantage of Raft's Log Replication
// -   Brute Forcing Log Replication(Data-Full-Sync): Leader sends all logs from 0 to all Followers,
// - followers save mismatched logs in local this method and consumes more and more bandwith as logs grow.
// -   Raft Leader sends its log array suffix: sending 1 log first -> if follower rejects -> sending 2 or more logs suffix
// - sending suffix with more and more logs util follower accept(namely the suffix is lost part of follower's log array).
// - this approach is based on how to judge fastly Longest Logs Prefix of two logs array by Log Matching Property:
// - 		follower determines whether prefix of its logs is the same as the prefix of leader's by comparing
// - 	if last log before the suffix in follower's logs is the same as leader's last log before the suffix,
// -	if true follower accept the suffix.
// -
// - In most case, leader only sends logs newly increased in log queue to majority of peers, saving net i/o
//
// when Leader replicates logs into a Follower, it can't assume that the states it maintains for the follower
// is completely right. state like: NextIndex[i], MatchIndex[i].
// Leader maybe request a follower with multiple RPCs for determine how many logs need to send to follower,
// namely determining the missing part of logs of the follower.
// - 	The only assumption that Leader can do is that its logs contains all committed logs(otherwise can't elected leader),
// - so it can append or ovrewrite followers'logs that are uncommitted, case like below:
// - 	A leader received new logs from clients and before commits those logs, it crashed, a new leader activate,
// - then old leader recover and becomes Follower. In this case, new leader's logs queue is shorter and
// - older than old leader's. But new leader can still overwrite uncommitted logs in old leader logs.
//
// A slow raft follower's logs may Far Behind Leader's, because of "majority vote", leader doesn't care whether
// the slow node saves logs received from leader when leader commits logs
//
// leader's CommitIndex may change during retring RPC, so peers may get old CommitIndex. this no matter, no one care
// whether followers'log is committed, only replicating logs in their logs queue is enough for leader and clients
//
// Implicit Heartbeat: when no new uncommitted logs for the follower, this function will send a append operation
// with no logs, this append operation is implicit heartbeat.
// Heartbeat's effect:
//  1. suppress other servers trigger election
//  2. periodically send leader's CommitIndex to notify followers that previous appended logs can be committed
func (rf *Raft) syncLogOrHeartbeat(peer int) {

	rf.mu.Lock()
	var (
		nextIdx      = rf.NextIndex[peer]       // need Double-Check after RPC
		term         = rf.currentTerm           // need Double-Check after RPC
		logsLen      = rf.le                    // need Double-Check after RPC
		prevLogTerm  = rf.logs[nextIdx-1].Term  // need Double-Check after RPC
		appendLogs   = rf.logs[nextIdx:logsLen] // need Double-Check after RPC,using logsLastTerm,appendLogs may change
		logsLastTerm = rf.logs[rf.le-1].Term
		commitIndex  = rf.CommitIndex
	)
	rf.mu.Unlock()

	// request the Follower to replicate logs in local, if rejected by follower, retry with more older logs
	for !rf.killed() && nextIdx >= 0 {
		if t := len(appendLogs); t > 0 && appendLogs[t-1].Term != term {
			// don't replicate/commit logs that belongs to Old Leader into followers.
			// maybe cause the newly elected Leader contains not all committed logs (Leader Completeness Property)
			// avoid that the same index in different log of different node commit different log entries
			return
		}

		args := &AppendEntriesArgs{
			Term:         term,
			Entries:      appendLogs,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: nextIdx - 1,
			LeaderCommit: commitIndex,
		}

		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(peer, args, &reply); !ok {
			return
		}

		rf.mu.Lock()
		// Double-Check if raft is still leader and alive
		// Double-Check if currentTerm is the same as original term before sending RPC
		// Double-Check whether current logs are still equal to original logs sub-queue by Log Matching Property
		// 		Log Matching Property refers to section 5.3 of original paper
		if rf.killed() || rf.status != SLEADER || rf.currentTerm != term || rf.NextIndex[peer] != nextIdx ||
			rf.le < logsLen || rf.logs[nextIdx-1].Term != prevLogTerm ||
			rf.logs[logsLen-1].Term != logsLastTerm {
			// something changed, invalidate reply
			rf.mu.Unlock()
			return
		}

		// check if the append/overwrite operation is accepted by follower
		if reply.Success {
			// replicate logs into peer node successfully
			rf.MatchIndex[peer] = logsLen - 1
			rf.NextIndex[peer] = logsLen
			rf.mu.Unlock()
			return
		} else {
			// slower approach, growing suffix one by one
			// nextIdx--

			// faster approach than upper method, assume reply's args is valid
			if reply.XLen <= args.PrevLogIndex {
				nextIdx = reply.XLen
			} else {
				idx := -1 // min index of log with reply.XTerm
				for i := nextIdx - 2; i >= 0; i-- {
					if rf.logs[i].Term < reply.XTerm {
						break
					} else if rf.logs[i].Term == reply.XTerm {
						idx = i
						break
					}
				}

				if idx == -1 {
					nextIdx = reply.XIndex
				} else {
					nextIdx = idx + 1 // less than old nextIdx
				}
			}

			if nextIdx > 0 {
				appendLogs = rf.logs[nextIdx:logsLen]
				prevLogTerm = rf.logs[nextIdx-1].Term
				rf.NextIndex[peer] = nextIdx
			}
		}
		rf.mu.Unlock()
	}
}

// compare two logs sub-queue is equal by Log Matching Property
func compareLogs(l0, l1 *LogEntry) bool {
	// assume log l0's index is equal to l1's
	// so if they have the same term, they are the same log and logs before they are all equal
	return l0.Term == l1.Term
}

// the maintaining task of Leader's CommitIndex is split out of workflow of Log-Replication
//   - find which log entries get majority of vote, namely replicated in majority of followers'log
//
// Log-Replication Restriction for Leader
// for ensuring new elected Leader contains all committed log entries once it's being leader:
//   - Leader can not replicate old log of old leader into followers
//   - Leader can not commit old log of old leader
//   - Leader can replicate and commit its own log ONLY!!!
//
// this restriction and the Leader Election Restriction is the guarantee of Leader Completeness Property
//
// How to determine if the log belongs to current Leader?
//   - like determining whether a log is newer than other in Leader Election Restriction
//     comparing last entry's term and index in log array
//
// example:
//   - log[0,1,2] represents a log: length=3; contains 3 entries/commands;
//     the number in it are the term of leaders that appended the entry into log.
//   - this log belongs to the leader in the term 2, because the term of the last entry is 2
//     it represents that this log was created during term 2 by the old leader.
//     maybe a newer log[0,1,3] was committed, if current leader replicates and commits log[0,1,2]
//     it could causes committed log entry with term 3 is overwrote by log entry with 2.
//   - if current leader of term 6 append a entry into log, it becomes log[0,1,2,6]
//     so this log belongs to current leader, it can be replicated and committed
//   - new leaders can not 'immediately' conclude that it was committed if leader don't
//     do some extra work that communicates with followers. In Raft, Leader doesn't do that work,
//     upon a node becomes a leader, it can assume that it saved all log from old leader
func (rf *Raft) commitLogs() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.status != SLEADER {
			rf.mu.Unlock()
			return
		}

		// check if current log belongs to current leader by checking last log entry's term
		if rf.logs[rf.le-1].Term != rf.currentTerm {
			// Leader can not commit previous term's log
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// get the largest index (that leader think so) in replicated logs of each node, except leader
		sortArr := copyIntSlice(rf.MatchIndex, rf.me)

		// descending sort
		sort.Slice(sortArr, func(i, j int) bool {
			return sortArr[i] > sortArr[j]
		})

		major := len(rf.peers) / 2 // assume server number is odd

		// get the max index that has major indexs larger than it
		// update rf.CommitIndex if true
		if t := sortArr[major-1]; t > rf.CommitIndex {
			if rf.logs[t].Term == rf.currentTerm {
				// Leader replicates and commits only its own log
				rf.CommitIndex = t
			} else {
				// don't replicate/commit logs that belongs to Old Leader into followers.
				// maybe cause the newly elected Leader contains not all committed logs (Leader Completeness Property)
			}
		}

		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

// throw newly committed logs up to upper layer application(namely state machine)
func (rf *Raft) apply2StateMachine(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		for ; rf.LastApplied+1 <= rf.CommitIndex; rf.LastApplied++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.LastApplied+1].Command,
				CommandIndex: rf.LastApplied + 1,
			}
			applyCh <- msg
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}
