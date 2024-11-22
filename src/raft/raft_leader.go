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
//
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
				rf.lock()
				if rf.killed() || rf.status != SLEADER {
					rf.unlock()
					return
				}
				rf.unlock()

				/**Do not Log-Replication for each follower One After One in serializable form.
				 * reason as below:
				 *	 serializing Log Replication makes no sense, and slow throughput compared to concurrent.
				 * RPC caller can't guarantee that callee will receive args in the order that caller sending,
				 * because of unreliable network.
				 *   for example: there are both Long-log AE and Short-log AE in the network at the same time,
				 * although Long-log AE is sent after Short-log AE by leader, (but)follower may receives Long-
				 * log AE first.
				 *   in this case, follower may saves long-log first and then replace long log with short log
				 * that arrived later. the replies of this 2 AE, will arrive Leader in any order also,if leader
				 * using reply value of short-log AE after reply of long-log AE, it will count wrong agreement
				 * vote for the discarded log entries by follower.
				 *
				 * For solving this, follower's AE handler does not delete log entries that follower can not
				 * determine as inconsistent
				 * P.S. AE = appendEntries RPC request
				 */
				go rf.syncLog(peer) // send log or heartbeat

				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}

	// maintain Leader's CommitIndex in background
	go rf.commitLogs()
}

// Leader replication Task: replicates its logs into majority of Followers, not all nodes is required.
// Workflow (In case of no Snapshot) as below:
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
// leader's CommitIndex may change during retring RPC, so peers may get old CommitIndex. it no matter, no one care
// whether followers'log is committed, only replicating logs in their logs queue is enough for leader and clients
// to which leader serves.
//
// Implicit Heartbeat: AE has the effect of explicit heartbeat
// Heartbeat's effect:
//  1. suppress other servers trigger election
//  2. attach leader's CommitIndex to followers to ACK Previous appended log entries
//
// "Log Matching Property" refers to section 5.3 of original paper
func (rf *Raft) syncLog(peer int) {
	rf.lock()

	// check if current leader can replicate this log into follower
	if rf.NextIndex[peer] <= rf.lastEntryIndex() && rf.lastEntryTerm() != rf.currentTerm {
		// it will breaks Leader Completeness Property if send log belongs to old leader
		rf.unlock()
		return
	}

	// Otherwise replicate leader log into follower or send heartbeat

	var (
		term = rf.currentTerm // need Double-Check after RPC

		/**Index of the will-sending suffix log
		 * Log[1:maxIdx-1]: the log that needs replicate into followers by leader
		 * Log[x:maxIdx-1]:
		 *		the log suffix that needs send to follower by leader in this loop,
		 * 	could be empty, then will send a heartbeat.
		 */
		maxIdx = rf.lastEntryIndex() + 1

		/**Offset in rf.logs array, not real index of log entry
		 * no need double check whether sended log entries was changed, because:
		 * 	 Double-checking current term after RPC represents that the leader did not malfunction,
		 * plus Leader never updates/deletes/inserts entry into the prefix of its own log.
		 * so sended log entris in leader's log are always the same as before.
		 *   Leader only change log in two cases: Appending new entries into its log after position rf.le;
		 * Compacting a prefix of applied logs into snapshot.
		 *
		 * rf.logs[start:end-1] represents a suffix of leader's log, sending the log suffix later.
		 * sending an implicit heartbeat if start == end
		 */
		start = rf.offset(rf.NextIndex[peer])
		end   = rf.le

		commitIndex = rf.CommitIndex
	)

	// query missing log entries of follower's log within the rf.logs[start:end]
	// if follower reply agreement, return
	for start > 0 {

		args := &AppendEntriesArgs{
			Term:         term,
			Entries:      rf.logs[start:end],
			PrevLogTerm:  rf.prevEntryTerm(start),
			PrevLogIndex: rf.prevEntryIndex(start),
			LeaderCommit: commitIndex,
		}

		rf.unlock()

		reply := AppendEntriesReply{}
		if ok := rf.sendAppendEntries(peer, args, &reply); !ok {
			return
		}

		rf.lock()
		// Double-Check if raft is still leader and alive
		// Double-Check if currentTerm is the same as original term before sending RPC
		// Double-Check if log[:maxIdx] have replicated by other goroutines (maybe next replication had run faster)
		//
		// Log's length maybe change after RPC, because snapshot can compact log
		if rf.killed() || rf.status != SLEADER || rf.currentTerm != term {
			// something changed, invalidate this reply
			rf.unlock()
			return
		}

		// check if detected higher term
		if reply.Term > term {
			rf.recvNewTerm(reply.Term)
			rf.unlock()
			return
		}

		// check if the append/overwrite operation is accepted by follower
		if reply.Success {
			// replicate Log[1:maxIdx-1] into peer node successfully
			rf.MatchIndex[peer] = maxIdx - 1
			rf.NextIndex[peer] = maxIdx
			rf.unlock()
			return
		} else {
			/**
			 * Grow the suffix on leader's log for next sending. if follower accepts it,
			 * represents that the suffix from leader's log and the prefix from follower's
			 * can constitute a log identical to leader's log.
			 *
			 * slower approach: growing suffix one by one
			 * faster approach: roll back the pointer start more at a time
			 */
			if reply.XLen <= args.PrevLogIndex {
				// follower's log is too short
				start = rf.offset(reply.XLen)
			} else {
				idx := -1
				// search for log entry with term=reply.XTerm
				for i := end - 1; i > 0; i-- {
					if rf.logs[i].Term < reply.XTerm { // no more, fast quit
						break
					} else if rf.logs[i].Term == reply.XTerm {
						idx = i
						break
					}
				}

				if idx == -1 {
					start = rf.offset(reply.XIndex)
				} else {
					start = idx + 1
				}
			}

			if start > 0 {
				// speed up subsequent AppendEntries in this term
				// if not, may fail tests due to the long time spent
				rf.NextIndex[peer] = rf.index(start)
			}
		}
	}

	rf.unlock()

	// check whether follower loses/lacks part of log entries that were compacted by leader
	if start <= 0 {
		// send snapshot if follower's log lags behind leader too much
		rf.sendSnapshot(peer)
		return
	}
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
		rf.lock()
		if rf.status != SLEADER {
			rf.unlock()
			return
		}

		// check if current log belongs to current leader by checking last log entry's term
		if rf.lastEntryTerm() != rf.currentTerm {
			// Leader can not commit previous term's log
			rf.unlock()
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
			if rf.logs[rf.offset(t)].Term == rf.currentTerm {
				// Leader replicates and commits only its own log
				rf.CommitIndex = t
				// apply committed log entries if rf.CommitIndex changed
				if rf.LastApplied < rf.CommitIndex {
					rf.applyCond.Broadcast() // wakeup
				}
			} else {
				// don't replicate/commit logs that belongs to Old Leader into followers.
				// maybe cause the newly elected Leader contains not all committed logs (Leader Completeness Property)
			}
		}

		rf.unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

// throw newly applied log entries up to upper layer application(namely state machine) by a go channel
// notice: go channel may blocks, don not hold lock to do time-spent operation
func (rf *Raft) send2StateMachine(applyCh chan ApplyMsg) {
	for !rf.killed() {

		if arr := rf.mq.pop(); len(arr) > 0 {
			for i := range arr {
				applyCh <- *(arr[i])
			}
		} else {
			rf.mq.wait() // wait new event
		}

		// time.Sleep(10*time.Millisecond) // busy wait not elegant
	}
}

func (rf *Raft) sendSnapshot(peer int) {

	rf.lock()
	var (
		term = rf.currentTerm // need Double-Check after RPC
		args = &InstallSnapshotArgs{
			Term:              term,
			LastIncludedTerm:  rf.snapshotTerm,
			LastIncludedIndex: rf.snapshotIndex,
			Snapshot:          rf.snapshot,
		}
	)
	rf.unlock()

	reply := InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(peer, args, &reply); !ok {
		return
	}

	rf.lock()
	defer rf.unlock()

	// check if higher term
	if reply.Term > rf.currentTerm {
		rf.recvNewTerm(reply.Term)
	}

	// Double-Check after RPC
	if rf.status != SLEADER || term != rf.currentTerm {
		return
	}
	// check reply's result
	if reply.Success {
		// update leader's states about the follower if need
		rf.NextIndex[peer] = args.LastIncludedIndex + 1
		rf.MatchIndex[peer] = args.LastIncludedIndex
	}

	// if fail, will try next time
}
