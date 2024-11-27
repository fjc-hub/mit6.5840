package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// randomized election timeout
func randTimestamp() int64 {
	return time.Now().UnixMilli() + 200 + (rand.Int63() % 200)
}

func copyIntSlice(src []int, exceptIdx int) (ret []int) {
	if exceptIdx < 0 || len(src) <= exceptIdx {
		return
	}
	for i := range src {
		if exceptIdx == i {
			continue
		}
		ret = append(ret, src[i])
	}
	return
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

// only for Debug
func obj2json(obj interface{}) string {
	s, e := json.Marshal(obj)
	if e != nil {
		panic("obj2json")
	}
	return string(s)
}

// only for Debug
/*
for i := range cfg.rafts {
	cfg.t.Logf("%v\n", raft2string(cfg.rafts[i]))
}
*/
func Raft2string(rf *Raft) string {
	return fmt.Sprintf("raft(%v;%v;%v) Logs=%v (%v,%v); %v %v (%v %v)",
		rf.me, rf.currentTerm, rf.status,
		serializeLogsCmd(rf.logs),
		rf.LastApplied, rf.CommitIndex,
		rf.NextIndex, rf.MatchIndex,
		rf.snapshotTerm, rf.snapshotIndex)
}

// only for Debug
func insArgs(args *InstallSnapshotArgs) string {
	return fmt.Sprintf("%v,%v,%v", args.Term, args.LastIncludedTerm, args.LastIncludedIndex)
}

// only for Debug
func serializeLogs(logs []LogEntry) string {
	var b strings.Builder
	for i := range logs {
		if i == len(logs)-1 {
			b.WriteString(fmt.Sprintf("%v", logs[i].Term))
		} else {
			b.WriteString(fmt.Sprintf("%v,", logs[i].Term))
		}
	}
	return fmt.Sprintf("[%s]", b.String())
}

// only for Debug
func serializeLogsCmd(logs []LogEntry) string {
	var b strings.Builder
	for i := range logs {
		if i == len(logs)-1 {
			b.WriteString(fmt.Sprintf("%v", logs[i].Command))
		} else {
			b.WriteString(fmt.Sprintf("%v,", logs[i].Command))
		}
	}
	return fmt.Sprintf("[%s]", b.String())
}

// only for Debug
func mtimer(s string) func() {
	t := time.Now()
	return func() {
		DPrintf("%s consume %vms\n", s, time.Now().Sub(t).Milliseconds())
	}
}
