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
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// randomized election timeout
func randTimestamp() int64 {
	return time.Now().UnixMilli() + 200 + (rand.Int63() % 300)
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
func raft2string(rf *Raft) string {
	return fmt.Sprintf("raft(%v;%v;%v) Logs=%v (%v,%v); %v %v",
		rf.me, rf.currentTerm, rf.status,
		serializeLogs(rf.logs),
		rf.CommitIndex, rf.LastApplied,
		rf.NextIndex, rf.MatchIndex)
}

func serializeLogs(logs []*LogEntry) string {
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
