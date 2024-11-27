package kvraft

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
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
func printOp(op Op) string {
	switch op.Typ {
	case OP_GET:
		return fmt.Sprintf("Get(%v)[%v,%v]", op.Key, op.ClientKey, op.RequestId)
	case OP_PUT:
		return fmt.Sprintf("PUT(%v)[%v,%v] value=%v", op.Key, op.ClientKey, op.RequestId, op.Value)
	case OP_APPEND:
		return fmt.Sprintf("APPEND(%v)[%v,%v] value=%v", op.Key, op.ClientKey, op.RequestId, op.Value)
	}
	return "unknown"
}

// only for Debug
func mtimer(s string) func() {
	t := time.Now()
	return func() {
		DPrintf("%s consume %vms\n", s, time.Now().Sub(t).Milliseconds())
	}
}
