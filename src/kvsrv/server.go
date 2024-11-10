package kvsrv

import (
	"fmt"
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu    sync.Mutex
	cache map[string]string

	// map<client-id, <request-id,result>>, for identifing duplicate request and return request result for subsequent duplicate request
	// base on the assumption that a client send a request at a time, no concurrent requests
	idempotentMap map[string]requestResult
}

type requestResult struct {
	requestId int64
	result    string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.cache[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if exist, ret := kv.idempotent(args); exist {
		reply.Value = ret
		return
	}
	kv.cache[args.Key] = args.Value

	// maintain idempotent key
	kv.idempotentMap[args.ClientKey] = requestResult{
		requestId: args.RequestKey,
		result:    reply.Value,
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if exist, ret := kv.idempotent(args); exist {
		reply.Value = ret
		return
	}
	reply.Value = kv.cache[args.Key]
	kv.cache[args.Key] = fmt.Sprintf("%s%s", reply.Value, args.Value)

	// maintain idempotent key
	kv.idempotentMap[args.ClientKey] = requestResult{
		requestId: args.RequestKey,
		result:    reply.Value,
	}
}

// check if request is duplicated.
// must get kv.mu lock before call this function
func (kv *KVServer) idempotent(args *PutAppendArgs) (bool, string) {
	if reqrst, ok := kv.idempotentMap[args.ClientKey]; ok && reqrst.requestId == args.RequestKey {
		return true, reqrst.result
	}
	return false, ""
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.cache = make(map[string]string)
	kv.idempotentMap = make(map[string]requestResult)
	return kv
}
