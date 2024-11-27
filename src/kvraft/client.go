package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientKey string // identify unique client. as a part of idempotent key
	// requestId int64

	currentLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientKey = fmt.Sprintf("%p", ck)
	ck.currentLeader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	req := GetArgs{
		Key:       key,
		ClientKey: ck.clientKey,
		RequestId: nrand(),
	}
	for {
		for i := 0; i < len(ck.servers); i++ {
			svr := (ck.currentLeader + i) % len(ck.servers)
			rsp := GetReply{}
			ok := ck.servers[svr].Call("KVServer.Get", &req, &rsp)

			if ok && rsp.Err == OK {
				ck.currentLeader = svr
				return rsp.Value
			} else if rsp.Err == ErrWrongLeader || rsp.Err == ErrTimeout ||
				len(rsp.Err) == 0 || !ok {
				continue // try next server
			} else {
				panic(fmt.Sprintf("unknown Err(%v) in Get", rsp.Err))
			}
		}
		// sleep a period of raft election-timeout
		time.Sleep(100 * time.Millisecond)
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	req := PutAppendArgs{
		Key:       key,
		Value:     value,
		ClientKey: ck.clientKey,
		RequestId: nrand(),
	}
	for {
		for i := 0; i < len(ck.servers); i++ {
			svr := (ck.currentLeader + i) % len(ck.servers)
			rsp := PutAppendReply{}

			ok := ck.servers[svr].Call("KVServer."+op, &req, &rsp)

			if ok && rsp.Err == OK {
				ck.currentLeader = svr
				return
			} else if rsp.Err == ErrWrongLeader || rsp.Err == ErrTimeout ||
				len(rsp.Err) == 0 || !ok {
				continue // try next server
			} else {
				panic(fmt.Sprintf("unknown Err(%v) in PutAppend", rsp.Err))
			}
		}
		// sleep a period of raft election-timeout
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
