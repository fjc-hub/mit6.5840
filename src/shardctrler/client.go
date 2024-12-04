package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	clientKey     string
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

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:       num,
		ClerkKey:  ck.clientKey,
		RequestId: int(nrand()),
	}
	for {
		// try each known server.
		n := len(ck.servers)
		for i := 0; i < n; i++ {
			t := (ck.currentLeader + i) % n
			srv := ck.servers[t]
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.currentLeader = t
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:   servers,
		ClerkKey:  ck.clientKey,
		RequestId: int(nrand()),
	}

	for {
		// try each known server.
		n := len(ck.servers)
		for i := 0; i < n; i++ {
			t := (ck.currentLeader + i) % n
			srv := ck.servers[t]
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.currentLeader = t
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:      gids,
		ClerkKey:  ck.clientKey,
		RequestId: int(nrand()),
	}

	for {
		// try each known server.
		n := len(ck.servers)
		for i := 0; i < n; i++ {
			t := (ck.currentLeader + i) % n
			srv := ck.servers[t]
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.currentLeader = t
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClerkKey:  ck.clientKey,
		RequestId: int(nrand()),
	}

	for {
		// try each known server.
		n := len(ck.servers)
		for i := 0; i < n; i++ {
			t := (ck.currentLeader + i) % n
			srv := ck.servers[t]
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.currentLeader = t
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
