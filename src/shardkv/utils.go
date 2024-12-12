package shardkv

import (
	"encoding/json"
	"fmt"

	"6.5840/shardctrler"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

func createSMOP(op *Op, snap []byte) *SMOP {
	return &SMOP{
		Op:       *deepCopyOp(op),
		Snapshot: snap,
	}
}

func deepCopyOp(op *Op) *Op {
	return &Op{
		Typ:       op.Typ,
		Key:       op.Key,
		Value:     op.Value,
		ClientKey: op.ClientKey,
		RequestId: op.RequestId,
		NewShard:  deepCopyGetShardArgs(&op.NewShard),
		ReConfig:  deepCopyReConfigLog(&op.ReConfig),
	}
}

func deepCopyGetShardArgs(t *GetShardArgs) GetShardArgs {
	return GetShardArgs{
		SID:     t.SID,
		From:    t.From,
		Version: t.Version,
		Shard:   deepCopyMap2(t.Shard),
		Cstate:  deepCopyMap3(t.Cstate),
	}
}

func deepCopyReConfigLog(t *ReConfigLog) ReConfigLog {
	return ReConfigLog{
		Shard:   t.Shard,
		To:      t.To,
		ToSvrs:  deepCopyStrArr(t.ToSvrs),
		Version: t.Version,
	}
}

func deepCopyMap(src map[int][]string) (dst map[int][]string) {
	if src == nil {
		return
	}
	dst = make(map[int][]string, len(src))
	for k, v := range src {
		dst[k] = deepCopyStrArr(v)
	}
	return
}

func deepCopyMap2(src map[string]string) (dst map[string]string) {
	if src == nil {
		return
	}
	dst = make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return
}

func createMapArr2() (ret [shardctrler.NShards]map[string]string) {
	for i := 0; i < shardctrler.NShards; i++ {
		ret[i] = map[string]string{}
	}
	return
}

func deepCopyMap3(src map[string]Clerkstate) (dst map[string]Clerkstate) {
	if src == nil {
		return
	}
	dst = make(map[string]Clerkstate, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return
}

func createMapArr3() (ret [shardctrler.NShards]map[string]Clerkstate) {
	for i := 0; i < shardctrler.NShards; i++ {
		ret[i] = map[string]Clerkstate{}
	}
	return
}

func deepCopyStrArr(src []string) (dst []string) {
	dst = make([]string, len(src))
	copy(dst, src)
	return
}

func deepCopyIntArr(src []int) (dst []int) {
	dst = make([]int, len(src))
	copy(dst, src)
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

func printConfig(cfg shardctrler.Config) string {
	return fmt.Sprintf("{%v %v}", cfg.Num, cfg.Shards)
}
