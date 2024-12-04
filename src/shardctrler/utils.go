package shardctrler

import (
	"encoding/json"

	"math/rand"
)

func deepCopyMap(src map[int][]string) (dst map[int][]string) {
	dst = make(map[int][]string, len(src))
	for k, v := range src {
		dst[k] = deepCopyStrArr(v)
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

func getMapByArr(arr []int) map[int]bool {
	m := make(map[int]bool)
	for _, v := range arr {
		m[v] = true
	}
	return m
}

// a0 + a1 and distinct
func addArr(a0, a1 []int) []int {
	m := getMapByArr(a0)
	for _, v := range a1 {
		if !m[v] {
			a0 = append(a0, v)
			m[v] = true
		}
	}
	return a0
}

// a0 - a1 and distinct
func subArr(a0, a1 []int) []int {
	m := getMapByArr(a1)
	ret := make([]int, 0)
	for _, v := range a0 {
		if !m[v] {
			ret = append(ret, v)
		}
	}
	return ret
}

// deterministic
func drand(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

// only for Debug
func obj2json(obj interface{}) string {
	s, e := json.Marshal(obj)
	if e != nil {
		panic("obj2json")
	}
	return string(s)
}
