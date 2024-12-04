package shardctrler

import "sort"

/* Implementation of Consistent Hasing (micro-shards opetimized version)
 * Gist:
 * 1.AddNode: insert k virtual nodes that belong to the new node into hash ring,
 * 			  and sort all node's position in the array.
 * 2.DelNode: delete the node's all k positions and re-sort position array.
 * 3.QueryNode: query(binary search) the node by position.
 *
 * Advantages:
 * 1.dynamically add/delete server node
 * 2.Minimize the Movements of data between server node when add/delete server node
 * 3.k-shards makes load more balanced
 *
 * BST may be more effecient than binary search
 * online judge: https://www.lintcode.com/problem/520/
 */

// ConsHash: consistent hashing
// Deprecated: can not be used in this lab, because tester strictly ask for replica-group has the same number of shards
type ConsHash struct {
	N         int         // hash ring range [0, N-1]
	K         int         // the number of position of server on the ring
	Pos2node  map[int]int // position to node's ID
	Positions []int       // all node's positions on the ring. Sorted
}

// constructor
func CreateConsHash(n, k int) *ConsHash {
	return &ConsHash{
		N:         n,
		K:         k,
		Pos2node:  make(map[int]int),
		Positions: make([]int, 0),
	}
}

// add a real node into hash ring. return the positions assigned to nodeId
func (ch *ConsHash) AddNode(nodeIds []int, seed int64) (ret []int) {
	if len(ch.Positions) >= ch.N {
		panic("AddNode error: full-filled")
	}
	if len(nodeIds) == 0 {
		return
	}
	rd := drand(seed)
	// insert k virtual nodes for the one node
	for _, nodeId := range nodeIds {
		for i := 0; i < ch.K; {
			pos := int(rd.Int63n(int64(ch.N)))
			if _, ok := ch.Pos2node[pos]; ok {
				continue
			}
			i++
			ch.Pos2node[pos] = nodeId
			ch.Positions = append(ch.Positions, pos)
			ret = append(ret, pos)
		}
	}

	// sort positions
	sort.Slice(ch.Positions, func(i, j int) bool {
		return ch.Positions[i] < ch.Positions[j]
	})

	return
}

// delete a node on the hash ring.
func (ch *ConsHash) DelNode(nodeIds []int) {
	if len(nodeIds) == 0 {
		return
	}

	delPos := make(map[int]bool)
	delNode := make(map[int]bool)
	for _, nodeId := range nodeIds {
		delNode[nodeId] = true
	}
	for pos, id := range ch.Pos2node {
		if delNode[id] {
			delete(ch.Pos2node, pos)
			delPos[pos] = true
		}
	}

	// delete and re-sort from positions array
	newPosition := make([]int, 0)
	for _, pos := range ch.Positions {
		if !delPos[pos] {
			newPosition = append(newPosition, pos)
		}
	}
	ch.Positions = newPosition
	sort.Slice(ch.Positions, func(i, j int) bool {
		return ch.Positions[i] < ch.Positions[j]
	})
}

// query node which the key should be assigned to by pos on the ring
func (ch *ConsHash) QueryNode(pos int) int {
	l, r := 0, ch.N-1
	for l < r {
		m := l + (r-l)/2
		if ch.Positions[m] == pos {
			return ch.Pos2node[pos]
		} else if ch.Positions[m] > pos {
			r = m
		} else {
			l = m + 1
		}
	}
	if ch.Positions[r] < pos {
		return ch.Pos2node[ch.Positions[0]]
	} else {
		return ch.Pos2node[ch.Positions[r]]
	}
}

// special for this lab
func (ch *ConsHash) GetHashRing() [NShards]int {
	if len(ch.Positions) == 0 {
		return [NShards]int{}
	}
	arr := [NShards]int{}

	m := len(ch.Positions)
	idx, nx_idx := 0, 1%m // server node's position index
	node := ch.Pos2node[ch.Positions[nx_idx]]
	pos := (ch.Positions[idx] + 1) % ch.N
	for i := 0; i < ch.N; i++ {
		arr[pos] = node
		if pos == ch.Positions[nx_idx] {
			idx = (idx + 1) % m
			nx_idx = (nx_idx + 1) % m
			node = ch.Pos2node[ch.Positions[nx_idx]]
		}
		pos = (pos + 1) % ch.N
	}

	return arr
}
