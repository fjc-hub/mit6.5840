package shardkv

import (
	"bytes"
	"sync"

	"6.5840/labgob"
)

/* 1.non-block dynamic message-queue, auto scaling the size
 * 2.using Condition Variable to coordinates Producers and Consumers
 */

type MQ struct {
	msg  []*SMOP
	cond *sync.Cond
}

// constructor of MQ
func createMq(snapshot []byte) *MQ {
	l := new(sync.Mutex) // for avoiding lost-wakeup
	mq := &MQ{
		cond: sync.NewCond(l),
		msg:  make([]*SMOP, 0),
	}

	if len(snapshot) == 0 {
		return mq
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var msg []*SMOP

	if d.Decode(&msg) != nil {
		panic("createMq Decode error")
	} else {
		mq.msg = msg
	}

	return mq
}

// push message
func (mq *MQ) push(m ...*SMOP) {
	mq.cond.L.Lock()
	mq.msg = append(mq.msg, m...)
	mq.cond.L.Unlock()

	mq.cond.Broadcast() // wake up consumers
}

// pop a batch of messages
func (mq *MQ) pop() (ret *SMOP) {
	mq.cond.L.Lock()

	if len(mq.msg) > 0 {
		ret = mq.msg[0]
		mq.msg = mq.msg[1:]
	}

	mq.cond.L.Unlock()
	return
}

// wait the event that there are new msg in MQ
func (mq *MQ) wait() {
	mq.cond.L.Lock()
	for len(mq.msg) == 0 {
		mq.cond.Wait()
	}
	mq.cond.L.Unlock()
}

func (mq *MQ) snapshot() []byte {
	mq.cond.L.Lock()
	defer mq.cond.L.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(mq.msg)

	return w.Bytes()
}
