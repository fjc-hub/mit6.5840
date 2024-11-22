package raft

import "sync"

/* 1.non-block dynamic message-queue, auto scaling the size
 * 2.using Condition Variable to coordinates Producers and Consumers
 */

type MQ struct {
	msg  []*ApplyMsg
	cond *sync.Cond
}

// constructor of MQ
func createMq() *MQ {
	l := new(sync.Mutex) // for avoiding lost-wakeup
	return &MQ{
		cond: sync.NewCond(l),
		msg:  make([]*ApplyMsg, 0),
	}
}

// push message
func (mq *MQ) push(m ...*ApplyMsg) {
	mq.cond.L.Lock()
	mq.msg = append(mq.msg, m...)
	mq.cond.L.Unlock()

	mq.cond.Broadcast() // wake up consumers
}

// pop a batch of messages
func (mq *MQ) pop() (ret []*ApplyMsg) {
	mq.cond.L.Lock()

	if len(mq.msg) > 0 {
		ret = mq.msg
		mq.msg = make([]*ApplyMsg, 0)
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
