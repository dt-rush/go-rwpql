package rwpql

import (
	"go.uber.org/atomic"
	"time"
)

type ABRWPQL struct {
	arr      []int
	queue_sz int

	sleep_ns        time.Duration
	lockState       *atomic.Uint32
	ticket          *atomic.Uint32
	dequeueCount    *atomic.Uint32
	nReaders        *atomic.Uint32
	priorityWaiting *atomic.Uint32
}

func NewABRWPQL(queue_sz int, sleep_ns int) *ABRWPQL {
	abql := ABRWPQL{
		arr:             make([]int, queue_sz),
		queue_sz:        queue_sz,
		sleep_ns:        time.Duration(sleep_ns),
		lockState:       atomic.NewUint32(OPEN),
		ticket:          atomic.NewUint32(0),
		dequeueCount:    atomic.NewUint32(0),
		nReaders:        atomic.NewUint32(0),
		priorityWaiting: atomic.NewUint32(0)}
	abql.arr[0] = 1
	return &abql
}

func (l *ABRWPQL) yieldToPriorityLock() {
	l.lockState.Store(PRIORITY_LOCK_RESERVED)
	for l.lockState.Load() != OPEN {
		time.Sleep(l.sleep_ns)
	}
}

func (l *ABRWPQL) RLock() {
	ticket := l.ticket.Inc() - 1
	Log(time.Now().UnixNano(), rlock("RLock [%d] waiting..."), ticket)
	for ticket-l.dequeueCount.Load() >= uint32(l.queue_sz) {
		time.Sleep(l.sleep_ns)
	}
	for l.arr[ticket%uint32(l.queue_sz)] != 1 {
		time.Sleep(l.sleep_ns)
	}
	for {
		for !(l.lockState.Load() == RLOCKED || l.lockState.CAS(OPEN, RLOCKED)) {
			time.Sleep(l.sleep_ns)
		}
		if l.priorityWaiting.Load() > 0 {
			Log(time.Now().UnixNano(), rlock("RLock [%d] yielding to priority locker..."),
				ticket)
			l.yieldToPriorityLock()
		} else {
			l.nReaders.Inc()
			// move the queue forward after incrementing nReaders, so that
			// either another call to RLock() can get the queue head or else
			// a call to Lock() can get the queue head but wait for nReaders = 0
			l.arr[int(ticket)%l.queue_sz] = 0
			l.arr[int(ticket+1)%l.queue_sz] = 1
			l.dequeueCount.Inc()
			Log(time.Now().UnixNano(), rlock("RLock acquired [%d]"), ticket)
			return
		}
	}
}

func (l *ABRWPQL) Lock() uint32 {
	ticket := l.ticket.Inc() - 1
	Log(time.Now().UnixNano(), lock("Lock [%d] waiting..."), ticket)
	for ticket-l.dequeueCount.Load() >= uint32(l.queue_sz) {
		time.Sleep(l.sleep_ns)
	}
	for l.arr[ticket%uint32(l.queue_sz)] != 1 {
		time.Sleep(l.sleep_ns)
	}
	for l.nReaders.Load() > 0 {
		time.Sleep(l.sleep_ns)
	}
	for {
		for !l.lockState.CAS(OPEN, LOCKED) {
			time.Sleep(l.sleep_ns)
		}
		if l.priorityWaiting.Load() > 0 {
			Log(time.Now().UnixNano(), lock("Lock [%d] yielding to priority locker..."),
				ticket)
			l.yieldToPriorityLock()
		} else {
			Log(time.Now().UnixNano(), lock("Lock acquired [%d]"), ticket)
			return ticket
		}
	}
}

func (l *ABRWPQL) PLock() {
	l.priorityWaiting.Inc()
	Log(time.Now().UnixNano(), plock("PLock waiting..."))
	for !(l.lockState.CAS(OPEN, PRIORITY_LOCKED) ||
		l.lockState.CAS(PRIORITY_LOCK_RESERVED, PRIORITY_LOCKED)) {
		time.Sleep(l.sleep_ns)
	}
	Log(time.Now().UnixNano(), plock("PLock acquired."))
	l.priorityWaiting.Dec()
}

func (l *ABRWPQL) RUnlock() {
	// decrement nReaders and store open if none left
	readersRemaining := l.nReaders.Dec()
	Log(time.Now().UnixNano(), rlock("-------------------- RUnlock (%d remaining)"), readersRemaining)
	if readersRemaining == 0 {
		Log(time.Now().UnixNano(), "LOCK: OPEN...")
		l.lockState.Store(OPEN)
	}
}

func (l *ABRWPQL) Unlock(ticket uint32) {
	Log(time.Now().UnixNano(), lock("-------------------- Unlock [%d]"), ticket)
	l.arr[int(ticket)%l.queue_sz] = 0
	l.arr[int(ticket+1)%l.queue_sz] = 1
	Log(time.Now().UnixNano(), "LOCK: OPEN...")
	l.lockState.Store(OPEN)
	l.dequeueCount.Inc()
}

func (l *ABRWPQL) PUnlock() {
	Log(time.Now().UnixNano(), plock("-------------------- PUnlock"))
	Log(time.Now().UnixNano(), "LOCK: OPEN...")
	l.lockState.Store(OPEN)
}
