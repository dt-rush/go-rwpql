package rwpql

import (
	"go.uber.org/atomic"
	"time"
)

type RWPQL struct {
	sleep_ns        time.Duration
	lockState       *atomic.Uint32
	ticket          *atomic.Uint32
	dequeueCount    *atomic.Uint32
	nReaders        *atomic.Uint32
	priorityWaiting *atomic.Uint32
}

func NewRWPQL(sleep_ns int) *RWPQL {
	return &RWPQL{
		sleep_ns:        time.Duration(sleep_ns),
		lockState:       atomic.NewUint32(OPEN),
		ticket:          atomic.NewUint32(0),
		dequeueCount:    atomic.NewUint32(0),
		nReaders:        atomic.NewUint32(0),
		priorityWaiting: atomic.NewUint32(0)}
}

func (l *RWPQL) RLock() {

	ticket := l.ticket.Inc() - 1
	Log(time.Now().UnixNano(), rlock("RLock [%d] waiting..."), ticket)
	for {
		for l.dequeueCount.Load() != ticket {
			time.Sleep(l.sleep_ns)
		}
		for !(l.lockState.CAS(OPEN, RLOCKED) || l.lockState.CAS(RLOCKED, RLOCKED)) {
			time.Sleep(l.sleep_ns)
		}
		if l.priorityWaiting.Load() > 0 {
			Log(time.Now().UnixNano(), rlock("RLock [%d] yielding to priority locker..."), ticket)
			l.lockState.Store(PRIORITY_LOCK_RESERVED)
			continue
		} else {
			l.dequeueCount.Inc()
			l.nReaders.Inc()
			Log(time.Now().UnixNano(), rlock("RLock acquired [%d]"), ticket)
			return
		}
	}
}

func (l *RWPQL) Lock() uint32 {

	ticket := l.ticket.Inc() - 1
	Log(time.Now().UnixNano(), lock("Lock [%d] waiting..."), ticket)
	for {
		for l.dequeueCount.Load() != ticket {
			time.Sleep(l.sleep_ns)
		}
		for !(l.lockState.CAS(OPEN, LOCKED)) {
			time.Sleep(l.sleep_ns)
		}
		if l.priorityWaiting.Load() > 0 {
			Log(time.Now().UnixNano(), rlock("Lock [%d] yielding to priority locker..."), ticket)
			l.lockState.Store(PRIORITY_LOCK_RESERVED)
			continue
		} else {
			Log(time.Now().UnixNano(), lock("Lock acquired [%d]"), ticket)
			return ticket
		}
	}
}

func (l *RWPQL) PLock() {

	l.priorityWaiting.Inc()
	Log(time.Now().UnixNano(), plock("PLock waiting..."))
	for !(l.lockState.CAS(OPEN, LOCKED) ||
		l.lockState.CAS(PRIORITY_LOCK_RESERVED, LOCKED)) {
		time.Sleep(l.sleep_ns)
	}
	Log(time.Now().UnixNano(), plock("PLock acquired..."))
	l.priorityWaiting.Dec()
}

func (l *RWPQL) RUnlock() {
	nReadersRemaining := l.nReaders.Dec()
	Log(time.Now().UnixNano(), rlock("-------------------- RUnlock (%d remaining)"),
		nReadersRemaining)
	if nReadersRemaining == 0 {
		Log(time.Now().UnixNano(), "LOCK: OPEN...")
		l.lockState.Store(OPEN)
	}
}

func (l *RWPQL) Unlock(ticket uint32) {
	Log(time.Now().UnixNano(), lock("-------------------- Unlock"))
	Log(time.Now().UnixNano(), "LOCK: OPEN...")
	l.lockState.Store(OPEN)
	l.dequeueCount.Inc()
}

func (l *RWPQL) PUnlock() {
	Log(time.Now().UnixNano(), plock("-------------------- PUnlock"))
	Log(time.Now().UnixNano(), "LOCK: OPEN...")
	l.lockState.Store(OPEN)
}
