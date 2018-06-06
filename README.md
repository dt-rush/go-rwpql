RWPQL
===

exploring the implementation of a read-write-priority queueing lock in Go

(a queueing lock which acts like an RWMutex except it also provides PLock() for
priority lockers to jump the queue)
