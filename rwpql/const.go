package rwpql

const (
	OPEN                   = uint32(iota)
	RLOCKED                = uint32(iota)
	LOCK_RESERVED          = uint32(iota)
	LOCKED                 = uint32(iota)
	PRIORITY_LOCK_RESERVED = uint32(iota)
	PRIORITY_LOCKED        = uint32(iota)
)
