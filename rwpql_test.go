package rwpql

import (
	"flag"
	"fmt"
	"go.uber.org/atomic"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/dt-rush/go-rwpql/rwpql"
)

// flags
var profile = flag.Bool("profile", false,
	"whether to run a cpuprofile and output a .pprof file")
var silent = flag.Bool("silent", false,
	"whether to silence output from the lock's logger")
var arrayBased = flag.Bool("arraybased", false,
	"whether to use an array-based lock")

// constants used for testing
const QUEUE_SZ = 4
const LOCK_SLEEP_NS = 1000
const SLEEP = time.Microsecond

// used to abstract over the two locks (one array-based, the other
// based on a traditional spinlock where all waiters watch one
// variable with atomic load / compare-and-swap
type RWPQLocker interface {
	RLock()
	Lock() uint32
	PLock()

	RUnlock()
	Unlock(uint32)
	PUnlock()
}

// used to test the lock and keep track of how many locks of each
// type it was able to serve
type TestRunner struct {
	L RWPQLocker

	locks  atomic.Uint32
	rlocks atomic.Uint32
	plocks atomic.Uint32

	stopFlag atomic.Uint32
	wg       sync.WaitGroup
}

// repeatedly tries to RLock()
func (t *TestRunner) RLocker() {
	t.wg.Add(1)
	go func() {
		for t.stopFlag.Load() != 1 {
			t.L.RLock()
			t.rlocks.Inc()
			time.Sleep(SLEEP)
			t.L.RUnlock()
			time.Sleep(SLEEP * 30)
		}
		t.wg.Done()
	}()
}

// repeatedly tries to Lock()
func (t *TestRunner) Locker() {
	t.wg.Add(1)
	go func() {
		for t.stopFlag.Load() != 1 {
			ticket := t.L.Lock()
			t.locks.Inc()
			time.Sleep(SLEEP)
			t.L.Unlock(ticket)
			time.Sleep(SLEEP * 100)
		}
		t.wg.Done()
	}()
}

// repeatedly tries to PLock()
func (t *TestRunner) PLocker() {
	t.wg.Add(1)
	go func() {
		for t.stopFlag.Load() != 1 {
			t.L.PLock()
			t.plocks.Inc()
			time.Sleep(SLEEP * 3)
			t.L.PUnlock()
			time.Sleep(SLEEP * 10000)
		}
		t.wg.Done()
	}()
}

// Stop the test
func (t *TestRunner) Stop() {
	t.stopFlag.Store(1)
	t.wg.Wait()
}

func TestRWPQL(t *testing.T) {

	// parse flags
	if *profile {
		f, err := os.Create("main.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	rwpql.Logger.SetSilent(*silent)

	// build the test runner
	T := TestRunner{}
	if *arrayBased {
		T.L = rwpql.NewABRWPQL(QUEUE_SZ, LOCK_SLEEP_NS)
	} else {
		T.L = rwpql.NewRWPQL(LOCK_SLEEP_NS)
	}

	// test params
	rlockers := 64
	lockers := 32
	plockers := 2

	// start the RLockers
	go func() {
		for i := 0; i < rlockers; i++ {
			T.RLocker()
			time.Sleep(SLEEP / 4)
		}
	}()
	// start the Lockers
	go func() {
		for i := 0; i < lockers; i++ {
			T.Locker()
			time.Sleep(SLEEP / 4)
		}
	}()
	// start the PLockers
	go func() {
		for i := 0; i < plockers; i++ {
			T.PLocker()
			time.Sleep(SLEEP * 16)
		}
	}()

	// sleep 30 seconds then stop the goroutines
	time.Sleep(30 * time.Second)
	T.Stop()
	// flush the ordered logger
	rwpql.Logger.Flush()
	// print the statistics
	fmt.Printf("%d rlocks, %d locks, %d plocks\n", T.rlocks, T.locks, T.plocks)
}
