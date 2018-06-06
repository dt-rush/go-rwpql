package rwpql

import (
	"fmt"
	"github.com/fatih/color"
	"sort"
	"sync"
	"time"
)

// OrderedLogger is used to print log messages in the order they
// were actually created, to make sure that things seem sensible when
// reading the test ouput
type OrderedLogger struct {
	logMutex sync.Mutex
	logLines []LogLine
	lastDump time.Time
	silent   bool
}

type LogLine struct {
	t int64
	s string
}

var Logger = &OrderedLogger{lastDump: time.Now()}

func (l *OrderedLogger) SetSilent(silent bool) {
	l.silent = silent
}

func (l *OrderedLogger) sortLineBuffer() {
	sort.Slice(l.logLines, func(i int, j int) bool {
		return l.logLines[i].t < l.logLines[j].t
	})
}

func (l *OrderedLogger) Log(line LogLine) {
	if !l.silent {
		l.logMutex.Lock()
		defer l.logMutex.Unlock()
		l.logLines = append(l.logLines, line)

		if len(l.logLines) > 10 {
			l.sortLineBuffer()
			for i := 0; i < 5; i++ {
				line, l.logLines = l.logLines[0], l.logLines[1:]
				fmt.Println(line.s)
			}
			l.lastDump = time.Now()
		}
	}
}

func (l *OrderedLogger) Flush() {
	for _, line := range l.logLines {
		l.sortLineBuffer()
		fmt.Println(line.s)
	}
}

func Log(t int64, s string, args ...interface{}) {
	Logger.Log(LogLine{t, fmt.Sprintf(s, args...)})
}

var rlock = color.New(color.FgCyan).SprintFunc()
var lock = color.New(color.FgRed).SprintFunc()
var plock = color.New(color.FgBlue).SprintFunc()
