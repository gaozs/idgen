package uintgen

import (
	"errors"
	"sync"
	"time"
)

// 0-13  bit: 14 bits are serial number
// 14-18 bit: 5 bits are node id
// 19-62 bit: 44 bits are ms
// 63    bit: is always 0

const (
	sequenceBits = 14
	nodeIDBits   = 5

	MaxNodeID = 1<<nodeIDBits - 1

	nodeIDShift = sequenceBits
	msShift     = sequenceBits + nodeIDBits

	sequenceMask = int64(1<<sequenceBits - 1)
	firstBitMask = int64(uint64(1)<<63 - 1)
)

// for reduce ms part
var baseMs = time.Date(2010, 9, 13, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)

type UIntGenWorker interface {
	NextInt() (i int64, err error)
}

type uIntGenWork struct {
	nodeMask int64
	lastMs   int64
	count    int64
	lock     sync.Mutex
}

func NewWorker(nodeid int) (worker UIntGenWorker, err error) {
	if nodeid < 0 || nodeid > MaxNodeID {
		err = errors.New("Node id is not allowed!")
		return
	}

	time.Sleep(time.Millisecond) // ensure program restart from crash is safe

	w := new(uIntGenWork)
	w.nodeMask = int64(nodeid) << nodeIDShift

	worker = w
	return
}

func (worker *uIntGenWork) NextInt() (i int64, err error) {
	worker.lock.Lock()
	defer worker.lock.Unlock()

	ms := getNowMs()
	if ms < worker.lastMs {
		err = errors.New("time error, now is before last time")
	}
	if ms > worker.lastMs {
		i = firstBitMask & ((ms-baseMs)<<msShift | worker.nodeMask)
		worker.lastMs = ms
		worker.count = 1
		return
	}

	worker.count &= sequenceMask
	if worker.count == 0 {
		for ms == worker.lastMs {
			ms = getNowMs()
		}
		worker.lastMs = ms
	}
	i = firstBitMask & ((ms-baseMs)<<msShift | worker.nodeMask | worker.count)
	worker.count++
	return
}

func getNowMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
