package idgen

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

type IDGenWorker interface {
	NextID() (i int64, err error)
}

type idGenWork struct {
	nodeMask int64
	lastMs   int64
	count    int64
	l        sync.Mutex
}

func NewWorker(nodeid int) (worker IDGenWorker, err error) {
	if nodeid < 0 || nodeid > MaxNodeID {
		err = errors.New("Node id is not allowed!")
		return
	}

	time.Sleep(time.Millisecond) // ensure program restart from crash is safe

	w := new(idGenWork)
	w.nodeMask = int64(nodeid) << nodeIDShift

	worker = w
	return
}

func (work *idGenWork) NextID() (i int64, err error) {
	work.l.Lock()
	defer work.l.Unlock()

	ms := getNowMs()
	if ms < work.lastMs {
		err = errors.New("time error, now is before last time")
		return
	}
	if ms > work.lastMs {
		work.count = 0
		work.lastMs = ms
	} else {
		work.count &= sequenceMask
		if work.count == 0 {
			for work.lastMs == ms {
				work.lastMs = getNowMs()
			}
		}
	}
	i = firstBitMask & ((ms-baseMs)<<msShift | work.nodeMask | work.count)
	work.count++
	return
}

func getNowMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
