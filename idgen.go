package idgen

import (
	"errors"
	"sync"
	"time"
)

// bit 0 to next sequenceBits-1 bit: serial number length
// bit sequenceBits to next nodeIDBits-1 bit: node id length
// bit sequenceBits+nodeIDBits to bit 62: ms length
// bit 63: always 0

// to ensure first bit is 0
const firstBitMask = int64(uint64(1)<<63 - 1)

// for reduce ms part, worker server time must after this base time(2010-9-13 12:00pm UTC)
var baseMs = time.Date(2010, 9, 13, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)

// Worker which generates IDs
type Worker interface {
	NextID() (i int64, err error)
	NextIDMust() (i int64)
	MaxNodeID() (maxNodeID int)
}

type idGenWork struct {
	// config info
	sequenceBits int // must >=12
	nodeIDBits   int // must >=1
	nodeID       int // must not great than maxnode based nodeIDBits

	// based on config info for gen id
	sequenceMask int64
	nodeMask     int64
	msShift      uint
	maxNodeID    int

	// used to gen id
	lastMs int64
	count  int64
	sync.Mutex
}

// NewWorker generate a new worker
// nodeID is the caller's ID, from 0 to 2^nodeIDBits-1
// sequenceBits must >=12 and nodeIDBits must >=1
// sequenceBits+nodeIDBits must <= 20 (which mean ms has 43bit+, totally has about 278+ years range)
// sequenceBits,nodeIDBits can be 0,default value will be 14,5
func NewWorker(nodeID, sequenceBits, nodeIDBits int) (worker Worker, err error) {
	if sequenceBits == 0 {
		sequenceBits = 14
	}
	if nodeIDBits == 0 {
		nodeIDBits = 5
	}

	if sequenceBits < 12 {
		err = errors.New("sequenceBits is not allowed. Must >= 12")
		return
	}
	if nodeIDBits < 1 {
		err = errors.New("nodeIDBits is not allowed. Must >= 1")
		return
	}
	if sequenceBits+nodeIDBits > 20 {
		err = errors.New("sequenceBits+nodeIDBits is not allowed. Must <= 20")
		return
	}
	if nodeID < 0 || nodeID > (1<<uint(nodeIDBits)-1) {
		err = errors.New("Node id is not allowed! Less than 0 or great then max nodes")
		return
	}

	// ensure program restart from last crash is safe
	time.Sleep(time.Millisecond)

	w := new(idGenWork)

	w.sequenceBits = sequenceBits
	w.nodeIDBits = nodeIDBits
	w.nodeID = nodeID

	w.sequenceMask = 1<<uint(sequenceBits) - 1
	w.nodeMask = int64(nodeID) << uint(sequenceBits)
	w.msShift = uint(sequenceBits + nodeIDBits)
	w.maxNodeID = 1<<uint(nodeIDBits) - 1

	worker = w
	return
}

func (work *idGenWork) NextIDMust() (i int64) {
	var err error
	i, err = work.NextID()
	if err != nil {
		panic(err)
	}
	return
}

func (work *idGenWork) NextID() (i int64, err error) {
	work.Lock()
	defer work.Unlock()

	ms := time.Now().UnixNano() / int64(time.Millisecond)
	if ms < work.lastMs {
		if work.lastMs-ms <= 200 {
			// to handle time adjust within 200ms
			time.Sleep(time.Duration(work.lastMs - ms))
			ms = time.Now().UnixNano() / int64(time.Millisecond)
		} else {
			err = errors.New("time error, now is before last time")
			return
		}
	}
	if ms < baseMs {
		err = errors.New("time error, now is before base time(2010-9-13 12:00pm UTC)")
		return
	}

	if ms > work.lastMs {
		// new ms
		work.count = 0
		work.lastMs = ms
	} else {
		work.count &= work.sequenceMask
		if work.count == 0 {
			// over count limit, wait for next ms and set ms&work.lastms
			for work.lastMs == ms {
				ms = time.Now().UnixNano() / int64(time.Millisecond)
			}
			work.lastMs = ms
		}
	}
	i = firstBitMask & ((((ms - baseMs) << work.msShift) | work.nodeMask) | work.count)
	work.count++
	return
}

func (work *idGenWork) MaxNodeID() (maxNodeID int) {
	if work != nil {
		maxNodeID = work.maxNodeID
	}
	return
}
