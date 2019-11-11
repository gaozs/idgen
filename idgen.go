package idgen

import (
	"errors"
	"sync"
	"time"
)

// 0-sequenceBits-1  bit: sequenceBits bits are serial number
// sequenceBits-nodeIDBits-1 bit: nodeIDBits bits are node id
// nodeIDBits-62 bit: 63-nodeIDBits-sequenceBits bits are ms
// 63    bit: is always 0

// to ensure first bit is 0
const firstBitMask = int64(uint64(1)<<63 - 1)

// for reduce ms part
var baseMs = time.Date(2010, 9, 13, 12, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)

// IDGenWorker defines a worker
type IDGenWorker interface {
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
// sequenceBits must >=12 and nodeIDBits must >=1
// sequenceBits+nodeIDBits must <= 20 (which mean ms has 43bit+, totally has about 278+ years range)
// sequenceBits,nodeIDBits can be 0,default value will be 14,5
func NewWorker(sequenceBits, nodeIDBits, nodeID int) (worker IDGenWorker, err error) {
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

	ms := getNowMs()
	if ms < work.lastMs {
		err = errors.New("time error, now is before last time")
		return
	}

	if ms > work.lastMs {
		// new ms
		work.count = 0
		work.lastMs = ms
	} else {
		work.count &= work.sequenceMask
		if work.count == 0 {
			// over count limit, wait for next ms and set work.lastms
			for work.lastMs == ms {
				work.lastMs = getNowMs()
			}
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

func getNowMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
