package snow

import (
	"hash/fnv"
	"log"
	"sync"
	"time"
)

const (
	thousand    int64 = 1000
	million     int64 = 1000_000
	epoch0      int64 = 1288834974657
	indexBits   byte  = 10
	inMilliBits byte  = 12
	indexMask   int64 = ^(-1 << indexBits)
	inMilliMask int64 = ^(-1 << inMilliBits)
)

type IDGenerator struct {
	mu              *sync.Mutex
	lastStamp       int64
	nodeIndex       int64
	sequenceInMilli int64
	epoch           time.Time
}

func NewIDGenerator(name string) *IDGenerator {
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(name))
	index := int64(fnv64.Sum64()) & indexMask
	curTime := time.Now()
	epoch := curTime.Add(time.Unix(epoch0/thousand, (epoch0%thousand)*million).Sub(curTime))
	return &IDGenerator{
		mu:              &sync.Mutex{},
		lastStamp:       -1,
		nodeIndex:       index,
		sequenceInMilli: 0,
		epoch:           epoch,
	}
}

func (g *IDGenerator) NextID() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	timestamp := g.getTimestamp()
	if timestamp < g.lastStamp {
		log.Fatal("cannot generate ID: unexpected timestamp")
		return -1
	}
	if timestamp == g.lastStamp {
		g.sequenceInMilli = (g.sequenceInMilli + 1) & inMilliMask
		if g.sequenceInMilli == 0 {
			for timestamp <= g.lastStamp {
				timestamp = g.getTimestamp()
			}
		}
	} else {
		g.sequenceInMilli = 0
	}
	g.lastStamp = timestamp
	return (timestamp << (indexBits + inMilliBits)) | (g.nodeIndex << inMilliBits) | g.sequenceInMilli
}

func (g *IDGenerator) getTimestamp() int64 {
	return time.Since(g.epoch).Nanoseconds() / million
}
