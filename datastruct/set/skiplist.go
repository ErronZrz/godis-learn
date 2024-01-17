package set

import "math/rand"

const (
	maxLevel = 0x10
)

type (
	Element struct {
		Member string
		Score  float64
	}
	Level struct {
		forward *node
		span    int64
	}
	node struct {
		Element
		backward *node
		levels   []*Level
	}
	skipList struct {
		head, tail *node
		length     int64
		level      int16
	}
)

func newSkipList() *skipList {
	return &skipList{
		level: 1,
		head:  newNode(maxLevel, "", 0),
	}
}

func newNode(lv int16, member string, score float64) *node {
	res := &node{
		Element: Element{
			Member: member,
			Score:  score,
		},
		levels: make([]*Level, lv),
	}
	for i := 0; i < int(lv); i++ {
		res.levels[i] = new(Level)
	}
	return res
}

func (sl *skipList) insert(member string, score float64) *node {
	// 每一层中，谁的 forward 要指向新建的节点
	update := make([]*node, maxLevel)
	// 每一层中，新建节点的排名，最小为 0
	rank := make([]int64, maxLevel)
	ptr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i < sl.level-1 {
			rank[i] = rank[i+1]
		}
		if ptr.levels[i] != nil {
			for {
				fwd := ptr.forwardAt(i)
				if fwd == nil || fwd.Score > score || (fwd.Score == score && fwd.Member >= member) {
					break
				}
				rank[i] += ptr.levels[i].span
				ptr = fwd
			}
		}
		update[i] = ptr
	}
	randomLevel := randomLevel()
	if randomLevel > sl.level {
		for i := sl.level; i < randomLevel; i++ {
			rank[i] = 0
			update[i] = sl.head
			sl.head.levels[i].span = sl.length
		}
		sl.level = randomLevel
	}
	res := newNode(randomLevel, member, score)
	for i := 0; i < int(randomLevel); i++ {
		res.levels[i].forward = update[i].forwardAt(int16(i))
		update[i].levels[i].forward = res
		diff := rank[0] - rank[i]
		res.levels[i].span = update[i].levels[i].span - diff
		update[i].levels[i].span = diff + 1
	}
	for i := randomLevel; i < sl.level; i++ {
		update[i].levels[i].span++
	}
	if update[0] == sl.head {
		res.backward = nil
	} else {
		res.backward = update[0]
	}
	if res.forward() != nil {
		res.forward().backward = res
	} else {
		sl.tail = res
	}
	sl.length++
	return res
}

func (sl *skipList) remove(member string, score float64) bool {
	update := make([]*node, maxLevel)
	ptr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		fwd := ptr.forwardAt(i)
		for fwd != nil && (fwd.Score < score || (fwd.Score == score && fwd.Member < member)) {
			ptr = fwd
			fwd = ptr.forwardAt(i)
		}
		update[i] = ptr
	}
	ptr = ptr.forward()
	if ptr != nil && ptr.Score == score && ptr.Member == member {
		sl.removeNode(ptr, update)
		return true
	}
	return false
}

func (sl *skipList) removeNode(ptr *node, update []*node) {
	levels := ptr.levels
	for i := 0; i < int(sl.level); i++ {
		curLevel := update[i].levels[i]
		if curLevel.forward == ptr {
			curLevel.span += levels[i].span - 1
			curLevel.forward = levels[i].forward
		} else {
			curLevel.span--
		}
	}
	if levels[0].forward != nil {
		levels[0].forward.backward = ptr.backward
	} else {
		sl.tail = ptr.backward
	}
	for sl.level > 1 && sl.head.forwardAt(sl.level-1) == nil {
		sl.level--
	}
	sl.length--
}

func (sl *skipList) getRank(member string, score float64) int64 {
	res := int64(0)
	ptr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		fwd := ptr.forwardAt(i)
		for fwd != nil && (fwd.Score < score || (fwd.Score == score && fwd.Member <= member)) {
			res += ptr.levels[i].span
			ptr = fwd
			fwd = ptr.forwardAt(i)
		}
		if ptr.Member == member {
			return res
		}
	}
	return 0
}

func (sl *skipList) nodeWithRank(rank int64) *node {
	curRank := int64(0)
	ptr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for ptr.levels[i] != nil && curRank+ptr.levels[i].span <= rank {
			curRank += ptr.levels[i].span
			ptr = ptr.forwardAt(i)
		}
		if curRank == rank {
			return ptr
		}
	}
	return nil
}

func (sl *skipList) containsBetween(lo, hi *ScoreBorder) bool {
	maxPtr := sl.tail
	minPtr := sl.head.forward()
	return maxPtr != nil && minPtr != nil && lo.lessThan(maxPtr.Score) && hi.greaterThan(minPtr.Score)
}

func (sl *skipList) firstNodeBetween(lo, hi *ScoreBorder) *node {
	if !sl.containsBetween(lo, hi) {
		return nil
	}
	ptr := sl.head
	var fwd *node
	for i := sl.level - 1; i >= 0; i-- {
		fwd = ptr.forwardAt(i)
		for fwd != nil && !lo.lessThan(fwd.Score) {
			ptr = fwd
			fwd = ptr.forwardAt(i)
		}
	}
	if hi.greaterThan(fwd.Score) {
		return fwd
	}
	return nil
}

func (sl *skipList) lastNodeBetween(lo, hi *ScoreBorder) *node {
	if !sl.containsBetween(lo, hi) {
		return nil
	}
	ptr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		fwd := ptr.forwardAt(i)
		for fwd != nil && hi.greaterThan(fwd.Score) {
			ptr = fwd
			fwd = ptr.forwardAt(i)
		}
	}
	if lo.lessThan(ptr.Score) {
		return ptr
	}
	return nil
}

func (sl *skipList) removeBetween(lo, hi *ScoreBorder, limit int) []*Element {
	update := make([]*node, maxLevel)
	removed := make([]*Element, 0)
	ptr := sl.firstNodeBetween(lo, hi)
	for ptr != nil {
		if !hi.greaterThan(ptr.Score) {
			break
		}
		fwd := ptr.forward()
		removedElement := &(ptr.Element)
		removed = append(removed, removedElement)
		sl.removeNode(ptr, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		ptr = fwd
	}
	return removed
}

func (sl *skipList) removeRankBetween(start, stop int64) []*Element {
	current := int64(0)
	update := make([]*node, maxLevel)
	removed := make([]*Element, 0)
	ptr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		fwd := ptr.forwardAt(i)
		for fwd != nil && (current+ptr.levels[i].span) < start {
			current += ptr.levels[i].span
			ptr = fwd
			fwd = ptr.forwardAt(i)
		}
		update[i] = ptr
	}
	ptr = ptr.forward()
	current++
	for ptr != nil && current < stop {
		fwd := ptr.forward()
		removedElement := &(ptr.Element)
		removed = append(removed, removedElement)
		sl.removeNode(ptr, update)
		ptr = fwd
		current++
	}
	return removed
}

func (n *node) forward() *node {
	return n.forwardAt(0)
}

func (n *node) forwardAt(lv int16) *node {
	return n.levels[lv].forward
}

func randomLevel() int16 {
	level := int16(1)
	for level < maxLevel && (rand.Int31()&0xFFFF) < 0x4000 {
		level++
	}
	return level
}
