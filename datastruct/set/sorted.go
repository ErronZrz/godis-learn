package set

import "strconv"

type ElementConsumer func(*Element) bool

type SortedSet struct {
	m   map[string]*Element
	skl *skipList
}

func NewSortedSet() *SortedSet {
	return &SortedSet{
		m:   make(map[string]*Element),
		skl: newSkipList(),
	}
}

func (s *SortedSet) Size() int64 {
	return int64(len(s.m))
}

func (s *SortedSet) Add(member string, score float64) bool {
	element, ok := s.m[member]
	s.m[member] = &Element{
		Member: member,
		Score:  score,
	}
	if !ok {
		s.skl.insert(member, score)
		return true
	}
	if score != element.Score {
		s.skl.remove(member, element.Score)
		s.skl.insert(member, score)
	}
	return false
}

func (s *SortedSet) Find(member string) (*Element, bool) {
	element, ok := s.m[member]
	return element, ok
}

func (s *SortedSet) Remove(member string) bool {
	element, ok := s.m[member]
	if !ok {
		return false
	}
	s.skl.remove(member, element.Score)
	delete(s.m, member)
	return true
}

func (s *SortedSet) RemoveBetween(lo, hi *ScoreBorder) int64 {
	removed := s.skl.removeBetween(lo, hi, 0)
	for _, element := range removed {
		delete(s.m, element.Member)
	}
	return int64(len(removed))
}

func (s *SortedSet) RemoveRankBetween(start, stop int64) int64 {
	removed := s.skl.removeRankBetween(start+1, stop+1)
	for _, element := range removed {
		delete(s.m, element.Member)
	}
	return int64(len(removed))
}

func (s *SortedSet) PopMin(count int) []*Element {
	first := s.skl.head.forward()
	if first == nil {
		return nil
	}
	border := &ScoreBorder{
		Value:   first.Score,
		Exclude: false,
	}
	removed := s.skl.removeBetween(border, border, count)
	for _, element := range removed {
		delete(s.m, element.Member)
	}
	return removed
}

func (s *SortedSet) RankOf(member string, desc bool) int64 {
	element, ok := s.m[member]
	if !ok {
		return -1
	}
	rank := s.skl.getRank(member, element.Score)
	if desc {
		rank = s.skl.length - rank
	} else {
		rank--
	}
	return rank
}

func (s *SortedSet) CountBetween(lo, hi *ScoreBorder) int64 {
	res := int64(0)
	s.ForEachRankBetween(0, s.Size(), false, func(e *Element) bool {
		if !hi.greaterThan(e.Score) {
			return false
		}
		if lo.lessThan(e.Score) {
			res++
		}
		return true
	})
	return res
}

func (s *SortedSet) SliceBetween(lo, hi *ScoreBorder, offset, limit int64, desc bool) []*Element {
	res := make([]*Element, 0)
	if limit == 0 || offset < 0 {
		return res
	}
	s.ForEachBetween(lo, hi, offset, limit, desc, func(e *Element) bool {
		res = append(res, e)
		return true
	})
	return res
}

func (s *SortedSet) SliceRankBetween(start, stop int64, desc bool) []*Element {
	n := int(stop - start)
	res := make([]*Element, n)
	i := 0
	s.ForEachRankBetween(start, stop, desc, func(e *Element) bool {
		res[i] = e
		i++
		return true
	})
	return res
}

func (s *SortedSet) ForEachRankBetween(start, stop int64, desc bool, c ElementConsumer) {
	sz := s.Size()
	if start < 0 || start >= sz {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > sz {
		panic("illegal stop " + strconv.FormatInt(stop, 10))
	}
	var ptr *node
	if desc {
		if start == 0 {
			ptr = s.skl.tail
		} else {
			ptr = s.skl.nodeWithRank(sz - start)
		}
	} else {
		if start == 0 {
			ptr = s.skl.head.forward()
		} else {
			ptr = s.skl.nodeWithRank(start + 1)
		}
	}
	n := int(stop - start)
	for i := 0; i < n; i++ {
		if !c(&ptr.Element) {
			break
		}
		ptr = move(ptr, desc)
	}
}

func (s *SortedSet) ForEachBetween(lo, hi *ScoreBorder, offset, limit int64, desc bool, c ElementConsumer) {
	var ptr *node
	if desc {
		ptr = s.skl.lastNodeBetween(lo, hi)
	} else {
		ptr = s.skl.firstNodeBetween(lo, hi)
	}
	for ptr != nil && offset > 0 {
		ptr = move(ptr, desc)
		offset--
	}
	for i := 0; limit < 0 || i < int(limit); i++ {
		if ptr == nil || !lo.lessThan(ptr.Element.Score) || !hi.greaterThan(ptr.Element.Score) || !c(&ptr.Element) {
			break
		}
		ptr = move(ptr, desc)
	}
}

func move(ptr *node, desc bool) *node {
	if desc {
		return ptr.backward
	}
	return ptr.forward()
}
