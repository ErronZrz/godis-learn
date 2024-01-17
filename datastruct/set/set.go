package set

import "godis-instruction/datastruct/dict"

type Consumer func(string) bool

type HashSet struct {
	m dict.HashMap
}

func NewHashSet(members ...string) *HashSet {
	res := &HashSet{m: dict.NewSimpleHashMap()}
	for _, str := range members {
		res.Add(str)
	}
	return res
}

func (s *HashSet) Size() int {
	return s.m.Size()
}

func (s *HashSet) Add(val string) (ok bool) {
	return s.m.PutIfAbsent(val, 0)
}

func (s *HashSet) Contains(val string) bool {
	_, ok := s.m.Get(val)
	return ok
}

func (s *HashSet) Remove(val string) (ok bool) {
	return s.m.Delete(val)
}

func (s *HashSet) ForEach(c Consumer) {
	s.m.ForEach(func(key string, _ any) bool {
		return c(key)
	})
}

func (s *HashSet) Members() []string {
	// 不直接调用 m.Keys() 是因为可能会引发并发问题
	res := make([]string, s.m.Size())
	i := 0
	s.m.ForEach(func(key string, _ any) bool {
		if i < len(res) {
			res[i] = key
		} else {
			res = append(res, key)
		}
		i++
		return true
	})
	return res
}

func (s *HashSet) RandomMembers(nMembers int) []string {
	return s.m.RandomKeys(nMembers)
}

func (s *HashSet) RandomDistinctMembers(nMembers int) []string {
	return s.m.RandomDistinctKeys(nMembers)
}

func (s *HashSet) Intersect(s1 *HashSet) *HashSet {
	if s == nil {
		panic("HashSet is nil")
	}
	res := NewHashSet()
	s.ForEach(func(member string) bool {
		if s1.Contains(member) {
			res.Add(member)
		}
		return true
	})
	return res
}

func (s *HashSet) Union(s1 *HashSet) *HashSet {
	if s == nil {
		panic("HashSet is nil")
	}
	res := NewHashSet()
	addFunc := func(member string) bool {
		res.Add(member)
		return true
	}
	s.ForEach(addFunc)
	s1.ForEach(addFunc)
	return res
}

func (s *HashSet) Diff(s1 *HashSet) *HashSet {
	if s == nil {
		panic("HashSet is nil")
	}
	res := NewHashSet()
	s.ForEach(func(member string) bool {
		if !s1.Contains(member) {
			res.Add(member)
		}
		return true
	})
	return res
}
