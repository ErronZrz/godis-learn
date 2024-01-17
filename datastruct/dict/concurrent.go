package dict

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

// ConcurrentHashMap 是线程安全的 map
type ConcurrentHashMap struct {
	shards  []*shard
	size    int32
	nShards int
}

type shard struct {
	data  map[string]any
	mutex sync.RWMutex
}

func NewConcurrentHashMap(nShards int) *ConcurrentHashMap {
	nShards = computeShardCount(nShards)
	shards := make([]*shard, nShards)
	for i := 0; i < nShards; i++ {
		shards[i] = &shard{
			data: make(map[string]any),
		}
	}
	return &ConcurrentHashMap{
		size:    0,
		shards:  shards,
		nShards: nShards,
	}
}

func (m *ConcurrentHashMap) Size() int {
	if m == nil {
		panic("Nil ConcurrentHashMap")
	}
	return int(atomic.LoadInt32(&(m.size)))
}

func (m *ConcurrentHashMap) Put(key string, value any) {
	if m == nil {
		panic("Nil ConcurrentHashMap")
	}
	hashCode := fnv32(key)
	index := m.codeIndex(hashCode)
	shard := m.shardAt(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.data[key]; !ok {
		m.ascendSize()
	}
	shard.data[key] = value
}

func (m *ConcurrentHashMap) PutIfAbsent(key string, value any) (ok bool) {
	if m == nil {
		panic("Nil ConcurrentHashMap")
	}
	hashCode := fnv32(key)
	index := m.codeIndex(hashCode)
	shard := m.shardAt(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.data[key]; ok {
		return false
	}
	shard.data[key] = value
	m.ascendSize()
	return true
}

func (m *ConcurrentHashMap) PutIfExists(key string, value any) (ok bool) {
	if m == nil {
		panic("Nil ConcurrentHashMap")
	}
	hashCode := fnv32(key)
	index := m.codeIndex(hashCode)
	shard := m.shardAt(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.data[key]; !ok {
		return false
	}
	shard.data[key] = value
	return true
}

func (m *ConcurrentHashMap) Get(key string) (value any, ok bool) {
	if m == nil {
		panic("Nil ConcurrentHashMap")
	}
	hashCode := fnv32(key)
	index := m.codeIndex(hashCode)
	shard := m.shardAt(index)
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	value, ok = shard.data[key]
	return
}

func (m *ConcurrentHashMap) Delete(key string) (ok bool) {
	if m == nil {
		panic("Nil ConcurrentHashMap")
	}
	hashCode := fnv32(key)
	index := m.codeIndex(hashCode)
	shard := m.shardAt(index)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.data[key]; ok {
		delete(shard.data, key)
		m.descendSize()
		return true
	}
	return false
}

func (m *ConcurrentHashMap) ForEach(p Processor) {
	if m == nil {
		panic("Nil ConcurrentHashMap")
	}
	for _, shard := range m.shards {
		shard.mutex.RLock()
		func() {
			defer shard.mutex.RUnlock()
			for key, value := range shard.data {
				if !p(key, value) {
					return
				}
			}
		}()
	}
}

func (m *ConcurrentHashMap) Keys() []string {
	keys := make([]string, m.Size())
	i := 0
	m.ForEach(func(key string, _ any) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

func (m *ConcurrentHashMap) RandomKeys(nKeys int) []string {
	if nKeys >= m.Size() {
		return m.Keys()
	}
	res := make([]string, nKeys)
	for i := 0; i < nKeys; {
		shard := m.shardAt(uint32(rand.Intn(m.nShards)))
		if shard == nil {
			continue
		}
		key := shard.randomKey()
		if key != "" {
			res[i] = key
			i++
		}
	}
	return res
}

func (m *ConcurrentHashMap) RandomDistinctKeys(nKeys int) []string {
	if nKeys >= m.Size() {
		return m.Keys()
	}
	m2 := NewConcurrentHashMap(nKeys)
	for m2.Size() < nKeys {
		shard := m.shardAt(uint32(rand.Intn(m.nShards)))
		if shard == nil {
			continue
		}
		key := shard.randomKey()
		if key != "" {
			m2.Put(key, true)
		}
	}
	return m2.Keys()
}

func (m *ConcurrentHashMap) Clear() {
	*m = *NewConcurrentHashMap(m.nShards)
}

// computeShardCount 计算不小于 n 的 2 的整数次幂
func computeShardCount(n int) int {
	if n <= 16 {
		return 16
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return 1 + n
}

// fnv32 是一个哈希函数
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	// 此处不用 for range 是因为不应考虑字符而是只考虑字节
	for i := 0; i < len(key); i++ {
		hash *= uint32(16777619)
		hash ^= uint32(key[i])
	}
	return hash
}

// codeIndex 根据哈希函数的结果，得到相应 shard 的下标
func (m *ConcurrentHashMap) codeIndex(code uint32) uint32 {
	if m == nil {
		panic("Nil ConcurrentHashMap")
	}
	return (uint32(m.nShards) - 1) & code
}

// 获取下标处的 shard
func (m *ConcurrentHashMap) shardAt(index uint32) *shard {
	if m == nil {
		panic("Nil ConcurrentHashMap")
	}
	if index < 0 || index >= uint32(m.nShards) {
		panic("Shard index out of boundary")
	}
	return m.shards[index]
}

// ascendSize 会使 size 自增
func (m *ConcurrentHashMap) ascendSize() int32 {
	return atomic.AddInt32(&(m.size), 1)
}

// descendSize 会使 size 自减
func (m *ConcurrentHashMap) descendSize() int32 {
	return atomic.AddInt32(&(m.size), -1)
}

// randomKey 从 shard 处随机取得一个 key
func (s *shard) randomKey() string {
	if s == nil {
		panic("Nil shard")
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for key := range s.data {
		return key
	}
	return ""
}
