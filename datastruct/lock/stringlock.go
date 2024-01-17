package lock

import (
	"godis-learn/lib/utils"
	"sort"
	"sync"
)

const (
	initialHash = uint32(2166136261)
	prime32     = uint32(16777619)
)

var mutexMethods = []mutexMethod{
	func(mu *sync.RWMutex) { mu.Lock() },
	func(mu *sync.RWMutex) { mu.Unlock() },
	func(mu *sync.RWMutex) { mu.RLock() },
	func(mu *sync.RWMutex) { mu.RUnlock() },
}

const (
	doLock = iota
	doUnlock
	doRLock
	doRUnlock
)

// mutexMethod 是一把 RWMutex 锁的某个方法
type mutexMethod func(*sync.RWMutex)

// StringLock 是能够实现对多个 string 类型的 key 手动获取和释放读锁和写锁的结构
type StringLock struct {
	mutexes []*sync.RWMutex
}

// NewStringLock 创建并返回 StringLock 的实例指针
func NewStringLock(capacity int) *StringLock {
	m := make([]*sync.RWMutex, capacity)
	for i := 0; i < capacity; i++ {
		m[i] = new(sync.RWMutex)
	}
	return &StringLock{mutexes: m}
}

// Lock 获取单个 key 的排他锁
func (l *StringLock) Lock(key string) {
	l.executeKey(key, doLock)
}

// Unlock 释放单个 key 的排他锁
func (l *StringLock) Unlock(key string) {
	l.executeKey(key, doUnlock)
}

// RLock 获取单个 key 的共享锁
func (l *StringLock) RLock(key string) {
	l.executeKey(key, doRLock)
}

// RUnlock 释放单个 key 的共享锁
func (l *StringLock) RUnlock(key string) {
	l.executeKey(key, doRUnlock)
}

// LockKeys 获取多个 key 的排他锁，会阻塞直到全部获取成功
func (l *StringLock) LockKeys(keys []string) {
	l.executeKeys(keys, doLock)
}

// UnlockKeys 释放多个 key 的排他锁
func (l *StringLock) UnlockKeys(keys []string) {
	l.executeKeys(keys, doUnlock)
}

// RLockKeys 获取多个 key 的共享锁，会阻塞直到全部获取成功
func (l *StringLock) RLockKeys(keys []string) {
	l.executeKeys(keys, doRLock)
}

// RUnlockKeys 释放多个 key 的共享锁
func (l *StringLock) RUnlockKeys(keys []string) {
	l.executeKeys(keys, doRUnlock)
}

// RWLockKeys 对于 wKeys 中的字符串获取排他锁，对于 rKeys 中不属于 wKeys 的字符串获取共享锁，会阻塞直到全部获取成功
func (l *StringLock) RWLockKeys(rKeys, wKeys []string) {
	l.executeRWKeys(rKeys, wKeys, false)
}

// RWUnlockKeys 对于 wKeys 中的字符串释放排他锁，对于 rKeys 中不属于 wKeys 的字符串释放共享锁
func (l *StringLock) RWUnlockKeys(rKeys, wKeys []string) {
	l.executeRWKeys(rKeys, wKeys, true)
}

// fnv32 是一个哈希函数
func fnv32(key string) uint32 {
	hash := initialHash
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// codeIndex 根据哈希函数的结果，得到相应锁的下标
func (l *StringLock) codeIndex(code uint32) uint32 {
	if l == nil {
		panic("Nil ConcurrentHashMap")
	}
	return (uint32(len(l.mutexes)) - 1) & code
}

// keyIndex 直接根据 key 获取下标
func (l *StringLock) keyIndex(key string) uint32 {
	return l.codeIndex(fnv32(key))
}

// executeKey 针对单个 key 执行加锁或释放锁
func (l *StringLock) executeKey(key string, methodIndex int) {
	index := l.keyIndex(key)
	mutexMethods[methodIndex](l.mutexes[index])
}

// executeKeys 针对多个 key 执行加锁或释放锁
func (l *StringLock) executeKeys(keys []string, methodIndex int) {
	order := l.orderFor(keys, 1 == methodIndex&1)
	for _, index := range order {
		mutexMethods[methodIndex](l.mutexes[index])
	}
}

// executeRWKeys 针对读和写的 key 分别获取或者释放共享锁和排他锁
func (l *StringLock) executeRWKeys(rKeys, wKeys []string, unlock bool) {
	keys := append(rKeys, wKeys...)
	order := l.orderFor(keys, unlock)
	wOrder := l.orderFor(wKeys, unlock)
	wSet := make(map[uint32]struct{})
	for _, index := range wOrder {
		wSet[index] = struct{}{}
	}
	methodIndex := 0
	if unlock {
		methodIndex++
	}
	for _, index := range order {
		_, write := wSet[index]
		if write {
			mutexMethods[methodIndex](l.mutexes[index])
		} else {
			mutexMethods[methodIndex+2](l.mutexes[index])
		}
	}
}

// orderFor 根据多个 key 获取各自的下标，并排序后返回，顺序请求锁防止死锁
func (l *StringLock) orderFor(keys []string, reverse bool) []uint32 {
	exist := make(map[uint32]bool)
	for _, key := range keys {
		exist[l.keyIndex(key)] = true
	}
	order := make([]uint32, 0, len(exist))
	for index := range exist {
		order = append(order, index)
	}
	sort.Slice(order, func(i, j int) bool {
		return utils.Xor(reverse, order[i] < order[j])
	})
	return order
}
