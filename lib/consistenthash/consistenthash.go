package consistenthash

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
)

type HashFunc func(data []byte) uint32

type ConsistentPicker struct {
	hashFunc     HashFunc
	replicaCount int
	keys         []int
	m            map[int]string
}

func NewPicker(replicaCount int, f HashFunc) *ConsistentPicker {
	if f == nil {
		f = crc32.ChecksumIEEE
	}
	return &ConsistentPicker{
		hashFunc:     f,
		replicaCount: replicaCount,
		m:            make(map[int]string),
	}
}

func (p *ConsistentPicker) IsEmpty() bool {
	return len(p.keys) == 0
}

func (p *ConsistentPicker) AddNodes(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0; i < p.replicaCount; i++ {
			hashed := int(p.hashFunc([]byte(fmt.Sprintf("%d%s", i, key))))
			p.keys = append(p.keys, hashed)
			p.m[hashed] = key
		}
	}
	sort.Ints(p.keys)
}

func (p *ConsistentPicker) PickNode(key string) string {
	if p.IsEmpty() {
		return ""
	}
	partitionKey := getPartitionKey(key)
	hashed := int(p.hashFunc([]byte(partitionKey)))
	index := sort.Search(len(p.keys), func(i int) bool {
		return p.keys[i] >= hashed
	})
	if index == len(p.keys) {
		index = 0
	}
	return p.m[p.keys[index]]
}

func getPartitionKey(key string) string {
	begin := strings.Index(key, "{")
	end := strings.Index(key, "}")
	if begin < 0 || end <= begin+1 {
		return key
	}
	return key[begin+1 : end]
}
