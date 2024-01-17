package persistent

import (
	"godis-learn/datastruct/dict"
	"godis-learn/datastruct/list"
	"godis-learn/datastruct/set"
	"godis-learn/interface/dbinterface"
	"godis-learn/interface/redis"
	"godis-learn/redis/protocol"
	"strconv"
	"time"
)

func ValueToReply(key string, val *dbinterface.EntryValue) redis.Reply {
	if val == nil {
		return nil
	}
	var data [][]byte
	keyBytes := []byte(key)
	switch v := val.V.(type) {
	case []byte:
		data = stringToArgs(keyBytes, v)
	case list.List:
		data = listToArgs(keyBytes, v)
	case *set.HashSet:
		data = setToArgs(keyBytes, v)
	case dict.HashMap:
		data = hashToArgs(keyBytes, v)
	case *set.SortedSet:
		data = zsetToArgs(keyBytes, v)
	}
	return protocol.ArrayReply(data)
}

func stringToArgs(key []byte, s []byte) [][]byte {
	return [][]byte{{'S', 'E', 'T'}, key, s}
}

func listToArgs(key []byte, l list.List) [][]byte {
	res := make([][]byte, 2+l.Size())
	res[0], res[1] = []byte{'R', 'P', 'U', 'S', 'H'}, key
	l.ForEach(func(i int, val any) bool {
		res[2+i] = val.([]byte)
		return true
	})
	return res
}

func setToArgs(key []byte, s *set.HashSet) [][]byte {
	res := make([][]byte, 2+s.Size())
	res[0], res[1] = []byte{'S', 'A', 'D', 'D'}, key
	i := 2
	s.ForEach(func(val string) bool {
		res[i] = []byte(val)
		i++
		return true
	})
	return res
}

func hashToArgs(key []byte, m dict.HashMap) [][]byte {
	res := make([][]byte, 2+m.Size()<<1)
	res[0], res[1] = []byte{'H', 'M', 'S', 'E', 'T'}, key
	i := 2
	m.ForEach(func(field string, val any) bool {
		res[i] = []byte(field)
		i++
		res[i] = val.([]byte)
		i++
		return true
	})
	return res
}

func zsetToArgs(key []byte, s *set.SortedSet) [][]byte {
	res := make([][]byte, 2+s.Size()<<1)
	res[0], res[1] = []byte{'Z', 'A', 'D', 'D'}, key
	i := 2
	s.ForEachRankBetween(0, s.Size(), true, func(e *set.Element) bool {
		val := strconv.FormatFloat(e.Score, 'f', -1, 64)
		res[i] = []byte(val)
		i++
		res[i] = []byte(e.Member)
		i++
		return true
	})
	return res
}

func expireToArgs(key []byte, expireTime time.Time) [][]byte {
	return [][]byte{
		{'P', 'E', 'X', 'P', 'I', 'R', 'E', 'A', 'T'},
		key,
		[]byte(strconv.FormatInt(expireTime.UnixNano()/1_000_000, 10)),
	}
}
