package dbinterface

import (
	"godis-instruction/interface/redis"
	"time"
)

type EntryValue struct {
	V any
}

type EntryProcessor func(string, *EntryValue, *time.Time) bool

type DB interface {
	Execute(conn redis.Connection, line redis.Line) redis.Reply
	AfterClientClose(conn redis.Connection)
	Close()
}

type EmbedDB interface {
	DB
	ExecWithLock(conn redis.Connection, line redis.Line) redis.Reply
	ExecMulti(conn redis.Connection, watching map[string]uint32, lines []redis.Line) redis.Reply
	GetUndoLogs(dbIndex int, line redis.Line) []redis.Line
	ForEach(dbIndex int, p EntryProcessor)
	RWLockKeys(dbIndex int, rKeys, wKeys []string)
	RWUnlockKeys(dbIndex int, rKeys, wKeys []string)
	GetDBSize(dbIndex int) (dataSize, ttlMapSize int)
}
