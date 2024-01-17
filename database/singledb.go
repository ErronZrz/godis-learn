package database

import (
	"fmt"
	"godis-instruction/datastruct/dict"
	"godis-instruction/datastruct/lock"
	"godis-instruction/interface/redis"
	"godis-instruction/redis/protocol"
)

const (
	dataMapSize = 1 << 16
	ttlMapSize  = 1 << 10
	lockerSize  = 1 << 10
)

type (
	ExecFunc func(db *DB, line redis.Line) redis.Reply
	PreFunc  func(line redis.Line) ([]string, []string)
	UndoFunc func(db *DB, line redis.Line) []redis.Line
)

type DB struct {
	index      int
	m          dict.HashMap
	ttlMap     dict.HashMap
	versionMap dict.HashMap
	locker     *lock.StringLock
	addAOF     func(redis.Line)
}

func newConcurrentDB() *DB {
	return &DB{
		index:      0,
		m:          dict.NewConcurrentHashMap(dataMapSize),
		ttlMap:     dict.NewConcurrentHashMap(ttlMapSize),
		versionMap: dict.NewConcurrentHashMap(dataMapSize),
		locker:     lock.NewStringLock(lockerSize),
		addAOF:     func(redis.Line) {},
	}
}

func newSimpleDB() *DB {
	return &DB{
		index:      0,
		m:          dict.NewSimpleHashMap(),
		ttlMap:     dict.NewSimpleHashMap(),
		versionMap: dict.NewSimpleHashMap(),
		locker:     lock.NewStringLock(1),
		addAOF:     func(redis.Line) {},
	}
}

func (db *DB) Execute(conn redis.Connection, line redis.Line) redis.Reply {
	n := len(line)
	cmdName := line.CommandName()
	switch string(cmdName) {
	case "multi":
		if n != 1 {
			return protocol.ArgumentCountErrorReply(cmdName)
		}
		return StartMulti(conn)
	case "discard":
		if n != 1 {
			return protocol.ArgumentCountErrorReply(cmdName)
		}
		return DiscardMulti(conn)
	case "watch":
		if n < 2 {
			return protocol.ArgumentCountErrorReply(cmdName)
		}
		return Watch(db, conn, line.CommandContent())
	case "exec":
		if n != 1 {
			return protocol.ArgumentCountErrorReply(cmdName)
		}
		return db.doExecMulti(conn)
	}
	if conn != nil && conn.CheckMultiMode() {
		return ConnectionEnqueue(conn, line)
	}
	return db.executeGenericCommand(line)
}

func (db *DB) RWLockKeys(rKeys, wKeys []string) {
	db.locker.RWLockKeys(rKeys, wKeys)
}

func (db *DB) RWUnlockKeys(rKeys, wKeys []string) {
	db.locker.RWUnlockKeys(rKeys, wKeys)
}

func (db *DB) doExecMulti(conn redis.Connection) redis.Reply {
	if !conn.CheckMultiMode() {
		return protocol.NewErrorReply([]byte("ERR EXEC without MULTI"))
	}
	defer conn.SetMultiMode(false)
	if len(conn.GetTxErrors()) > 0 {
		return protocol.NewErrorReply([]byte("EXECABORT Transaction discarded because of previous errors"))
	}
	lines := conn.GetQueuedCmdLines()
	return db.ExecMulti(conn.GetWatching(), lines)
}

func (db *DB) executeGenericCommand(line redis.Line) redis.Reply {
	cmdName := line.CommandName()
	cmd, ok := cmdMap[string(cmdName)]
	if !ok {
		return protocol.NewErrorReply([]byte(fmt.Sprintf("ERR unknown command '%s'", cmdName)))
	}
	if invalidArity(line, cmd) {
		return protocol.ArgumentCountErrorReply(cmdName)
	}
	rKeys, wKeys := cmd.prepare(line.CommandContent())
	db.incrVersions(wKeys)
	db.RWLockKeys(rKeys, wKeys)
	defer db.RWUnlockKeys(rKeys, wKeys)
	return cmd.executor(db, line.CommandContent())
}

func (db *DB) executeWithLock(line redis.Line) redis.Reply {
	cmdName := line.CommandName()
	cmd, ok := cmdMap[string(cmdName)]
	if !ok {
		return protocol.NewErrorReply([]byte(fmt.Sprintf("ERR unknown command '%s'", cmdName)))
	}
	if invalidArity(line, cmd) {
		return protocol.ArgumentCountErrorReply(cmdName)
	}
	return cmd.executor(db, line.CommandContent())
}

func invalidArity(line redis.Line, cmd *command) bool {
	n, arity := len(line), cmd.arity
	if arity >= 0 {
		return n != arity
	}
	return n < -arity
}

func RelatedKeys(line redis.Line) (rKeys, wKeys []string) {
	cmd, ok := cmdMap[string(line.CommandName())]
	if !ok {
		return nil, nil
	}
	prepare := cmd.prepare
	if prepare == nil {
		return nil, nil
	}
	return prepare(line.CommandContent())
}
