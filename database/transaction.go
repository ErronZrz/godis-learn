package database

import (
	"fmt"
	"godis-learn/interface/redis"
	"godis-learn/redis/protocol"
)

func StartMulti(conn redis.Connection) redis.Reply {
	if conn.CheckMultiMode() {
		return protocol.NewErrorReply([]byte("ERR MULTI calls cannot be nested"))
	}
	conn.SetMultiMode(true)
	return protocol.OkReply()
}

func DiscardMulti(conn redis.Connection) redis.Reply {
	if !conn.CheckMultiMode() {
		return protocol.NewErrorReply([]byte("ERR DISCARD without MULTI"))
	}
	conn.ClearQueue()
	conn.SetMultiMode(false)
	return protocol.OkReply()
}

func Watch(db *DB, conn redis.Connection, args redis.Line) redis.Reply {
	watching := conn.GetWatching()
	for _, arg := range args {
		key := string(arg)
		watching[key] = db.getVersion(key)
	}
	return protocol.OkReply()
}

func (db *DB) ExecMulti(watching map[string]uint32, lines []redis.Line) redis.Reply {
	rKeys, wKeys := make([]string, 0), make([]string, 0)
	for _, line := range lines {
		cmd := cmdMap[string(line.CommandName())]
		read, write := cmd.prepare(line.CommandContent())
		rKeys = append(rKeys, read...)
		wKeys = append(wKeys, write...)
	}
	for key := range watching {
		rKeys = append(rKeys, key)
	}
	db.RWLockKeys(rKeys, wKeys)
	defer db.RWUnlockKeys(rKeys, wKeys)
	if watchingUpdated(db, watching) {
		return protocol.EmptyArrayReply()
	}
	n := len(lines)
	res := make([]redis.Reply, 0, n)
	aborted := false
	undoLines := make([][]redis.Line, 0, n)
	for _, line := range lines {
		undoLines = append(undoLines, db.GetUndoLogs(line))
		reply := db.executeWithLock(line)
		if protocol.CheckErrorReply(reply) {
			aborted = true
			undoLines = undoLines[:len(undoLines)-1]
			break
		}
		res = append(res, reply)
	}
	if !aborted {
		db.incrVersions(wKeys)
		return protocol.ContainingReply(res)
	}
	// if aborted
	n = len(undoLines)
	for i := n - 1; i >= 0; i-- {
		uLines := undoLines[i]
		if len(uLines) == 0 {
			continue
		}
		for _, line := range uLines {
			db.executeWithLock(line)
		}
	}
	return protocol.NewErrorReply([]byte("EXECABORT Transaction discarded because of previous errors"))
}

func ConnectionEnqueue(conn redis.Connection, line redis.Line) redis.Reply {
	cmdName := line.CommandName()
	cmd, ok := cmdMap[string(cmdName)]
	var err redis.ErrorReply
	if !ok {
		err = protocol.NewErrorReply([]byte(fmt.Sprintf("ERR unknown command '%s'", cmdName)))
	} else if cmd.prepare == nil {
		err = protocol.NewErrorReply([]byte(fmt.Sprintf("ERR command '%s' cannot be used in MULTI", cmdName)))
	} else if invalidArity(line, cmd) {
		err = protocol.ArgumentCountErrorReply(cmdName)
	}
	if err != nil {
		conn.AddTxError(err)
		return err
	}
	conn.EnqueueCmdLine(line)
	return protocol.QueuedReply()
}

func (db *DB) GetUndoLogs(line redis.Line) []redis.Line {
	cmd, ok := cmdMap[string(line.CommandName())]
	if !ok {
		return nil
	}
	undo := cmd.undo
	if undo == nil {
		return nil
	}
	return undo(db, line.CommandContent())
}

func (db *DB) incrVersions(keys []string) {
	for _, key := range keys {
		version := db.getVersion(key)
		db.versionMap.Put(key, version+1)
	}
}

func (db *DB) getVersion(key string) uint32 {
	value, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	return value.(uint32)
}

func watchingUpdated(db *DB, watching map[string]uint32) bool {
	for key, version := range watching {
		if cur := db.getVersion(key); cur != version {
			return true
		}
	}
	return false
}
