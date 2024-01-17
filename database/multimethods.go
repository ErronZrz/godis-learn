package database

import (
	"fmt"
	"godis-learn/interface/dbinterface"
	"godis-learn/interface/redis"
	"godis-learn/lib/logger"
	"godis-learn/persistent"
	"godis-learn/redis/protocol"
	"runtime/debug"
)

func (db *MultiDB) GetDBSize(dbIndex int) (dataSize, ttlMapSize int) {
	selected := db.dbPanicAt(dbIndex)
	dataSize, ttlMapSize = selected.m.Size(), selected.ttlMap.Size()
	return
}

func (db *MultiDB) Execute(conn redis.Connection, line redis.Line) redis.Reply {
	var res redis.Reply
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
		}
		res = protocol.UnknownErrorReply()
	}()
	res = db.doExecute(conn, line)
	return res
}

func (db *MultiDB) ExecWithLock(conn redis.Connection, line redis.Line) redis.Reply {
	single, err := db.dbAt(conn.GetDBIndex())
	if err != nil {
		return err
	}
	return single.executeWithLock(line)
}

func (db *MultiDB) ExecMulti(conn redis.Connection, watching map[string]uint32, lines []redis.Line) redis.Reply {
	single, err := db.dbAt(conn.GetDBIndex())
	if err != nil {
		return err
	}
	return single.ExecMulti(watching, lines)
}

func (db *MultiDB) GetUndoLogs(dbIndex int, line redis.Line) []redis.Line {
	return db.dbPanicAt(dbIndex).GetUndoLogs(line)
}

func (db *MultiDB) AfterClientClose(conn redis.Connection) {
	db.hub.UnsubscribeAll(conn)
}

func (db *MultiDB) Close() {
	_ = db.rep.close()
	if db.aofHandler != nil {
		db.aofHandler.Close()
	}
}

func (db *MultiDB) RWLockKeys(dbIndex int, rKeys, wKeys []string) {
	db.dbPanicAt(dbIndex).RWLockKeys(rKeys, wKeys)
}

func (db *MultiDB) RWUnlockKeys(dbIndex int, rKeys, wKeys []string) {
	db.dbPanicAt(dbIndex).RWUnlockKeys(rKeys, wKeys)
}

func (db *MultiDB) ForEach(dbIndex int, p dbinterface.EntryProcessor) {
	db.dbPanicAt(dbIndex).ForEach(p)
}

func (db *MultiDB) RewriteAOF() redis.Reply {
	if err := db.aofHandler.Rewrite(); err != nil {
		return protocol.NewErrorReply([]byte(err.Error()))
	}
	return protocol.OkReply()
}

func (db *MultiDB) BGRewriteAOF() redis.Reply {
	go func(h *persistent.Handler) {
		_ = h.Rewrite()
	}(db.aofHandler)
	return protocol.StatusReply([]byte("Background append only file rewriting started"))
}

func (db *MultiDB) SaveRDB() redis.Reply {
	if err := db.aofHandler.Rewrite2RDB(); err != nil {
		return protocol.NewErrorReply([]byte(err.Error()))
	}
	return protocol.OkReply()
}

func (db *MultiDB) BGSaveRDB() redis.Reply {
	if db.aofHandler == nil {
		return protocol.NewErrorReply([]byte("please enable aof before using save"))
	}
	go func(h *persistent.Handler) {
		defer func() {
			if err := recover(); err != nil {
				logger.Error(err)
			}
		}()
		if err := h.Rewrite2RDB(); err != nil {
			logger.Error(err)
		}
	}(db.aofHandler)
	return protocol.StatusReply([]byte("Background saving started"))
}

func (db *MultiDB) dbAt(dbIndex int) (*DB, redis.ErrorReply) {
	if err := db.checkIndex(dbIndex); err != nil {
		return nil, err
	}
	return db.dbs[dbIndex].Load().(*DB), nil
}

func (db *MultiDB) dbPanicAt(dbIndex int) *DB {
	res, err := db.dbAt(dbIndex)
	if err != nil {
		panic(err)
	}
	return res
}

func (db *MultiDB) checkIndex(dbIndex int) redis.ErrorReply {
	if dbIndex < 0 || dbIndex >= len(db.dbs) {
		return protocol.NewErrorReply([]byte("ERR DB index out of bounds"))
	}
	return nil
}
