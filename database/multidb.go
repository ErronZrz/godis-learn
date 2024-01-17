package database

import (
	"bytes"
	"godis-learn/config"
	"godis-learn/hub"
	"godis-learn/interface/dbinterface"
	"godis-learn/interface/redis"
	"godis-learn/lib/utils"
	"godis-learn/persistent"
	"godis-learn/redis/connection"
	"godis-learn/redis/protocol"
	"strconv"
	"sync/atomic"
	"time"
)

type MultiDB struct {
	dbs        []*atomic.Value
	hub        *hub.Hub
	aofHandler *persistent.Handler
	masterAddr string
	role       int32
	rep        *replicationStatus
}

func NewStandaloneServer() *MultiDB {
	db := &MultiDB{}
	if config.Properties.DatabaseCount == 0 {
		config.Properties.DatabaseCount = 16
	}
	db.dbs = make([]*atomic.Value, config.Properties.DatabaseCount)
	for i := 0; i < config.Properties.DatabaseCount; i++ {
		singleDB := newConcurrentDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		db.dbs[i] = holder
	}
	db.hub = hub.NewHub()
	validAOF := false
	if config.Properties.AppendOnly {
		aofHandler, err := persistent.NewAOFHandler(db, func() dbinterface.EmbedDB {
			return NewBasicMultiDB()
		})
		if err != nil {
			panic(err)
		}
		db.aofHandler = aofHandler
		for _, value := range db.dbs {
			single := value.Load().(*DB)
			single.addAOF = func(line redis.Line) {
				db.aofHandler.AddAOF(single.index, line)
			}
		}
		validAOF = true
	}
	if config.Properties.RDBFilename != "" && !validAOF {
		loadRDBFile(db)
	}
	db.rep = newReplicationStatus()
	startReplicationCron(db)
	db.role = masterRole
	return db
}

func NewBasicMultiDB() *MultiDB {
	res := &MultiDB{
		dbs: make([]*atomic.Value, config.Properties.DatabaseCount),
	}
	for i := 0; i < config.Properties.DatabaseCount; i++ {
		holder := &atomic.Value{}
		holder.Store(newSimpleDB())
		res.dbs[i] = holder
	}
	return res
}

func (db *MultiDB) doExecute(conn redis.Connection, line redis.Line) redis.Reply {
	cmdName := string(line.CommandName())
	content := line.CommandContent()
	if cmdName == "auth" {
		return Auth(conn, content)
	}
	if !authenticated(conn) {
		return protocol.NewErrorReply([]byte("NOAUTH Authentication required"))
	}
	if cmdName == "slaveof" {
		if conn != nil && conn.CheckMultiMode() {
			return protocol.NewErrorReply([]byte("SLAVEOF"))
		}
		return db.execSlaveOf(conn, content)
	}
	role := atomic.LoadInt32(&(db.role))
	if role == slaveRole && conn.GetRole() != connection.ReplicationClient {
		if !checkReadOnlyCommand(cmdName) {
			return protocol.NewErrorReply([]byte("READONLY You cannot write against a read only slave"))
		}
	}
	switch cmdName {
	case "subscribe":
		if len(line) < 2 {
			return protocol.ArgumentCountErrorReply([]byte("subscribe"))
		}
		return db.hub.Subscribe(conn, content)
	case "publish":
		return db.hub.Publish(content)
	case "unsubscribe":
		return db.hub.Unsubscribe(conn, content)
	case "rewriteaof":
		return db.RewriteAOF()
	case "bgrewriteaof":
		return db.BGRewriteAOF()
	case "save":
		return db.SaveRDB()
	case "bgsave":
		return db.BGSaveRDB()
	case "flushdb":
		if len(line) != 1 {
			return protocol.ArgumentCountErrorReply([]byte(cmdName))
		} else if conn != nil && conn.CheckMultiMode() {
			return protocol.NewErrorReply([]byte("ERR command 'FLUSHDB' cannot be used in MULTI"))
		}
		return db.flushAt(conn.GetDBIndex())
	case "flushall":
		return db.flushAll()
	case "select":
		if len(line) != 2 {
			return protocol.ArgumentCountErrorReply([]byte(cmdName))
		} else if conn != nil && conn.CheckMultiMode() {
			return protocol.NewErrorReply([]byte("cannot select database within MULTI"))
		}
		return db.select_(conn, content)
	case "copy":
		if len(line) < 3 {
			return protocol.ArgumentCountErrorReply([]byte(cmdName))
		}
		return db.copy_(conn, content)
	}
	dbIndex := conn.GetDBIndex()
	single, err := db.dbAt(dbIndex)
	if err != nil {
		return err
	}
	return single.Execute(conn, line)
}

func (db *MultiDB) storeNewDB(dbIndex int, single *DB) redis.Reply {
	if err := db.checkIndex(dbIndex); err != nil {
		return err
	}
	single.index = dbIndex
	single.addAOF = db.dbPanicAt(dbIndex).addAOF
	db.dbs[dbIndex].Store(single)
	return protocol.OkReply()
}

func (db *MultiDB) flushAll() redis.Reply {
	for i := 0; i < len(db.dbs); i++ {
		db.flushAt(i)
	}
	if db.aofHandler != nil {
		db.aofHandler.AddAOF(0, utils.StringsToLine("FlushAll"))
	}
	return protocol.OkReply()
}

func (db *MultiDB) flushAt(dbIndex int) redis.Reply {
	if err := db.checkIndex(dbIndex); err != nil {
		return err
	}
	newDB := newConcurrentDB()
	db.storeNewDB(dbIndex, newDB)
	return protocol.OkReply()
}

func (db *MultiDB) select_(conn redis.Connection, line redis.Line) redis.Reply {
	dbIndex, err := strconv.Atoi(string(line[0]))
	if err != nil {
		return protocol.NewErrorReply([]byte("ERR invalid DB index"))
	} else if errReply := db.checkIndex(dbIndex); errReply != nil {
		return errReply
	}
	conn.SelectDB(dbIndex)
	return protocol.OkReply()
}

func (db *MultiDB) copy_(conn redis.Connection, line redis.Line) redis.Reply {
	dbIndex := conn.GetDBIndex()
	single := db.dbPanicAt(dbIndex)
	allowReplace := false
	if len(line) > 2 {
		for i := 2; i < len(line); i++ {
			arg := string(bytes.ToLower(line[i]))
			if arg == "db" {
				if i+1 == len(line) {
					return protocol.SyntaxErrorReply()
				}
				idx, err := strconv.Atoi(string(line[i+1]))
				if err != nil {
					return protocol.SyntaxErrorReply()
				} else if errReply := db.checkIndex(idx); errReply != nil {
					return errReply
				}
				dbIndex = idx
				i++
			} else if arg == "replace" {
				allowReplace = true
			} else {
				return protocol.SyntaxErrorReply()
			}
		}
	}
	if utils.BytesEqual(line[0], line[1]) && dbIndex == conn.GetDBIndex() {
		return protocol.NewErrorReply([]byte("ERR source and destination objects are the same"))
	}
	valKey, dstKey := string(line[0]), string(line[1])
	val, ok := single.Get(valKey)
	if !ok {
		return protocol.IntReply(0)
	}
	dst := db.dbPanicAt(dbIndex)
	if _, ok = dst.Get(dstKey); ok && !allowReplace {
		return protocol.IntReply(0)
	}
	dst.Put(dstKey, val)
	if expireTime, ok := single.ttlMap.Get(valKey); ok {
		dst.Expire(dstKey, expireTime.(time.Time))
	}
	db.aofHandler.AddAOF(conn.GetDBIndex(), utils.StringsWithNameToLine("copy", line))
	return protocol.IntReply(1)
}
