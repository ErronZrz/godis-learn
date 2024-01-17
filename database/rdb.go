package database

import (
	"github.com/hdt3213/rdb/core"
	rdb "github.com/hdt3213/rdb/parser"
	"godis-instruction/config"
	"godis-instruction/datastruct/dict"
	"godis-instruction/datastruct/list"
	"godis-instruction/datastruct/set"
	"godis-instruction/interface/dbinterface"
	"godis-instruction/lib/logger"
	"os"
)

func loadRDBFile(db *MultiDB) {
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		logger.Error("open rdb file failed " + err.Error())
		return
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(rdbFile)
	decoder := rdb.NewDecoder(rdbFile)
	err = dumpRDB(decoder, db)
	if err != nil {
		logger.Error("dump rdb file failed " + err.Error())
	}
}

func dumpRDB(decoder *core.Decoder, db *MultiDB) error {
	return decoder.Parse(func(obj rdb.RedisObject) bool {
		single := db.dbPanicAt(obj.GetDBIndex())
		switch obj.GetType() {
		case rdb.StringType:
			strObj := obj.(*rdb.StringObject)
			single.Put(obj.GetKey(), &dbinterface.EntryValue{V: strObj.Value})
		case rdb.ListType:
			listObj := obj.(*rdb.ListObject)
			l := list.NewQuickList()
			for _, v := range listObj.Values {
				l.Add(v)
			}
			single.Put(obj.GetKey(), &dbinterface.EntryValue{V: l})
		case rdb.HashType:
			hashObj := obj.(*rdb.HashObject)
			m := dict.NewSimpleHashMap()
			for k, v := range hashObj.Hash {
				m.Put(k, v)
			}
			single.Put(obj.GetKey(), &dbinterface.EntryValue{V: m})
		case rdb.ZSetType:
			setObj := obj.(*rdb.ZSetObject)
			s := set.NewSortedSet()
			for _, e := range setObj.Entries {
				s.Add(e.Member, e.Score)
			}
			single.Put(obj.GetKey(), &dbinterface.EntryValue{V: s})
		}
		if obj.GetExpiration() != nil {
			single.Expire(obj.GetKey(), *obj.GetExpiration())
		}
		return true
	})
}
