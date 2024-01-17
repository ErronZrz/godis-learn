package persistent

import (
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
	"godis-learn/config"
	"godis-learn/datastruct/dict"
	"godis-learn/datastruct/list"
	"godis-learn/datastruct/set"
	"godis-learn/interface/dbinterface"
	"os"
	"strconv"
	"time"
)

func (h *Handler) Rewrite2RDB() error {
	var err error
	if ctx, err := h.StartRewrite(); err == nil {
		if err = h.rewrite2RDB(ctx); err == nil {
			rdbFileName := config.Properties.RDBFilename
			if rdbFileName == "" {
				rdbFileName = "dump.rdb"
			}
			if err = ctx.tempFile.Close(); err == nil {
				return os.Rename(ctx.tempFile.Name(), rdbFileName)
			}
		}
	}
	return err
}

func (h *Handler) rewrite2RDB(ctx *RewriteContext) error {
	tempHandler := h.newRewriteHandler()
	tempHandler.LoadAOF(int(ctx.fileSize))
	encoderPtr := core.NewEncoder(ctx.tempFile).EnableCompress()
	if err := encoderPtr.WriteHeader(); err != nil {
		return err
	}
	auxMap := map[string]string{
		"redis-ver":    "6.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10),
	}
	for k, v := range auxMap {
		if err := encoderPtr.WriteAux(k, v); err != nil {
			return err
		}
	}
	for i := 0; i < config.Properties.DatabaseCount; i++ {
		dataSize, ttlMapSize := tempHandler.db.GetDBSize(i)
		if dataSize == 0 {
			continue
		}
		if err := encoderPtr.WriteDBHeader(uint(i), uint64(dataSize), uint64(ttlMapSize)); err != nil {
			return err
		}
		var err error
		tempHandler.db.ForEach(i, func(key string, val *dbinterface.EntryValue, expireTime *time.Time) bool {
			err = consumer(key, val, expireTime, encoderPtr)
			return err == nil
		})
		if err != nil {
			return err
		}
	}
	return encoderPtr.WriteEnd()
}

func consumer(key string, val *dbinterface.EntryValue, expireTime *time.Time, encoder *encoder.Encoder) error {
	var opts []any
	if expireTime != nil {
		opts = append(opts, core.WithTTL(uint64(expireTime.UnixNano()/1_000_000)))
	}
	switch v := val.V.(type) {
	case []byte:
		return encoder.WriteStringObject(key, v, opts...)
	case list.List:
		res := make([][]byte, 0, v.Size())
		v.ForEach(func(i int, s any) bool {
			res = append(res, s.([]byte))
			return true
		})
		return encoder.WriteListObject(key, res, opts...)
	case *set.HashSet:
		res := make([][]byte, 0, v.Size())
		v.ForEach(func(s string) bool {
			res = append(res, []byte(s))
			return true
		})
		return encoder.WriteSetObject(key, res, opts...)
	case dict.HashMap:
		res := make(map[string][]byte)
		v.ForEach(func(key string, s any) bool {
			res[key] = s.([]byte)
			return true
		})
		return encoder.WriteHashMapObject(key, res, opts...)
	case *set.SortedSet:
		var res []*model.ZSetEntry
		v.ForEachRankBetween(0, v.Size(), true, func(e *set.Element) bool {
			res = append(res, &model.ZSetEntry{
				Member: e.Member,
				Score:  e.Score,
			})
			return true
		})
		return encoder.WriteZSetObject(key, res, opts...)
	}
	return nil
}
