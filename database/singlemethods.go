package database

import (
	"godis-learn/interface/dbinterface"
	"godis-learn/lib/logger"
	"godis-learn/lib/timewheel"
	"time"
)

func (db *DB) Put(key string, value *dbinterface.EntryValue) {
	db.m.Put(key, value)
}

func (db *DB) PutIfAbsent(key string, value *dbinterface.EntryValue) (ok bool) {
	return db.m.PutIfAbsent(key, value)
}

func (db *DB) PutIfExists(key string, value *dbinterface.EntryValue) (ok bool) {
	return db.m.PutIfExists(key, value)
}

func (db *DB) Get(key string) (*dbinterface.EntryValue, bool) {
	value, ok := db.m.Get(key)
	if !ok || db.CheckExpired(key) {
		return nil, false
	}
	return value.(*dbinterface.EntryValue), true
}

func (db *DB) Delete(key string) (ok bool) {
	ok = db.m.Delete(key)
	db.ttlMap.Delete(key)
	timewheel.Cancel(withExpirePrefix(key))
	return ok
}

func (db *DB) DeleteKeys(keys []string) int {
	res := 0
	for _, key := range keys {
		if db.Delete(key) {
			res++
		}
	}
	return res
}

func (db *DB) Expire(key string, t time.Time) {
	db.ttlMap.Put(key, t)
	taskKey := withExpirePrefix(key)
	job := func() {
		keys := []string{key}
		db.RWLockKeys(nil, keys)
		defer db.RWUnlockKeys(nil, keys)
		logger.Info(taskKey)
		db.CheckExpired(key)
	}
	timewheel.JobAtTime(job, taskKey, t)
}

func (db *DB) Persist(key string) {
	db.ttlMap.Delete(key)
	timewheel.Cancel(withExpirePrefix(key))
}

func (db *DB) CheckExpired(key string) bool {
	expireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expired := time.Now().After(expireTime.(time.Time))
	if expired {
		db.Delete(key)
	}
	return expired
}

func (db *DB) ForEach(p dbinterface.EntryProcessor) {
	db.m.ForEach(func(key string, value any) bool {
		var timePtr *time.Time
		expireTime, ok := db.ttlMap.Get(key)
		if ok {
			tmp := expireTime.(time.Time)
			timePtr = &tmp
		}
		return p(key, value.(*dbinterface.EntryValue), timePtr)
	})
}

func withExpirePrefix(key string) string {
	return "expire:" + key
}
