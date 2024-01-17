package database

import "godis-learn/interface/redis"

func noPrepare(_ redis.Line) (rKeys, wKeys []string) {
	return nil, nil
}
