package database

import "godis-instruction/interface/redis"

func noPrepare(_ redis.Line) (rKeys, wKeys []string) {
	return nil, nil
}
