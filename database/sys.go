package database

import (
	"godis-learn/config"
	"godis-learn/interface/redis"
	"godis-learn/redis/protocol"
)

func Ping(_ *DB, line redis.Line) redis.Reply {
	switch len(line) {
	case 0:
		return protocol.PongReply()
	case 1:
		return protocol.StatusReply(line[0])
	default:
		return protocol.NewErrorReply([]byte("ERR wrong number of arguments for 'ping' command"))
	}
}

func Auth(conn redis.Connection, line redis.Line) redis.Reply {
	if len(line) != 1 {
		return protocol.NewErrorReply([]byte("ERR wrong number of arguments for 'auth' command"))
	}
	if config.Properties.RequirePass == "" {
		return protocol.NewErrorReply([]byte("ERR Client sent AUTH, but no password is set"))
	}
	password := string(line[0])
	conn.SetPassword(password)
	if config.Properties.RequirePass != password {
		return protocol.NewErrorReply([]byte("ERR invalid password"))
	}
	return protocol.OkReply()
}

func authenticated(conn redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return conn.GetPassword() == config.Properties.RequirePass
}

func init() {
	RegisterCommand("ping", Ping, noPrepare, nil, -1, readOnlyFlag)
}
