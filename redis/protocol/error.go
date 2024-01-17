package protocol

import (
	"fmt"
	"godis-instruction/interface/redis"
)

type argumentCountErrorReply struct {
	cmd []byte
}

func (r *argumentCountErrorReply) GetBytes() []byte {
	return []byte(fmt.Sprintf("-ERR wrong argument count for '%s'\r\n", r.cmd))
}

func (r *argumentCountErrorReply) Error() string {
	return fmt.Sprintf("-ERR wrong argument count for '%s'", r.cmd)
}

func ArgumentCountErrorReply(cmd []byte) redis.ErrorReply {
	return &argumentCountErrorReply{cmd: cmd}
}
