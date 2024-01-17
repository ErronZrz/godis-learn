package client

import (
	"godis-instruction/lib/sync/wait"
	"net"
)

type OldClient struct {
	// TCP 连接
	Conn net.Conn
	// 当服务端发送数据时进入等待，阻止其他 goroutine 关闭连接
	Waiting wait.Wait
}
