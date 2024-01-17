package tcp

import (
	"bufio"
	"context"
	"godis-instruction/lib/logger"
	"godis-instruction/lib/sync/atomic"
	"godis-instruction/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

// EchoClient 是客户端的抽象
type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

func (c *EchoClient) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second)
	return c.Conn.Close()
}

type EchoHandler struct {
	// 存活的连接集合，使用 Map 来表示
	activeCli sync.Map
	closing   atomic.Boolean
}

func CreateEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (h *EchoHandler) Handle(_ context.Context, conn net.Conn) {
	// 如果处于关闭状态则拒绝处理
	if h.closing.Get() {
		_ = conn.Close()
		return
	}
	cli := &EchoClient{
		Conn: conn,
	}
	// 将连接保存到集合中
	h.activeCli.Store(cli, struct{}{})
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("Connection closed.")
				h.activeCli.Delete(cli)
			} else {
				logger.Warn(err)
			}
			return
		}
		// 写数据前，先置为 Waiting 状态，阻止连接关闭
		cli.Waiting.Add(1)
		_, _ = conn.Write([]byte(msg))
		cli.Waiting.Done()
	}
}

func (h *EchoHandler) Close() (err error) {
	logger.Info("Handler shutting down...")
	h.closing.Set(true)
	h.activeCli.Range(func(key, _ any) bool {
		err = key.(*EchoClient).Close()
		return true
	})
	return
}
