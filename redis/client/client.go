package client

import (
	"errors"
	"godis-learn/interface/redis"
	"godis-learn/lib/logger"
	"godis-learn/lib/sync/wait"
	"godis-learn/redis/parse"
	"godis-learn/redis/protocol"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	chanSize = 256
	timeout  = 3 * time.Second
	running  = iota
	closed
)

type request struct {
	id        uint64
	line      redis.Line
	reply     redis.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

type Client struct {
	addr        string
	conn        net.Conn
	pendingChan chan *request
	waitingChan chan *request
	ticker      *time.Ticker
	status      int32
	working     *sync.WaitGroup
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingChan: make(chan *request, chanSize),
		waitingChan: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

func (c *Client) Start() {
	c.ticker = time.NewTicker(10 * time.Second)
	go c.handleWrite()
	go c.handleRead()
	go c.heartbeat()
	atomic.StoreInt32(&c.status, running)
}

func (c *Client) Close() {
	atomic.StoreInt32(&c.status, closed)
	c.ticker.Stop()
	close(c.pendingChan)
	c.working.Wait()
	_ = c.conn.Close()
	close(c.waitingChan)
}

func (c *Client) Send(line redis.Line) redis.Reply {
	if atomic.LoadInt32(&c.status) != running {
		return protocol.NewErrorReply([]byte("client closed"))
	}
	req := &request{
		line:      line,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	c.working.Add(1)
	defer c.working.Done()
	c.pendingChan <- req
	checkTimeout := req.waiting.WaitWithTimeout(timeout)
	if checkTimeout {
		return protocol.NewErrorReply([]byte("server time out"))
	}
	if req.err != nil {
		return protocol.NewErrorReply([]byte("request failed"))
	}
	return req.reply
}

func (c *Client) handleWrite() {
	for req := range c.pendingChan {
		c.doRequest(req)
	}
}

func (c *Client) handleRead() {
	ch := parse.StartParseStream(c.conn)
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&c.status)
			if status == closed {
				return
			}
			c.reconnect()
			return
		}
		c.finishRequest(payload.Data)
	}
}

func (c *Client) heartbeat() {
	for range c.ticker.C {
		c.doHeartbeat()
	}
}

func (c *Client) doHeartbeat() {
	req := &request{
		line:      redis.Line{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	c.working.Add(1)
	defer c.working.Done()
	c.pendingChan <- req
	req.waiting.WaitWithTimeout(timeout)
}

func (c *Client) doRequest(req *request) {
	if req == nil || len(req.line) == 0 {
		return
	}
	reply := protocol.ArrayReply(req.line)
	bytes := reply.GetBytes()
	var err error
	for i := 0; i < 3; i++ {
		_, err = c.conn.Write(bytes)
		if err == nil {
			break
		}
		errStr := err.Error()
		if !(strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline exceeded")) {
			break
		}
	}
	if err != nil {
		req.err = err
		req.waiting.Done()
	} else {
		c.waitingChan <- req
	}
}

func (c *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	req := <-c.waitingChan
	if req == nil {
		return
	}
	req.reply = reply
	if req.waiting != nil {
		req.waiting.Done()
	}
}

func (c *Client) reconnect() {
	logger.Infof("reconnect with: %s", c.addr)
	_ = c.conn.Close()
	for i := 0; i < 3; i++ {
		var err error
		c.conn, err = net.Dial("tcp", c.addr)
		if err != nil {
			logger.Error("reconnect error: " + err.Error())
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	if c.conn == nil {
		c.Close()
		return
	}
	close(c.waitingChan)
	for req := range c.waitingChan {
		req.err = errors.New("connection closed")
		req.waiting.Done()
	}
	c.waitingChan = make(chan *request, chanSize)
	go c.handleRead()
}
