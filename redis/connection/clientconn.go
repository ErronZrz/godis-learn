package connection

import (
	"godis-learn/interface/redis"
	"godis-learn/lib/sync/wait"
	"net"
	"sync"
	"time"
)

const (
	NormalClient = iota
	ReplicationClient
)

type ClientConn struct {
	conn          net.Conn
	waitingReply  wait.Wait
	mutex         sync.Mutex
	subsMap       map[string]bool
	password      string
	handlingMulti bool
	queue         []redis.Line
	watching      map[string]uint32
	txErrors      []error
	selectedIndex int
	role          int32
}

func NewClientConn(conn net.Conn) *ClientConn {
	return &ClientConn{conn: conn}
}

func (c *ClientConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *ClientConn) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	return c.conn.Close()
}

func (c *ClientConn) Write(s []byte) error {
	if len(s) == 0 {
		return nil
	}
	c.waitingReply.Add(1)
	defer func(c *ClientConn) {
		c.waitingReply.Done()
	}(c)
	_, err := c.conn.Write(s)
	return err
}

func (c *ClientConn) SetPassword(password string) {
	c.password = password
}

func (c *ClientConn) GetPassword() string {
	return c.password
}

func (c *ClientConn) Subscribe(channel string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.subsMap == nil {
		c.subsMap = make(map[string]bool)
	}
	c.subsMap[channel] = true
}

func (c *ClientConn) Unsubscribe(channel string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.SubscriberCount() > 0 {
		delete(c.subsMap, channel)
	}
}

func (c *ClientConn) SubscriberCount() int {
	return len(c.subsMap)
}

func (c *ClientConn) GetChannels() []string {
	if c.subsMap == nil {
		return make([]string, 0)
	}
	res := make([]string, c.SubscriberCount())
	i := 0
	for channel := range c.subsMap {
		res[i] = channel
		i++
	}
	return res
}

func (c *ClientConn) CheckMultiMode() bool {
	return c.handlingMulti
}

func (c *ClientConn) SetMultiMode(mode bool) {
	if !mode {
		c.watching = nil
		c.queue = nil
	}
	c.handlingMulti = mode
}

func (c *ClientConn) GetQueuedCmdLines() []redis.Line {
	return c.queue
}

func (c *ClientConn) EnqueueCmdLine(line redis.Line) {
	c.queue = append(c.queue, line)
}

func (c *ClientConn) ClearQueue() {
	c.queue = nil
}

func (c *ClientConn) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

func (c *ClientConn) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

func (c *ClientConn) GetTxErrors() []error {
	return c.txErrors
}

func (c *ClientConn) GetDBIndex() int {
	return c.selectedIndex
}

func (c *ClientConn) SelectDB(dbIndex int) {
	c.selectedIndex = dbIndex
}

func (c *ClientConn) SetRole(role int32) {
	c.role = role
}

func (c *ClientConn) GetRole() int32 {
	if c == nil {
		return NormalClient
	}
	return c.role
}
