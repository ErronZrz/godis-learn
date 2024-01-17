package connection

import (
	"bytes"
	"godis-learn/interface/redis"
)

type FakeConn struct {
	redis.Connection
	buf bytes.Buffer
}

func NewFakeConn() *FakeConn {
	return &FakeConn{}
}

func (c *FakeConn) Write(s []byte) error {
	c.buf.Write(s)
	return nil
}

func (c *FakeConn) Clean() {
	c.buf.Reset()
}

func (c *FakeConn) GetBytes() []byte {
	return c.buf.Bytes()
}
