package tcp

import (
	"bufio"
	"fmt"
	"godis-instruction/lib/logger"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestListenAndServe(t *testing.T) {
	closeChan := make(chan struct{})
	lr, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := lr.Addr().String()
	logger.Infof("Address of listener: %s", addr)
	go ListenAndServe(lr, CreateEchoHandler(), closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 100; i += 10 {
		randInt := rand.Int()
		logger.Infof("Generate int %d", randInt)
		_, err = conn.Write([]byte(fmt.Sprintf("%d\n", randInt)))
		if err != nil {
			t.Error(err)
			return
		}
		reader := bufio.NewReader(conn)
		line, _, err := reader.ReadLine()
		if err != nil {
			t.Error(err)
			return
		}
		if rcv, _ := strconv.Atoi(string(line)); rcv != randInt {
			t.Error("Wrong response.")
			return
		}
	}
	_ = conn.Close()
	for i := 0; i < 5; i++ {
		_, _ = net.Dial("tcp", addr)
	}
	closeChan <- struct{}{}
	time.Sleep(time.Second)
}
