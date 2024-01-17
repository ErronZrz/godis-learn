package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	rdb "github.com/hdt3213/rdb/parser"
	"godis-learn/config"
	"godis-learn/interface/redis"
	"godis-learn/lib/logger"
	"godis-learn/lib/utils"
	"godis-learn/redis/connection"
	"godis-learn/redis/parse"
	"godis-learn/redis/protocol"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	masterRole = iota
	slaveRole
)

type replicationStatus struct {
	mutex             sync.Mutex
	ctx               context.Context
	cancel            context.CancelFunc
	modCount          int32
	masterHost        string
	masterPort        int
	masterConn        net.Conn
	masterChan        <-chan *parse.Payload
	replicationId     string
	replicationOffset int64
	lastReceiveTime   time.Time
	running           sync.WaitGroup
}

func newReplicationStatus() *replicationStatus {
	return &replicationStatus{}
}

func startReplicationCron(db *MultiDB) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic", err)
			}
		}()
		ticker := time.Tick(time.Second)
		for range ticker {
			db.rep.slaveCron(db)
		}
	}()
}

func (r *replicationStatus) slaveCron(db *MultiDB) {
	if r.masterConn == nil {
		return
	}
	repTimeout := 60 * time.Second
	if config.Properties.ReplTimeout != 0 {
		repTimeout = time.Duration(config.Properties.ReplTimeout) * time.Second
	}
	if r.lastReceiveTime.Before(time.Now().Add(-repTimeout)) {
		err := r.reconnectMaster(db)
		if err != nil {
			logger.Error("send failed " + err.Error())
		}
		return
	}
	err := r.sendAck()
	if err != nil {
		logger.Error("send failed " + err.Error())
	}
}

func (r *replicationStatus) reconnectMaster(db *MultiDB) error {
	logger.Info("reconnecting with master")
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.stopSlaveWithMutex()
	go r.syncWithMaster(db)
	return nil
}

func (r *replicationStatus) sendAck() error {
	line := utils.StringsToLine(
		"REPLCONF",
		"ACK",
		strconv.FormatInt(r.replicationOffset, 10),
	)
	reply := protocol.ArrayReply(line)
	_, err := r.masterConn.Write(reply.GetBytes())
	return err
}

func (r *replicationStatus) stopSlaveWithMutex() {
	atomic.AddInt32(&(r.modCount), 1)
	if r.cancel != nil {
		r.cancel()
		r.running.Wait()
	}
	r.ctx = context.Background()
	r.cancel = nil
	if r.masterConn != nil {
		_ = r.masterConn.Close()
	}
	r.masterConn, r.masterChan = nil, nil
}

func (r *replicationStatus) syncWithMaster(db *MultiDB) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	r.mutex.Lock()
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.mutex.Unlock()
	if err := r.connectMaster(); err != nil {
		logger.Error("full sync failed during connecting " + err.Error())
	} else if err := r.pSync(db); err != nil {
		logger.Error("full sync failed during PSYNC " + err.Error())
	} else if err := r.receiveAOF(db); err != nil {
		logger.Error("full sync failed during receiving aof " + err.Error())
	}
}

func (r *replicationStatus) connectMaster() error {
	modCount := atomic.LoadInt32(&(r.modCount))
	addr := fmt.Sprintf("%s:%d", r.masterHost, r.masterPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		r.abortConnect()
		return errors.New("connect master failed " + err.Error())
	}
	masterChan := parse.StartParseStream(conn)
	if err = r.tryPing(conn, masterChan); err != nil {
		return err
	}
	if auth := config.Properties.MasterAuth; auth != "" {
		authCmdLine := utils.StringsToLine("auth", auth)
		if err = r.sendCmdToMaster(conn, authCmdLine, masterChan); err != nil {
			return err
		}
	}
	port := config.Properties.SlaveAnnouncePort
	if port == 0 {
		port = config.Properties.Port
	}
	portCmdLine := utils.StringsToLine("REPLCONF", "listening-port", strconv.Itoa(port))
	if err = r.sendCmdToMaster(conn, portCmdLine, masterChan); err != nil {
		return err
	}
	capCmdLine := utils.StringsToLine("REPLCONF", "capa", "psync2")
	if err = r.sendCmdToMaster(conn, capCmdLine, masterChan); err != nil {
		return err
	}
	r.mutex.Lock()
	if r.modCount != modCount {
		return nil
	}
	r.masterConn = conn
	r.masterChan = masterChan
	r.mutex.Unlock()
	return nil
}

func (r *replicationStatus) pSync(db *MultiDB) error {
	modCount := atomic.LoadInt32(&(r.modCount))
	cmdLine := utils.StringsToLine("psync", "?", "-1")
	req := protocol.ArrayReply(cmdLine)
	if _, err := r.masterConn.Write(req.GetBytes()); err != nil {
		return errors.New("send failed " + err.Error())
	}
	payload := <-r.masterChan
	if payload.Err != nil {
		return errors.New("read response failed: " + payload.Err.Error())
	}
	entireHeader, ok := protocol.FetchStatus(payload.Data)
	if !ok {
		return errors.New(fmt.Sprintf("illegal payload header: %s", payload.Data.GetBytes()))
	}
	headers := bytes.Split(entireHeader, []byte{' '})
	if len(headers) != 3 {
		return errors.New(fmt.Sprintf("illegal payload header: %s", entireHeader))
	}
	logger.Info("receive psync header from master")
	payload = <-r.masterChan
	if payload.Err != nil {
		return errors.New("read response failed: " + payload.Err.Error())
	}
	body, ok := protocol.FetchBulkString(payload.Data)
	if !ok {
		return errors.New(fmt.Sprintf("illegal payload body: %s", payload.Data.GetBytes()))
	}
	logger.Infof("receive %d bytes of rdb from master", len(body))
	rdbDecoder := rdb.NewDecoder(bytes.NewReader(body))
	rdbHolder := NewBasicMultiDB()
	if err := dumpRDB(rdbDecoder, rdbHolder); err != nil {
		return errors.New("dump rdb failed " + err.Error())
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.modCount != modCount {
		return nil
	}
	id := headers[1]
	offset, err := strconv.ParseInt(string(headers[2]), 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("get illegal replication offset: %s", headers[2]))
	}
	logger.Infof("full rsync from master: %s", id)
	logger.Infof("current offset: %d", int(offset))
	r.replicationId, r.replicationOffset = string(id), offset
	for i, value := range rdbHolder.dbs {
		single := value.Load().(*DB)
		_ = db.storeNewDB(i, single)
	}
	r.masterChan = parse.StartParseStream(r.masterConn)
	return nil
}

func (r *replicationStatus) receiveAOF(db *MultiDB) error {
	cc := connection.NewClientConn(r.masterConn)
	cc.SetRole(connection.ReplicationClient)
	r.mutex.Lock()
	modCount := r.modCount
	done := r.ctx.Done()
	r.mutex.Unlock()
	if done == nil {
		return nil
	}
	r.running.Add(1)
	defer r.running.Done()
	for {
		select {
		case payload, open := <-r.masterChan:
			if !open {
				return errors.New("master channel unexpected close")
			}
			if payload.Err != nil {
				return payload.Err
			}
			line, ok := protocol.FetchArrayArgs(payload.Data)
			if !ok {
				return errors.New(fmt.Sprintf("unexpected payload: %s", payload.Data.GetBytes()))
			}
			r.mutex.Lock()
			if r.modCount != modCount {
				return nil
			}
			db.Execute(cc, line)
			n := len(payload.Data.GetBytes())
			r.replicationOffset += int64(n)
			logger.Infof("receive %d bytes from master, current offset %d", n, int(r.replicationOffset))
			r.mutex.Unlock()
		case <-done:
			return nil
		}
	}
}

func (r *replicationStatus) abortConnect() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.masterHost = ""
	r.masterPort = 0
	r.replicationId = ""
	r.replicationOffset = -1
	r.stopSlaveWithMutex()
}

func (r *replicationStatus) tryPing(c net.Conn, masterChan <-chan *parse.Payload) error {
	pingLine := utils.StringsToLine("ping")
	pingReq := protocol.ArrayReply(pingLine)
	_, err := c.Write(pingReq.GetBytes())
	if err != nil {
		return errors.New("send failed " + err.Error())
	}
	pingResp := <-masterChan
	if pingResp.Err != nil {
		return errors.New("read response failed: " + pingResp.Err.Error())
	}
	reply := pingResp.Data
	if protocol.CheckErrorReply(reply) {
		if hasNonePrefix(reply.(redis.ErrorReply).Error(), "NOAUTH", "NOPERM", "ERR operation not permitted") {
			info := fmt.Sprintf("Error reply to PING from master: %s", reply.GetBytes())
			logger.Error(info)
			r.abortConnect()
			return errors.New(info)
		}
	}
	return nil
}

func (r *replicationStatus) sendCmdToMaster(c net.Conn, line redis.Line, masterChan <-chan *parse.Payload) error {
	req := protocol.ArrayReply(line)
	_, err := c.Write(req.GetBytes())
	if err != nil {
		r.abortConnect()
		return errors.New("send failed " + err.Error())
	}
	resp := <-masterChan
	if resp.Err != nil {
		r.abortConnect()
		return errors.New("read reaponse failed" + resp.Err.Error())
	}
	if !protocol.CheckOKReply(resp.Data) {
		r.abortConnect()
		return errors.New(fmt.Sprintf("unexpected auth response: %s", resp.Data.GetBytes()))
	}
	return nil
}

func (r *replicationStatus) close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.stopSlaveWithMutex()
	return nil
}

func (db *MultiDB) execSlaveOf(conn redis.Connection, line redis.Line) redis.Reply {
	return protocol.OkReply()
}

func hasNonePrefix(s string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, prefix) {
			return false
		}
	}
	return true
}
