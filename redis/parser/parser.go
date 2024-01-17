package parser

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"godis-learn/interface/redis"
	"godis-learn/lib/logger"
	"godis-learn/redis/protocol"
	"io"
	"runtime/debug"
	"strconv"
)

// Payload 储存了 redis.Reply 或是一个 error
type Payload struct {
	Data redis.Reply
	Err  error
}

// ParseStream 从 io.Reader 中读取数据，并将对应的 Payload 送进通道
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// readState 存储着从读取到解析的过程中需要传递的信息
type readState struct {
	expectedArgCount int
	msgType          byte
	args             [][]byte
	bulkLen          int64
}

// keepState 用于判断本次读之前是否需要清空 state
func (s *readState) keepState() bool {
	return s.parsingBody() && !s.finished()
}

const bodyBulkLen = -(0x3F3F3F3F)

// parsingBody 用于判断当前是否正在解析 body
func (s *readState) parsingBody() bool {
	return s.bulkLen == bodyBulkLen || s.bulkLen > 0
}

// finished 当 Bulk String 读取完毕或者 Array 已经读取满时，返回 true
func (s *readState) finished() bool {
	// 这里需不需要判断 len(s.args) > 0 呢
	return len(s.args) == s.expectedArgCount
}

// parse0 负责执行解析
func parse0(reader io.Reader, ch chan<- *Payload) {
	// 对于捕获到的 error 进行记录
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	state := readState{}
	for {
		if !state.keepState() {
			state = readState{}
		}
		readLen := int(state.bulkLen)
		if readLen > 0 {
			// 如果是读取固定长度，则长度需要算上末尾的 CRLF
			readLen += 2
		}
		// logger.Info(state)
		msg, hasIoErr, err := readNext(bufReader, readLen)
		// logger.Infof("len=%d, msg=\"%s\"", len(msg), string(msg))
		if err != nil {
			if hasIoErr {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			ch <- &Payload{Err: err}
			continue
		}
		if !state.parsingBody() {
			parseSingleLine(msg, &state, ch)
		} else {
			parseBody(msg, &state, ch)
		}
	}
}

// readNext 会从给定的 bufio.Reader 读取一行或指定长度，并将内容返回
func readNext(reader *bufio.Reader, readLen int) ([]byte, bool, error) {
	var msg []byte
	var err error
	if readLen <= 0 {
		// 只读一行，使用 ReadBytes
		msg, err = reader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if l := len(msg); l == 0 || msg[l-2] != '\r' {
			return nil, false, protocolError(msg)
		}
	} else {
		// 读取指定长度的 Bulk String，使用 ReadFull 来保证二进制安全
		msg = make([]byte, readLen)
		_, err = io.ReadFull(reader, msg)
		if err != nil {
			return nil, true, err
		}
	}
	return msg, false, nil
}

// parseSingleLine 解析一行的内容，该行一定不带 CRLF，可能是 header 也可能是普通的一行
func parseSingleLine(msg []byte, state *readState, ch chan<- *Payload) {
	var err error
	if msg[0] == '*' {
		err = parseMultiBulkHeader(msg, state)
		if err != nil {
			ch <- &Payload{Err: err}
			return
		}
		if state.expectedArgCount == 0 {
			ch <- &Payload{Data: protocol.EmptyArrayReply()}
		}
	} else if msg[0] == '$' {
		err = parseBulkHeader(msg, state)
		if err != nil {
			ch <- &Payload{Err: err}
			return
		}
		if state.bulkLen == -1 {
			ch <- &Payload{Data: protocol.NullBulkStringReply()}
		}
	} else {
		reply, err := parseSimpleLine(msg)
		ch <- &Payload{
			Data: reply,
			Err:  err,
		}
	}
}

// parseMultiBulkHeader 用于解析 Array 类型的 header
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var nLines uint64
	nLines, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil || nLines < 0 {
		return protocolError(msg)
	}
	state.expectedArgCount = int(nLines)
	if nLines > 0 {
		state.msgType = '*'
		// 开始读取 Array 的第一个 header
		state.bulkLen = bodyBulkLen
		state.args = make([][]byte, 0, nLines)
	}
	return nil
}

// parseBulkHeader 用于解析 Bulk String 类型的 header
func parseBulkHeader(msg []byte, state *readState) error {
	bulkLen, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil || bulkLen < -1 {
		return protocolError(msg)
	}
	state.bulkLen = bulkLen
	if bulkLen >= 0 {
		state.msgType = '$'
		state.expectedArgCount = 1
		state.args = make([][]byte, 0, 1)
	}
	if bulkLen == 0 {
		// 使得下一次 parsingBody 判定为 true
		state.bulkLen = bodyBulkLen
	}
	return nil
}

// parseSimpleLine 用于解析 Simple String, Error 或 Integer
func parseSimpleLine(msg []byte) (redis.Reply, error) {
	trimmed := bytes.TrimSuffix(msg, []byte{'\r', '\n'})
	var reply redis.Reply
	switch msg[0] {
	case '+':
		reply = protocol.StatusReply(trimmed[1:])
	case '-':
		reply = protocol.NewErrorReply(trimmed[1:])
	case ':':
		value, err := strconv.ParseInt(string(trimmed[1:]), 10, 64)
		if err != nil {
			return nil, protocolError(msg)
		}
		reply = protocol.IntReply(value)
	default:
		args := bytes.Split(trimmed, []byte{' '})
		reply = protocol.ArrayReply(args)
	}
	return reply, nil
}

// parseBody 用于解析 Array 或 BulkString 的 body
func parseBody(msg []byte, state *readState, ch chan<- *Payload) {
	err := prepareArgs(msg, state)
	payload := &Payload{Err: err}
	if err != nil {
		ch <- payload
		return
	}
	if state.finished() {
		if state.msgType == '*' {
			payload.Data = protocol.ArrayReply(state.args)
		} else if state.msgType == '$' {
			payload.Data = protocol.BulkStringReply(state.args[0])
		}
		ch <- payload
	}
}

// prepareArgs 负责将一行 header 或者 Bulk String 添加到 state 的 args 中
func prepareArgs(msg []byte, state *readState) error {
	line := msg[:len(msg)-2]
	if len(line) > 0 && line[0] == '$' {
		bulkLen, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil || bulkLen < -1 {
			return protocolError(msg)
		}
		if bulkLen == -1 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		} else if bulkLen == 0 {
			// 使得下一次 parsingBody 判定为 true
			state.bulkLen = bodyBulkLen
		} else {
			state.bulkLen = bulkLen
		}
	} else {
		state.args = append(state.args, line)
		// 读取了一次 Bulk String，下一行一定是 header
		state.bulkLen = bodyBulkLen
	}
	return nil
}

// protocolError 根据指定字符串生成一个异常
func protocolError(msg []byte) error {
	err := errors.New(fmt.Sprintf("Protocol error: %s", string(msg)))
	// logger.Warn(string(debug.Stack()))
	return err
}
