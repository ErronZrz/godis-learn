package persistent

import (
	"godis-instruction/config"
	"godis-instruction/interface/dbinterface"
	"godis-instruction/interface/redis"
	"godis-instruction/lib/logger"
	"godis-instruction/lib/utils"
	"godis-instruction/redis/connection"
	"godis-instruction/redis/parse"
	"godis-instruction/redis/protocol"
	"io"
	"os"
	"strconv"
	"sync"
)

const (
	aofQueueSize = 1 << 16
)

type payload struct {
	cmdLine redis.Line
	dbIndex int
}

type Handler struct {
	db            dbinterface.EmbedDB
	tempProducer  func() dbinterface.EmbedDB
	aofChan       chan *payload
	aofFile       *os.File
	aofFilename   string
	aofFinishChan chan struct{}
	aofMutex      sync.RWMutex
	currentIndex  int
}

func NewAOFHandler(db dbinterface.EmbedDB, producer func() dbinterface.EmbedDB) (*Handler, error) {
	filename := config.Properties.AppendFilename
	aofFile, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	res := &Handler{
		db:            db,
		tempProducer:  producer,
		aofChan:       make(chan *payload, aofQueueSize),
		aofFile:       aofFile,
		aofFilename:   filename,
		aofFinishChan: make(chan struct{}),
	}
	res.LoadAOF(0)
	go func() {
		res.handleAOF()
	}()
	return res, nil
}

func (h *Handler) LoadAOF(maxBytes int) {
	aofChan := h.aofChan
	h.aofChan = nil
	defer func(ac chan *payload) {
		h.aofChan = ac
	}(aofChan)
	file, err := os.Open(h.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(file)
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	payloadChan := parse.StartParseStream(reader)
	fakeConn := connection.NewFakeConn()
	for p := range payloadChan {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		args, ok := protocol.FetchArrayArgs(p.Data)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		reply := h.db.Execute(fakeConn, args)
		if protocol.CheckErrorReply(reply) {
			logger.Error("exec error", reply.GetBytes())
		}
	}
}

func (h *Handler) AddAOF(dbIndex int, line redis.Line) {
	if config.Properties.AppendOnly && h.aofChan != nil {
		h.aofChan <- &payload{
			cmdLine: line,
			dbIndex: dbIndex,
		}
	}
}

func (h *Handler) Close() {
	if h.aofFile == nil {
		return
	}
	close(h.aofChan)
	<-h.aofFinishChan
	if err := h.aofFile.Close(); err != nil {
		logger.Warn(err)
	}
}

func (h *Handler) handleAOF() {
	h.currentIndex = 0
	for p := range h.aofChan {
		h.aofMutex.RLock()
		if p.dbIndex != h.currentIndex {
			data := protocol.ArrayReply(utils.StringsToLine("SELECT", strconv.Itoa(p.dbIndex))).GetBytes()
			_, err := h.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue
			}
			h.currentIndex = p.dbIndex
		}
		data := protocol.ArrayReply(p.cmdLine).GetBytes()
		_, err := h.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
		h.aofMutex.RUnlock()
	}
	h.aofFinishChan <- struct{}{}
}
