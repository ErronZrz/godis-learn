package persistent

import (
	"godis-learn/config"
	"godis-learn/interface/dbinterface"
	"godis-learn/lib/logger"
	"godis-learn/lib/utils"
	"godis-learn/redis/protocol"
	"io"
	"os"
	"strconv"
	"time"
)

func (h *Handler) newRewriteHandler() *Handler {
	return &Handler{
		aofFilename: h.aofFilename,
		db:          h.tempProducer(),
	}
}

type RewriteContext struct {
	tempFile *os.File
	fileSize int64
	dbIndex  int
}

func (h *Handler) Rewrite() error {
	var err error
	if ctx, err := h.StartRewrite(); err == nil {
		if err = h.DoRewrite(ctx); err == nil {
			return h.FinishRewrite(ctx)
		}
	}
	return err
}

func (h *Handler) StartRewrite() (*RewriteContext, error) {
	h.aofMutex.Lock()
	defer h.aofMutex.Unlock()
	if err := h.aofFile.Sync(); err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}
	fileInfo, _ := os.Stat(h.aofFilename)
	fileSize := fileInfo.Size()
	file, err := os.CreateTemp("", "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteContext{
		tempFile: file,
		fileSize: fileSize,
		dbIndex:  h.currentIndex,
	}, nil
}

func (h *Handler) DoRewrite(ctx *RewriteContext) error {
	tempFile := ctx.tempFile
	tempAOF := h.newRewriteHandler()
	tempAOF.LoadAOF(int(ctx.fileSize))
	for i := 0; i < config.Properties.DatabaseCount; i++ {
		data := protocol.ArrayReply(utils.StringsToLine("SELECT", strconv.Itoa(i))).GetBytes()
		if _, err := tempFile.Write(data); err != nil {
			return err
		}
		tempAOF.db.ForEach(i, func(key string, val *dbinterface.EntryValue, expiration *time.Time) bool {
			if cmd := ValueToReply(key, val); cmd != nil {
				_, _ = tempFile.Write(cmd.GetBytes())
			}
			if expiration != nil {
				if expireArgs := expireToArgs([]byte(key), *expiration); expireArgs != nil {
					_, _ = tempFile.Write(protocol.ArrayReply(expireArgs).GetBytes())
				}
			}
			return true
		})
	}
	return nil
}

func (h *Handler) FinishRewrite(ctx *RewriteContext) error {
	h.aofMutex.Lock()
	defer h.aofMutex.Unlock()
	tempFile := ctx.tempFile
	src, err := os.Open(h.aofFilename)
	if err != nil {
		logger.Error("open aofFilename failed: " + err.Error())
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(src)
	_, err = src.Seek(ctx.fileSize, 0)
	if err != nil {
		logger.Error("seek failed: " + err.Error())
		return err
	}
	data := protocol.ArrayReply(utils.StringsToLine("SELECT", strconv.Itoa(ctx.dbIndex))).GetBytes()
	if _, err = tempFile.Write(data); err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return err
	}
	if _, err = io.Copy(tempFile, src); err != nil {
		logger.Error("copy aof file failed: " + err.Error())
		return err
	}
	_ = h.aofFile.Close()
	_ = os.Rename(tempFile.Name(), h.aofFilename)
	aofFile, err := os.OpenFile(h.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	data = protocol.ArrayReply(utils.StringsToLine("SELECT", strconv.Itoa(h.currentIndex))).GetBytes()
	if _, err = aofFile.Write(data); err != nil {
		panic(err)
	}
	h.aofFile = aofFile
	return nil
}
