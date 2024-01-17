package redis

import (
	"bytes"
)

type Line [][]byte

type Connection interface {
	Write([]byte) error
	SetPassword(string)
	GetPassword() string
	Subscribe(string)
	Unsubscribe(string)
	SubscriberCount() int
	GetChannels() []string
	CheckMultiMode() bool
	SetMultiMode(bool)
	GetQueuedCmdLines() []Line
	EnqueueCmdLine(Line)
	ClearQueue()
	GetWatching() map[string]uint32
	AddTxError(error)
	GetTxErrors() []error
	GetDBIndex() int
	SelectDB(int)
	SetRole(int32)
	GetRole() int32
}

func (l Line) CommandName() []byte {
	return bytes.ToLower(l[0])
}

func (l Line) CommandContent() [][]byte {
	return l[1:]
}
