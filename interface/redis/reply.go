package redis

// Reply 是对 RESP 协议中回复的消息的抽象
type Reply interface {
	GetBytes() []byte
}

// ErrorReply 是用于表示错误信息的 Reply
type ErrorReply interface {
	GetBytes() []byte
	Error() string
}
