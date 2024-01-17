package protocol

import (
	"bytes"
	"fmt"
	"godis-instruction/interface/redis"
	"godis-instruction/lib/utils"
)

var (
	emptyMultiBulkBytes = []byte{'*', '0', '\r', '\n'}
	nullBulkBytes       = []byte{'$', '-', '1', '\r', '\n'}
	okBytes             = []byte{'+', 'O', 'K', '\r', '\n'}
	pongBytes           = []byte{'+', 'P', 'O', 'N', 'G', '\r', '\n'}
	queuedBytes         = []byte{'+', 'Q', 'U', 'E', 'U', 'E', 'D', '\r', '\n'}
	unknownBytes        = []byte{'-', 'E', 'r', 'r', ' ', 'u', 'n', 'k', 'n', 'o', 'w', 'n', '\r', '\n'}
	syntaxBytes         = []byte{'-', 'E', 'r', 'r', ' ', 's', 'y', 'n', 't', 'a', 'x', ' ', 'e', 'r', 'r', 'o', 'r', '\r', '\n'}
)

type (
	emptyReply          struct{}
	emptyArrayReply     struct{}
	nullBulkStringReply struct{}
	okReply             struct{}
	pongReply           struct{}
	queuedReply         struct{}
	statusReply         struct{ status []byte }
	simpleErrorReply    struct{ info []byte }
	unknownErrorReply   struct{}
	syntaxErrorReply    struct{}
	intReply            struct{ code int64 }
	bulkReply           struct{ arg []byte }
	arrayReply          struct{ args [][]byte }
	containingReply     struct{ replies []redis.Reply }
)

func CheckErrorReply(r redis.Reply) bool {
	return '-' == r.GetBytes()[0]
}

func CheckOKReply(r redis.Reply) bool {
	return utils.BytesEqual(okBytes, r.GetBytes())
}

func CheckEmptyArrayReply(r redis.Reply) bool {
	return utils.BytesEqual(emptyMultiBulkBytes, r.GetBytes())
}

func (r *emptyReply) GetBytes() []byte {
	return make([]byte, 0)
}

func (r *emptyArrayReply) GetBytes() []byte {
	return emptyMultiBulkBytes
}

func (r *nullBulkStringReply) GetBytes() []byte {
	return nullBulkBytes
}

func (r *okReply) GetBytes() []byte {
	return okBytes
}

func (r *pongReply) GetBytes() []byte {
	return pongBytes
}

func (r *queuedReply) GetBytes() []byte {
	return queuedBytes
}

func (r *statusReply) GetBytes() []byte {
	return []byte(fmt.Sprintf("+%s\r\n", r.status))
}

func (r *simpleErrorReply) GetBytes() []byte {
	return []byte(fmt.Sprintf("-%s\r\n", r.info))
}

func (r *simpleErrorReply) Error() string {
	return string(r.info)
}

func (r *unknownErrorReply) GetBytes() []byte {
	return unknownBytes
}

func (r *unknownErrorReply) Error() string {
	return "Err unknown"
}

func (r *syntaxErrorReply) GetBytes() []byte {
	return syntaxBytes
}

func (r *syntaxErrorReply) Error() string {
	return "Err syntax error"
}

func (r *intReply) GetBytes() []byte {
	return []byte(fmt.Sprintf(":%d\r\n", r.code))
}

func (r *bulkReply) GetBytes() []byte {
	return convertArg(r.arg)
}

func (r *arrayReply) GetBytes() []byte {
	nArgs := len(r.args)
	var buf bytes.Buffer
	buf.Write([]byte(fmt.Sprintf("*%d\r\n", nArgs)))
	for _, arg := range r.args {
		buf.Write(convertArg(arg))
	}
	return buf.Bytes()
}

func (r *containingReply) GetBytes() []byte {
	nReplies := len(r.replies)
	var buf bytes.Buffer
	buf.Write([]byte(fmt.Sprintf("*%d\r\n", nReplies)))
	for _, reply := range r.replies {
		buf.Write(reply.GetBytes())
	}
	return buf.Bytes()
}

func EmptyReply() redis.Reply {
	return &emptyReply{}
}

func EmptyArrayReply() redis.Reply {
	return &emptyArrayReply{}
}

func NullBulkStringReply() redis.Reply {
	return &nullBulkStringReply{}
}

func OkReply() redis.Reply {
	return &okReply{}
}

func PongReply() redis.Reply {
	return &pongReply{}
}

func QueuedReply() redis.Reply {
	return &queuedReply{}
}

func StatusReply(status []byte) redis.Reply {
	return &statusReply{status: status}
}

func NewErrorReply(info []byte) redis.ErrorReply {
	return &simpleErrorReply{info: info}
}

func UnknownErrorReply() redis.ErrorReply {
	return &unknownErrorReply{}
}

func SyntaxErrorReply() redis.ErrorReply {
	return &syntaxErrorReply{}
}

func IntReply(code int64) redis.Reply {
	return &intReply{code: code}
}

func BulkStringReply(arg []byte) redis.Reply {
	return &bulkReply{arg: arg}
}

func ArrayReply(args [][]byte) redis.Reply {
	return &arrayReply{args: args}
}

func ContainingReply(replies []redis.Reply) redis.Reply {
	return &containingReply{replies: replies}
}

func FetchStatus(r redis.Reply) (status []byte, ok bool) {
	st, ok := r.(*statusReply)
	if !ok {
		return nil, false
	}
	return st.status, true
}

func FetchCode(r redis.Reply) (code int64, ok bool) {
	ir, ok := r.(*intReply)
	if !ok {
		return 0, false
	}
	return ir.code, true
}

func FetchBulkString(r redis.Reply) (str []byte, ok bool) {
	st, ok := r.(*bulkReply)
	if !ok {
		return nil, false
	}
	return st.arg, true
}

func FetchArrayArgs(r redis.Reply) (args [][]byte, ok bool) {
	ar, ok := r.(*arrayReply)
	if !ok {
		return nil, false
	}
	return ar.args, true
}

func FetchReplies(r redis.Reply) (replies []redis.Reply, ok bool) {
	cr, ok := r.(*containingReply)
	if !ok {
		return nil, false
	}
	return cr.replies, true
}

func convertArg(arg []byte) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
}
