package parser

import (
	"bytes"
	"godis-instruction/interface/redis"
	"godis-instruction/lib/utils"
	"godis-instruction/redis/protocol"
	"io"
	"testing"
)

func TestParseStream(t *testing.T) {
	replies := []redis.Reply{
		protocol.IntReply(1),
		protocol.StatusReply([]byte("OK")),
		protocol.NewErrorReply([]byte("ERR unknown")),
		protocol.BulkStringReply([]byte("a\r\nb")), // test binary safe
		protocol.BulkStringReply([]byte{}),
		protocol.NullBulkStringReply(),
		protocol.ArrayReply([][]byte{
			[]byte("ccc"),
			[]byte("d\r\ne"),
		}),
		protocol.ArrayReply([][]byte{
			[]byte("fff"),
			{},
			[]byte("ggg"),
		}),
		protocol.EmptyArrayReply(),
	}
	reqs := bytes.Buffer{}
	for _, re := range replies {
		reqs.Write(re.GetBytes())
	}
	reqs.Write([]byte("set a a\r\n")) // test text protocol
	expected := make([]redis.Reply, len(replies))
	copy(expected, replies)
	expected = append(expected, protocol.ArrayReply([][]byte{
		[]byte("set"), {'a'}, {'a'},
	}))

	// logger.Info(string(reqs.Bytes()))
	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expected[i]
		i++
		if !utils.BytesEqual(exp.GetBytes(), payload.Data.GetBytes()) {
			t.Error("parse failed: " + string(exp.GetBytes()) + "---" + string(payload.Data.GetBytes()))
		}
	}
}
