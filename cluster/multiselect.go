package cluster

import (
	"bytes"
	"encoding/base64"
	"godis-instruction/config"
	"godis-instruction/database"
	"godis-instruction/interface/redis"
	"godis-instruction/lib/utils"
	"godis-instruction/redis/parse"
	"godis-instruction/redis/protocol"
	"strconv"
)

const (
	relayStr = "_multi"
	innerStr = "_watch"
)

func execMulti(cluster *Cluster, conn redis.Connection) redis.Reply {
	if !conn.CheckMultiMode() {
		return protocol.NewErrorReply([]byte("ERR EXEC without MULTI"))
	}
	defer conn.SetMultiMode(false)

	lines := conn.GetQueuedCmdLines()
	keys := make([]string, 0)
	for _, cmd := range lines {
		rKeys, wKeys := database.RelatedKeys(cmd)
		keys = append(keys, wKeys...)
		keys = append(keys, rKeys...)
	}
	watching := conn.GetWatching()
	for key := range watching {
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return cluster.db.ExecMulti(conn, watching, lines)
	}
	groupMap := groupBy(cluster, keys)
	if len(groupMap) > 1 {
		return protocol.NewErrorReply([]byte("ERR MULTI commands transaction must within one slot in cluster mode"))
	}
	peer := ""
	for p := range groupMap {
		peer = p
	}
	if peer == cluster.self {
		return cluster.db.ExecMulti(conn, watching, lines)
	}
	return execMultiOnAnotherNode(cluster, conn, peer, watching, lines)
}

func execSelect(conn redis.Connection, line redis.Line) redis.Reply {
	dbIndex, err := strconv.Atoi(string(line[1]))
	if err != nil {
		return protocol.NewErrorReply([]byte("ERR invalid DB index"))
	}
	if dbIndex < 0 || dbIndex >= config.Properties.DatabaseCount {
		return protocol.NewErrorReply([]byte("ERR DB index out of bounds"))
	}
	conn.SelectDB(dbIndex)
	return protocol.OkReply()
}

func execMultiOnAnotherNode(cluster *Cluster, conn redis.Connection, peer string, watching map[string]uint32, lines []redis.Line) redis.Reply {
	defer func(cn redis.Connection) {
		cn.ClearQueue()
		cn.SetMultiMode(false)
	}(conn)
	relayLine := utils.StringsToLine(relayStr)
	watchingLine := utils.StringsToLine(innerStr)
	for key, version := range watching {
		versionStr := strconv.FormatUint(uint64(version), 10)
		watchingLine = append(watchingLine, []byte(key), []byte(versionStr))
	}
	relayLine = append(relayLine, encodeCmdLines([]redis.Line{watchingLine})...)
	relayLine = append(relayLine, encodeCmdLines(lines)...)
	var rawRelayReply redis.Reply
	if peer == cluster.self {
		rawRelayReply = execRelayedMulti(cluster, conn, relayLine)
	} else {
		rawRelayReply = cluster.relayFunc(cluster, peer, conn, relayLine)
	}
	if protocol.CheckErrorReply(rawRelayReply) || protocol.CheckEmptyArrayReply(rawRelayReply) {
		return rawRelayReply
	}
	args, ok := protocol.FetchArrayArgs(rawRelayReply)
	if !ok {
		return protocol.NewErrorReply([]byte("execute failed"))
	}
	decoded, err := decodeToContaining(args)
	if err != nil {
		return protocol.NewErrorReply([]byte(err.Error()))
	}
	return decoded
}

func execRelayedMulti(cluster *Cluster, conn redis.Connection, line redis.Line) redis.Reply {
	if len(line) < 2 {
		return protocol.ArgumentCountErrorReply([]byte("_exec"))
	}
	decoded, err := decodeToContaining(line.CommandContent())
	if err != nil {
		return protocol.NewErrorReply([]byte(err.Error()))
	}
	var txLines []redis.Line
	replies, ok := protocol.FetchReplies(decoded)
	if !ok {
		return protocol.NewErrorReply([]byte("Unexpected error"))
	}
	for _, r := range replies {
		args, ok := protocol.FetchArrayArgs(r)
		if !ok {
			return protocol.NewErrorReply([]byte("exec failed"))
		}
		txLines = append(txLines, args)
	}
	watching := make(map[string]uint32)
	watchCmdLine := txLines[0]
	for i := 2; i < len(watchCmdLine); i += 2 {
		key, versionStr := string(watchCmdLine[i-1]), string(watchCmdLine[i])
		version, err := strconv.ParseUint(versionStr, 10, 64)
		if err != nil {
			return protocol.NewErrorReply([]byte("watching command line failed"))
		}
		watching[key] = uint32(version)
	}
	rawReply := cluster.db.ExecMulti(conn, watching, txLines[1:])
	return encodeContaining(rawReply)
}

func execWatch(cluster *Cluster, conn redis.Connection, line redis.Line) redis.Reply {
	if len(line) < 2 {
		return protocol.ArgumentCountErrorReply([]byte("watch"))
	}
	content := line.CommandContent()
	watching := conn.GetWatching()
	for _, key := range content {
		keyStr := string(key)
		peer := cluster.picker.PickNode(keyStr)
		reply := cluster.relayFunc(cluster, peer, conn, utils.StringsToLine("GetVer", keyStr))
		if protocol.CheckErrorReply(reply) {
			return reply
		}
		code64, ok := protocol.FetchCode(reply)
		if !ok {
			return protocol.NewErrorReply([]byte("get version failed"))
		}
		watching[keyStr] = uint32(code64)
	}
	return protocol.OkReply()
}

func groupBy(cluster *Cluster, keys []string) map[string][]string {
	res := make(map[string][]string)
	for _, key := range keys {
		peer := cluster.picker.PickNode(key)
		group, ok := res[peer]
		if !ok {
			group = make([]string, 0)
		}
		group = append(group, key)
		res[peer] = group
	}
	return res
}

func encodeCmdLines(lines []redis.Line) redis.Line {
	var res redis.Line
	for _, line := range lines {
		data := protocol.ArrayReply(line).GetBytes()
		encoded := make([]byte, base64.StdEncoding.EncodedLen(len(data)))
		base64.StdEncoding.Encode(encoded, data)
		res = append(res, encoded)
	}
	return res
}

func encodeContaining(reply redis.Reply) redis.Reply {
	replies, ok := protocol.FetchReplies(reply)
	if !ok {
		return protocol.NewErrorReply([]byte("encode failed"))
	}
	args := make([][]byte, 0, len(replies))
	for _, r := range replies {
		data := r.GetBytes()
		encoded := make([]byte, base64.StdEncoding.EncodedLen(len(data)))
		base64.StdEncoding.Encode(encoded, data)
		args = append(args, encoded)
	}
	return protocol.ArrayReply(args)
}

func decodeToContaining(line redis.Line) (redis.Reply, error) {
	cmdBuf := new(bytes.Buffer)
	for _, arg := range line {
		dBuf := make([]byte, base64.StdEncoding.DecodedLen(len(arg)))
		n, err := base64.StdEncoding.Decode(dBuf, arg)
		if err != nil {
			continue
		}
		cmdBuf.Write(dBuf[:n])
	}
	replies, err := parse.ParseBytes(cmdBuf.Bytes())
	if err != nil {
		return nil, protocol.NewErrorReply([]byte(err.Error()))
	}
	return protocol.ContainingReply(replies), nil
}
