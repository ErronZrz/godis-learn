package cluster

import (
	"context"
	"godis-learn/interface/redis"
	"godis-learn/lib/utils"
	"godis-learn/redis/client"
	"godis-learn/redis/protocol"
	"strconv"
)

var defaultRelayFunc = func(cluster *Cluster, node string, conn redis.Connection, line redis.Line) redis.Reply {
	if node == cluster.self {
		return cluster.db.Execute(conn, line)
	}
	factory, ok := cluster.connPoolMap[node]
	if !ok {
		return protocol.NewErrorReply([]byte("connection factory not found"))
	}
	borrowedObj, err := factory.BorrowObject(context.Background())
	if err != nil {
		return protocol.NewErrorReply([]byte("connection factory borrow failed"))
	}
	borrowedConn := borrowedObj.(*client.Client)
	defer func(cluster *Cluster) {
		factory, ok := cluster.connPoolMap[node]
		if ok {
			_ = factory.ReturnObject(context.Background(), borrowedConn)
		}
	}(cluster)
	_ = borrowedConn.Send(utils.StringsToLine("SELECT", strconv.Itoa(conn.GetDBIndex())))
	return borrowedConn.Send(line)
}
