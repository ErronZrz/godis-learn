package cluster

import (
	"context"
	"fmt"
	"github.com/jolestar/go-commons-pool/v2"
	"godis-instruction/config"
	"godis-instruction/database"
	"godis-instruction/datastruct/dict"
	"godis-instruction/interface/dbinterface"
	"godis-instruction/interface/redis"
	"godis-instruction/lib/consistenthash"
	"godis-instruction/lib/logger"
	"godis-instruction/lib/snow"
	"godis-instruction/redis/protocol"
	"runtime/debug"
)

type PeerPicker interface {
	AddNodes(keys ...string)
	PickNode(key string) string
}

type CommandFunc func(cluster *Cluster, conn redis.Connection, line redis.Line) redis.Reply

type Cluster struct {
	self        string
	nodes       []string
	picker      PeerPicker
	connPoolMap map[string]*pool.ObjectPool
	db          dbinterface.EmbedDB
	txMap       dict.HashMap
	idGenerator *snow.IDGenerator
	relayFunc   func(cluster *Cluster, node string, conn redis.Connection, line redis.Line) redis.Reply
}

const (
	replicaCount = 4
	allowFastTx  = true
)

var (
	router = newRouter()
)

func NewCluster() *Cluster {
	self := config.Properties.Self
	cluster := &Cluster{
		self:        self,
		picker:      consistenthash.NewPicker(replicaCount, nil),
		connPoolMap: make(map[string]*pool.ObjectPool),
		db:          database.NewStandaloneServer(),
		txMap:       dict.NewSimpleHashMap(),
		idGenerator: snow.NewIDGenerator(self),
		relayFunc:   defaultRelayFunc,
	}
	contains := make(map[string]struct{})
	peers := config.Properties.Peers
	nodes := make([]string, 0, len(peers)+1)
	for _, peer := range peers {
		if _, ok := contains[peer]; !ok {
			contains[peer] = struct{}{}
			nodes = append(nodes, peer)
		}
	}
	nodes = append(nodes, self)
	cluster.picker.AddNodes(nodes...)
	ctx := context.Background()
	for _, peer := range peers {
		cluster.connPoolMap[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			peer: peer,
		})
	}
	cluster.nodes = nodes
	return cluster
}
func (c *Cluster) Execute(conn redis.Connection, line redis.Line) redis.Reply {
	var res redis.Reply
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, debug.Stack()))
			res = protocol.UnknownErrorReply()
		}
	}()
	cmdName := string(line.CommandName())
	argumentCountErrorReply := protocol.ArgumentCountErrorReply(line.CommandName())
	if cmdName == "auth" {
		return database.Auth(conn, line.CommandContent())
	} else if config.Properties.RequirePass != "" && conn.GetPassword() != config.Properties.RequirePass {
		return protocol.NewErrorReply([]byte("NOAUTH Authentication required"))
	} else if cmdName == "multi" {
		if len(line) != 1 {
			return argumentCountErrorReply
		}
		return database.StartMulti(conn)
	} else if cmdName == "discard" {
		if len(line) != 1 {
			return argumentCountErrorReply
		}
		return database.DiscardMulti(conn)
	} else if cmdName == "exec" {
		if len(line) != 1 {
			return argumentCountErrorReply
		}
		return execMulti(c, conn)
	} else if cmdName == "select" {
		if len(line) != 2 {
			return argumentCountErrorReply
		}
		return execSelect(conn, line)
	}
	if conn != nil && conn.CheckMultiMode() {
		return database.ConnectionEnqueue(conn, line)
	}
	commandFunc, ok := router[cmdName]
	if !ok {
		return protocol.NewErrorReply([]byte(fmt.Sprintf("ERR unknown command '%s', or not supported in cluster mode", cmdName)))
	}
	res = commandFunc(c, conn, line)
	return res
}

func (c *Cluster) AfterClientClose(conn redis.Connection) {
	c.db.AfterClientClose(conn)
}

func (c *Cluster) Close() {
	c.db.Close()
}

// TODO: 上一次写完了 cluster.go，还没决定下一步写啥
