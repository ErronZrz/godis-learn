package hub

import (
	"fmt"
	"godis-learn/datastruct/dict"
	"godis-learn/datastruct/list"
	"godis-learn/datastruct/lock"
	"godis-learn/interface/redis"
	"godis-learn/lib/utils"
	"godis-learn/redis/protocol"
)

type Hub struct {
	subsMap dict.HashMap
	locker  *lock.StringLock
}

func NewHub() *Hub {
	return &Hub{
		subsMap: dict.NewConcurrentHashMap(16),
		locker:  lock.NewStringLock(16),
	}
}

func (h *Hub) Publish(args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.ArgumentCountErrorReply([]byte("publish"))
	}
	channel := string(args[0])
	h.locker.Lock(channel)
	defer h.locker.Unlock(channel)
	subs, ok := h.subsMap.Get(channel)
	if !ok {
		return protocol.IntReply(0)
	}
	subs.(*list.LinkedList).ForEach(func(_ int, conn any) bool {
		message := protocol.ArrayReply([][]byte{
			[]byte("message"),
			args[0],
			args[1],
		})
		return conn.(redis.Connection).Write(message.GetBytes()) == nil
	})
	return protocol.IntReply(int64(h.subsMap.Size()))
}

func (h *Hub) Subscribe(conn redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, s := range args {
		channels[i] = string(s)
	}
	h.locker.LockKeys(channels)
	defer h.locker.UnlockKeys(channels)
	for _, channel := range channels {
		if h.subscribe(channel, conn) {
			_ = conn.Write(generateBytes("subscribe", channel, conn.SubscriberCount()))
		}
	}
	return protocol.EmptyReply()
}

func (h *Hub) Unsubscribe(conn redis.Connection, args [][]byte) redis.Reply {
	var channels []string
	if len(args) > 0 {
		channels = make([]string, len(args))
		for i, arg := range args {
			channels[i] = string(arg)
		}
	} else {
		channels = conn.GetChannels()
	}
	h.locker.LockKeys(channels)
	defer h.locker.UnlockKeys(channels)
	if len(channels) == 0 {
		_ = conn.Write([]byte("*3\r\n$11\r\nunsubscribe\r\n$-1\n:0\r\n"))
	} else {
		for _, channel := range channels {
			if h.unsubscribe(channel, conn) {
				_ = conn.Write(generateBytes("unsubscribe", channel, conn.SubscriberCount()))
			}
		}
	}
	return protocol.EmptyReply()
}

func (h *Hub) UnsubscribeAll(conn redis.Connection) {
	channels := conn.GetChannels()
	h.locker.LockKeys(channels)
	defer h.locker.UnlockKeys(channels)
	for _, channel := range channels {
		h.unsubscribe(channel, conn)
	}
}

func (h *Hub) subscribe(channel string, conn redis.Connection) bool {
	conn.Subscribe(channel)
	l := list.NewLinkedList(nil)
	subs, ok := h.subsMap.Get(channel)
	if ok {
		l = subs.(*list.LinkedList)
	} else {
		h.subsMap.Put(channel, l)
	}
	if l.Count(func(c any) bool { return c == conn }) > 0 {
		return false
	}
	l.Add(conn)
	return true
}

func (h *Hub) unsubscribe(channel string, conn redis.Connection) bool {
	conn.Unsubscribe(channel)
	subs, ok := h.subsMap.Get(channel)
	if !ok {
		return false
	}
	l := subs.(*list.LinkedList)
	l.RemoveAll(func(c any) bool { return utils.PotentialBytesEqual(c, conn) })
	if l.Size() == 0 {
		h.subsMap.Delete(channel)
	}
	return true
}

func generateBytes(name, channel string, code int) []byte {
	return []byte(fmt.Sprintf("*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n:%d\r\n",
		len(name), name, len(channel), channel, code))
}
