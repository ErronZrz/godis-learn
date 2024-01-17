package main

import (
	"bufio"
	"io"
	"log"
	"net"
)

func ListenAndServe(address string) {
	// 绑定监听地址
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Listen error: %v", err)
	}
	defer listener.Close()
	log.Printf("Bind: %s success, start listening.\n", address)
	for {
		// 此处会阻塞直至有新的连接建立
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Accept error: %v", err)
		}
		go Handle(conn)
	}
}

func Handle(c net.Conn) {
	reader := bufio.NewReader(c)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("Connection closed.")
			} else {
				log.Println(err)
			}
			return
		}
		_, _ = c.Write([]byte(msg))
	}
}

func main() {
	ListenAndServe(":8080")
}
