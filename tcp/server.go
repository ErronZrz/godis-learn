package tcp

import (
	"context"
	"godis-learn/interface/tcp"
	"godis-learn/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config stores tcp server properties
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ListenAndServeWithSignal 监听中断信号，并且通过 closeChan 通知服务器关闭
func ListenAndServeWithSignal(cfg *Config, hr tcp.Handler) error {
	closeChan := make(chan struct{})
	signalChan := make(chan os.Signal)
	// 只允许这 4 类信号传送到 signalChan
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-signalChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	lr, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Infof("Bound %s success, start listening...")
	ListenAndServe(lr, hr, closeChan)
	return nil
}

// ListenAndServe 提供服务
func ListenAndServe(lr net.Listener, hr tcp.Handler, closeChan <-chan struct{}) {
	// 监听 closeChan 的关闭通知
	go func() {
		<-closeChan
		logger.Info("Shutting down...")
		_ = lr.Close()
		_ = hr.Close()
	}()
	defer func() {
		_ = lr.Close()
		_ = hr.Close()
	}()
	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := lr.Accept()
		if err != nil {
			break
		}
		logger.Info("Connection accepted.")
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
			}()
			hr.Handle(ctx, conn)
		}()
	}
	waitDone.Wait()
}
