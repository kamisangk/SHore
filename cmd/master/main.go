package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"shore-master/monitor/config"
	httpserver "shore-master/monitor/http"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("加载 shore.toml 失败: %v", err)
	}
	server, err := httpserver.NewServer(cfg)
	if err != nil {
		log.Fatalf("初始化 SHore 失败: %v", err)
	}
	defer func() {
		_ = server.Close()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server.StartBackground(ctx)
	if err := server.Run(); err != nil {
		log.Fatalf("启动 SHore 失败: %v", err)
	}
}
