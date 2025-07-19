package main

import (
	"context"
	"log"
	"loganalytics/config"
	"loganalytics/kafka"
	"loganalytics/monitor"
	"loganalytics/pipeline"
	"loganalytics/storage"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log.Println("Starting log analytics application...")

	// 初始化上下文和终止通道
	ctx, cancel := context.WithCancel(context.Background())
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	// 加载配置
	cfg := config.Load()
	log.Printf("Configuration loaded: ES=%s, PrometheusPort=%s", cfg.ESAddress, cfg.PrometheusPort)

	// 初始化存储
	esClient, err := storage.NewElasticClient(cfg.ESAddress)
	if err != nil {
		log.Fatalf("Failed to connect to ES: %v", err)
	}
	log.Printf("Successfully connected to Elasticsearch, using index: %s", esClient.GetIndexName())

	// 构建处理流水线
	logChan := make(chan string, 10000) // 原始日志通道

	// 初始化监控
	monitor.InitPrometheus(cfg.PrometheusPort)
	log.Printf("Prometheus monitoring started on port %s", cfg.PrometheusPort)

	done := pipeline.BuildPipeline(ctx, logChan, esClient)
	log.Println("Pipeline built successfully")

	// 模拟日志生产者
	//go simulateLogProducer(logChan)
	// 读取 kafka 数据
	go kafka.SingleConsumerGetData(logChan)
	log.Println("Log producer started")

	log.Println("Application is running. Press Ctrl+C to stop...")

	// 等待终止信号
	<-shutdown
	log.Println("Shutdown signal received...")

	// 优雅关闭
	cancel()
	select {
	case <-done:
		log.Println("Pipeline stopped cleanly")
	case <-time.After(30 * time.Second):
		log.Println("Forced shutdown after timeout")
	}
}

// 模拟数据，供测试使用
func simulateLogProducer(out chan<- string) {
	log.Println("Starting log simulation...")
	out <- `{"time":"2023-01-01T12:00:00Z","level":"error","message":"connection timeout"}`
}
