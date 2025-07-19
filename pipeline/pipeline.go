package pipeline

import (
	"context"
	"loganalytics/storage"
	"runtime"
	"time"
)

func BuildPipeline(
	ctx context.Context,
	logChan <-chan string,
	es *storage.ElasticClient,
) <-chan struct{} {
	// Stage 1: 日志解析
	parsedCh := StartParsers(ctx, logChan, 10)

	// Stage 2: 过滤链处理
	filteredCh := StartFilters(ctx, parsedCh, nil, runtime.NumCPU()*2)

	// Stage 3: 批量写入
	return StartBatcher(ctx, filteredCh, es, 1000, 5*time.Second)
}
