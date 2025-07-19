package pipeline

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"
)

type ParsedLog struct {
	Timestamp time.Time
	Level     string
	Message   string
}

func parseWorker(ctx context.Context, in <-chan string, out chan<- ParsedLog, wg *sync.WaitGroup) {
	defer wg.Done()
	count := 0
	for {
		select {
		case raw, ok := <-in:
			if !ok {
				log.Printf("Parser worker processed %d logs", count)
				return
			}
			var plog ParsedLog
			if err := json.Unmarshal([]byte(raw), &plog); err != nil {
				log.Printf("Parse failed: %v", err)
				continue
			}
			count++
			if count%100 == 0 {
				log.Printf("Parser worker processed %d logs", count)
			}
			out <- plog
		case <-ctx.Done():
			log.Printf("Parser worker stopped, processed %d logs", count)
			return
		}
	}
}

func StartParsers(ctx context.Context, in <-chan string, parserCount int) <-chan ParsedLog {
	out := make(chan ParsedLog, 1000)
	var wg sync.WaitGroup

	for i := 0; i < parserCount; i++ {
		wg.Add(1)
		go parseWorker(ctx, in, out, &wg)
	}

	// 等待所有worker退出后关闭通道
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
