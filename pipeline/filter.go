package pipeline

import (
	"context"
	"loganalytics/monitor"
	"loganalytics/storage"
	"strings"
	"sync"
)

type FilteredLog struct {
	ParsedLog
	Valid    bool
	Filtered string // 记录被哪个过滤器处理
}

func (f FilteredLog) ToDoc() storage.LogDoc {
	return storage.LogDoc{
		Timestamp: f.Timestamp,
		Level:     f.Level,
		Message:   f.Message,
		Meta:      map[string]interface{}{"filtered": f.Filtered}, // 添加过滤元信息
	}
}

// 过滤器接口
type LogFilter interface {
	Filter(ParsedLog) (FilteredLog, bool)
	Name() string
}

// 实现过滤器
type LevelFilter struct {
	allowedLevels map[string]bool
}

func NewLevelFilter(levels []string) *LevelFilter {
	m := make(map[string]bool)
	for _, lvl := range levels {
		m[lvl] = true
	}
	return &LevelFilter{allowedLevels: m}
}

func (f *LevelFilter) Filter(plog ParsedLog) (FilteredLog, bool) {
	if !f.allowedLevels[plog.Level] {
		return FilteredLog{
			ParsedLog: plog,
			Valid:     false,
			Filtered:  f.Name(),
		}, false
	}
	return FilteredLog{ParsedLog: plog, Valid: true}, true
}

func (f *LevelFilter) Name() string { return "level_filter" }

// 关键词黑名单过滤器
type KeywordFilter struct {
	blacklist []string
}

func NewKeywordFilter(keywords []string) *KeywordFilter {
	return &KeywordFilter{blacklist: keywords}
}

func (f *KeywordFilter) Filter(plog ParsedLog) (FilteredLog, bool) {
	for _, kw := range f.blacklist {
		if strings.Contains(plog.Message, kw) {
			return FilteredLog{
				ParsedLog: plog,
				Valid:     false,
				Filtered:  f.Name(),
			}, false
		}
	}
	return FilteredLog{ParsedLog: plog, Valid: true}, true
}

func (f *KeywordFilter) Name() string { return "keyword_filter" }

// 过滤worker
func filterWorker(
	ctx context.Context,
	in <-chan ParsedLog,
	out chan<- FilteredLog,
	filters []LogFilter,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for {
		select {
		case plog, ok := <-in:
			if !ok {
				return
			}

			// 顺序执行过滤链
			var flog FilteredLog
			valid := true
			for _, filter := range filters {
				if flog, valid = filter.Filter(plog); !valid {
					monitor.RecordError() // 记录过滤事件
					break
				}
			}

			if valid {
				flog = FilteredLog{
					ParsedLog: plog,
					Valid:     true,
				}
			}

			out <- flog

		case <-ctx.Done():
			return
		}
	}
}

// 启动过滤阶段
func StartFilters(
	ctx context.Context,
	in <-chan ParsedLog,
	filterChain []LogFilter,
	workerCount int,
) <-chan FilteredLog {
	out := make(chan FilteredLog, 1000)
	var wg sync.WaitGroup

	// 初始化过滤器
	if filterChain == nil {
		filterChain = []LogFilter{
			NewLevelFilter([]string{"error", "warn"}),
			NewKeywordFilter([]string{"password", "token"}),
		}
	}

	// 启动worker池
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go filterWorker(ctx, in, out, filterChain, &wg)
	}

	// 等待所有worker退出后关闭通道
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
