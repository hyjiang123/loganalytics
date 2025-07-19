package monitor

import (
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	logCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logs_processed_total",
			Help: "Total number of processed logs",
		},
		[]string{"level"},
	)

	queueGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "pipeline_queue_length",
			Help: "Current input queue length",
		},
	)

	// 原子计数器示例
	errorCount uint64
)

func InitPrometheus(port string) {
	prometheus.MustRegister(logCounter)
	prometheus.MustRegister(queueGauge)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		http.ListenAndServe(":"+port, nil)
	}()
}

func RecordError() {
	atomic.AddUint64(&errorCount, 1)
}
