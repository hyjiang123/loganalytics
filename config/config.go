package config

import "time"

type Config struct {
	ESAddress      string
	PrometheusPort string
	ParserCount    int
	BatchSize      int
	FlushInterval  time.Duration
	KafkaAddress   string
	KafkaTopic     string
}

func Load() *Config {
	return &Config{
		ESAddress:      "http://192.168.2.2:9200",
		PrometheusPort: "2112",
		ParserCount:    10,
		BatchSize:      1000,
		FlushInterval:  5 * time.Second,
		KafkaAddress:   "192.168.2.2:9092",
		KafkaTopic:     "loganalytics",
	}
}
