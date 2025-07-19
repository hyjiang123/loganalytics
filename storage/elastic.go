package storage

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/olivere/elastic/v7"
)

type ElasticClient struct {
	client *elastic.Client
	index  string
}

type LogDoc struct {
	Timestamp time.Time              `json:"@timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Meta      map[string]interface{} `json:"meta,omitempty"`
}

func NewElasticClient(addr string) (*ElasticClient, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(addr),
		elastic.SetSniff(false),
	)
	if err != nil {
		return nil, err
	}

	// 测试连接
	info, code, err := client.Ping(addr).Do(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to ping ES: %v", err)
	}
	log.Printf("Connected to ES: %s (version: %s, status: %d)", info.ClusterName, info.Version.Number, code)

	index := "logs-" + time.Now().Format("2006-01-02")

	// 检查索引是否存在，如果不存在则创建
	exists, err := client.IndexExists(index).Do(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to check index existence: %v", err)
	}

	if !exists {
		log.Printf("Creating index: %s", index)
		// 创建索引映射
		mapping := `{
			"mappings": {
				"properties": {
					"@timestamp": {"type": "date"},
					"level": {"type": "keyword"},
					"message": {"type": "text"},
					"meta": {"type": "object"}
				}
			}
		}`

		_, err = client.CreateIndex(index).Body(mapping).Do(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to create index: %v", err)
		}
		log.Printf("Index %s created successfully", index)
	} else {
		log.Printf("Index %s already exists", index)
	}

	return &ElasticClient{
		client: client,
		index:  index,
	}, nil
}

func (e *ElasticClient) BulkInsert(docs []LogDoc) error {
	bulk := e.client.Bulk()
	for _, doc := range docs {
		req := elastic.NewBulkIndexRequest().
			Index(e.index).
			Doc(doc).
			OpType("create")
		bulk.Add(req)
	}

	result, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}

	// 检查批量操作结果
	if result.Errors {
		for _, item := range result.Items {
			for _, action := range item {
				if action.Error != nil {
					log.Printf("Bulk insert error: %v", action.Error)
				}
			}
		}
	}

	return nil
}

func (e *ElasticClient) GetIndexName() string {
	return e.index
}
