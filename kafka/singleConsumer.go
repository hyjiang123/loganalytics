package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"loganalytics/config"

	"log"
)

func SingleConsumerGetData(out chan<- string) {
	// Kafka 配置
	configKafka := sarama.NewConfig()
	configKafka.Consumer.Return.Errors = true

	cfg := config.Load()

	// 创建消费者客户端
	client, err := sarama.NewClient([]string{cfg.KafkaAddress}, configKafka)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	// 创建消费者组
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// 订阅主题
	partitionConsumer, err := consumer.ConsumePartition(cfg.KafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start consumer for partition 0: %v", err)
	}
	defer partitionConsumer.Close()

	// 消费消息
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// 打印消息内容
			out <- string(msg.Value)
			fmt.Printf("get kafka message: %s\n", string(msg.Value))
		case err := <-partitionConsumer.Errors():
			log.Printf("Error consuming message: %v", err)
		}
	}
}
