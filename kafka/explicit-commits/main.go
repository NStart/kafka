package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	//make a new reader that consumes from topic-A
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "console-consumer-7872",
		Topic:    "quickstart-events",
		MaxBytes: 10e6, //10MB

		/*
			Managing Commits
			By default, CommitMessages will synchronously commit offsets to Kafka.
			For improved performance, you can instead periodically commit offsets
			 to Kafka by setting CommitInterval on the ReaderConfig.
		*/
		//CommitInterval: time.Second, // flushes commits to Kafka every second
	})

	ctx := context.Background()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v %v %v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		//这段的作用是，提交本次的offset,重启服务就会从下一个offset开始读取，如果没有就还是会从头读取
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Fatal("failed to commit messages:", err)
		}

	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
