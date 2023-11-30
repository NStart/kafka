package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	startTime := time.Now().Add(-time.Hour * 3)
	endTime := time.Now()
	batchSize := int(10e6) //10MB
	//make a new reader than consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "quickstart-events",
		Partition: 0,
		MaxBytes:  batchSize,
	})
	r.SetOffsetAt(context.Background(), startTime)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		if m.Time.After(endTime) {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
