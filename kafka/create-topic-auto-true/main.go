package main

import (
	"context"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

// to create topics when auto.create.topics.enable='true'
func main() {
	topic := "auto-topic"
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("fail to create topics:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}
}
