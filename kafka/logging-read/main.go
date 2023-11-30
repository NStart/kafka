package main

import (
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func logf(msg string, a ...interface{}) {
	fmt.Printf(msg, a...)
	fmt.Println()
}

func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "quickstart-events",
		Partition:   0,
		MaxBytes:    10e6,
		Logger:      kafka.LoggerFunc(logf),
		ErrorLogger: kafka.LoggerFunc(logf),
	})

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
