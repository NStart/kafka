package main

import (
	"context"
	"errors"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	// Make a writer that publishes messages to topic-A.
	// The topic will be created if it is missing.
	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "topic-missing",
		AllowAutoTopicCreation: true,
	}

	message := []kafka.Message{
		{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World"),
		},
		{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		{
			Key:   []byte("Key-C"),
			Value: []byte("Two"),
		},
	}

	var err error
	const retries = 3
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		//accempt to create topic prior to publishing the message
		err = w.WriteMessages(ctx, message...)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			log.Fatalf("unexpected error %v", err)
		}
		break
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
