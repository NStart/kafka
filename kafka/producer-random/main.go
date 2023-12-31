package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	//get kafka writer using enviroment variables.
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")

	//set env in linux terminal
	//export kafkaURL="172.30.254.207:9092"
	//export topic="quickstart-events"

	// kafkaURL := "172.30.254.207:9092"
	// topic := "quickstart-events"
	writer := newKafkaWriter(kafkaURL, topic)
	defer writer.Close()

	fmt.Println("star producer ... !!")

	for i := 0; ; i++ {
		key := fmt.Sprintf("key-%d", i)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(fmt.Sprint(uuid.New())),
		}
		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("produced", key)
		}

		time.Sleep(1 * time.Second)
	}
}
