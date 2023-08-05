package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	log.Println("starting read write to kafka topic")
	// create a new writer that produces to topic-A, using the least bytes compression
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   "topic-A",
	})

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello Kafka!"),
		},
	)
	if err != nil {
		log.Printf("error in writing to topic %s\n", err)
	}
	w.Close()

	// create a new reader that consumes from topic-A
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"kafka:9092"},
		Topic:     "topic-A",
		Partition: 0,
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("error i reading message from topic-a %s\n", err)
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
	log.Println("read write completed")
}
