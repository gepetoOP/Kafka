package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProvider struct {
	connection Connection
	topic      string
	partition  int
}

type Connection interface{}

func main() {
	// to produce messages
	topic := "meu-topico"
	partition := 0

	kafkaProvider := KafkaProvider{
		topic:     "meu-topico",
		partition: 0,
	}

	err := kafkaProvider.Connect()

	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9093", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func (kafkaProvider *KafkaProvider) Connect() error {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9093", kafkaProvider.topic, kafkaProvider.partition)

	if err != nil {
		return err
	}

	kafkaProvider.connection = conn

	return nil
}
