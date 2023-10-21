package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	Topic = "meu-topico"
)

type KafkaProvider struct {
	connection Connection
	topic      string
	partition  int
}

type Connection interface{}

func main() {
	kafkaProvider := KafkaProvider{
		topic:     Topic,
		partition: 0,
	}

	err := kafkaProvider.Connect()

	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn := kafkaProvider.GetConnection()

	kafkaProvider.SetWriteDeadline(time.Now().Add(10 * time.Second))

	err = kafkaProvider.Write("OPAAA!")

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

func (kafkaProvider *KafkaProvider) GetConnection() *kafka.Conn {
	if v, ok := kafkaProvider.connection.(*kafka.Conn); ok {
		return v
	}

	panic("invalid implementation")
}

func (kafkaProvider *KafkaProvider) SetWriteDeadline(time time.Time) {
	kafkaProvider.GetConnection().SetWriteDeadline(time)
}

func (kafkaProvider *KafkaProvider) Write(message string) error {
	_, err := kafkaProvider.GetConnection().WriteMessages(
		kafka.Message{Value: []byte(message)},
	)

	return err
}
