package main

import (
	"fmt"
	"time"

	provider "kafka/cmd/model"
)

const (
	Topic = "meu-topico"
)

type KafkaMessage struct {
	Now   time.Time
	Name  string
	Value int
}

func main() {
	kafkaProvider := provider.NewProvider(Topic, 0)

	kafkaProvider.Connect()

	kafkaProvider.SetWriteDeadline(time.Now().Add(10 * time.Second))

	msg := KafkaMessage{
		Now:   time.Now(),
		Name:  "Lucao",
		Value: 12,
	}

	kafkaProvider.Write(msg)

	kafkaProvider.CloseConnection()
}

func (msg KafkaMessage) String() string {
	return fmt.Sprintf("Name: %v, Date: %v, Value: %v", msg.Name, msg.Now, msg.Value)
}
