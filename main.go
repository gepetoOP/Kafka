package main

import (
	"fmt"
	"math/rand"
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

	for i := 0; i < 1; i++ {
		run(*kafkaProvider)
	}
	kafkaProvider.SetReadDeadline(time.Now().Add(20 * time.Second))

	kafkaProvider.Read()

	kafkaProvider.CloseConnection()
}

func run(provider provider.KafkaProvider) {
	for i := 0; i < 2; i++ {
		msg := KafkaMessage{
			Now:   time.Now(),
			Name:  "Lucao",
			Value: rand.Intn(1000),
		}

		provider.Write(msg)
	}
}

func (msg KafkaMessage) String() string {
	return fmt.Sprintf("Name: %v, Date: %v, Value: %v", msg.Name, msg.Now, msg.Value)
}
