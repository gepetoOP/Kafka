package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	provider "kafka/cmd/model"
)

const (
	Topic = "meu-topico"
)

type KafkaMessage struct {
	Now   time.Time `json:"now"`
	Name  string    `json:"name"`
	Value int       `json:"value"`
}

func main() {
	kafkaProvider := provider.NewProvider(Topic, 0)

	kafkaProvider.Connect()

	kafkaProvider.SetWriteDeadline(time.Now().Add(10 * time.Second))

	for i := 0; i < 5; i++ {
		run(*kafkaProvider)
	}

	kafkaProvider.SetReadDeadline(time.Now().Add(10 * time.Second))

	rawMessages := kafkaProvider.Read()

	kafkaMessages := convertBytes(rawMessages)

	for _, message := range kafkaMessages {
		fmt.Println(message)
	}

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

func convertBytes(messages [][]byte) []KafkaMessage {
	var convertedBytes []KafkaMessage

	for _, message := range messages {
		output := KafkaMessage{}

		err := json.Unmarshal(message, &output)

		if err != nil {
			log.Fatal("Erro ao deserializar mensagem:", err)
		} else {
			convertedBytes = append(convertedBytes, output)
		}
	}

	return convertedBytes
}
