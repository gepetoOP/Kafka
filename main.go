package main

import (
	"time"

	provider "kafka/cmd/model"
)

const (
	Topic = "meu-topico"
)

func main() {
	kafkaProvider := provider.NewProvider(Topic, 0)

	kafkaProvider.Connect()

	kafkaProvider.SetWriteDeadline(time.Now().Add(10 * time.Second))

	kafkaProvider.Write("OPAAA2!" + time.Now().Local().String())

	kafkaProvider.CloseConnection()
}
