package provider

import (
	"context"
	"encoding/json"
	"fmt"
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

type MessageContent interface {
	Unmarshal([]byte) any
}

func NewProvider(topic string, partition int) *KafkaProvider {
	kafkaProvider := &KafkaProvider{
		topic:     topic,
		partition: 0,
	}

	return kafkaProvider
}

func (kafkaProvider *KafkaProvider) Connect() {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9093", kafkaProvider.topic, kafkaProvider.partition)

	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	kafkaProvider.connection = conn
}

func (kafkaProvider *KafkaProvider) GetConnection() *kafka.Conn {
	if connection, ok := kafkaProvider.connection.(*kafka.Conn); ok {
		return connection
	}

	panic("invalid implementation")
}

func (kafkaProvider *KafkaProvider) SetWriteDeadline(time time.Time) {
	kafkaProvider.GetConnection().SetWriteDeadline(time)
}

func (kafkaProvider *KafkaProvider) Write(message any) {
	jsonStruct, err := json.Marshal(message)

	if err != nil {
		log.Fatal("failed to convert object:", err)
	}

	kafkaProvider.WriteBytes(jsonStruct)
}

func (kafkaProvider *KafkaProvider) WriteBytes(message []byte) {
	_, err := kafkaProvider.GetConnection().WriteMessages(
		kafka.Message{Value: []byte(message)},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
}

func (kafkaProvider *KafkaProvider) SetReadDeadline(time time.Time) {
	kafkaProvider.GetConnection().SetReadDeadline(time)
}

func (kafkaProvider *KafkaProvider) Read() [][]byte {
	batch := kafkaProvider.GetConnection().ReadBatch(0, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message

	var messages [][]byte

	timeout := time.After(5 * time.Second)

readChannel:
	for {
		select {
		case <-timeout:
			fmt.Println("Timeout reached. Exiting the loop.")
			break readChannel
		default:
			n, err := batch.Read(b)
			if err != nil {
				break readChannel
			}

			message := make([]byte, n)
			copy(message, b[:n])
			messages = append(messages, message)
		}
	}

	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}

	return messages
}

func ConvertBytes[T MessageContent](convertedContent []T, rawMessages [][]byte) []T {
	for _, message := range rawMessages {
		var object T

		output := object.Unmarshal(message)

		if convertedOutput, ok := output.(T); ok {
			convertedContent = append(convertedContent, convertedOutput)
		} else {
			log.Fatal("Classe não implementa interface necessária")
		}
	}

	return convertedContent
}

func (kafkaProvider *KafkaProvider) CloseConnection() {
	if connection, ok := kafkaProvider.connection.(*kafka.Conn); ok {
		err := connection.Close()

		if err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}
}
