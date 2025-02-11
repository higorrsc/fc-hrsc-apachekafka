package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producer := NewKafkaProducer()
	Publish("mensagem", "teste", producer, nil)
	producer.Flush(1000)
}

// NewKafkaProducer returns a new Kafka producer.
//
// The returned producer is configured to connect to the broker at
// apache-kafka-kafka-1:9092. If the connection to the broker fails, the
// error is logged with log.Println.
func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "apache-kafka-kafka-1:9092",
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

// Publish sends a message to a specified Kafka topic using the provided producer.
//
// Parameters:
// - msg: the message to be sent.
// - topic: the Kafka topic to which the message will be published.
// - producer: the Kafka producer used to send the message.
// - key: an optional key for partitioning the message.
//
// Returns an error if the message could not be produced.

func Publish(msg string, topic string, producer *kafka.Producer, key []byte) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}

	err := producer.Produce(message, nil)
	if err != nil {
		return err
	}

	return nil
}
