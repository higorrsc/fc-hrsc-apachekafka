package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("mensagem", "teste", producer, nil, deliveryChan)
	go DeliveryReport(deliveryChan)

	fmt.Println("sent message")
	producer.Flush(2000)

	// e := <-deliveryChan
	// m, ok := e.(*kafka.Message)
	// if !ok {
	// 	log.Println("Couldn't get message from delivery channel")
	// }
	// if m.TopicPartition.Error != nil {
	// 	log.Println(m.TopicPartition.Error)
	// } else {
	// 	log.Println("Delivered message:", m.TopicPartition)
	// }

	// producer.Flush(1000)
}

// NewKafkaProducer returns a new Kafka producer.
//
// The returned producer is configured to connect to the broker at
// apache-kafka-kafka-1:9092. If the connection to the broker fails, the
// error is logged with log.Println.
func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "apache-kafka-kafka-1:9092",
		"delivery.timeout.ms": "0",
		"acks":                "all",
		"enable.idempotence":  "true",
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

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}

	return nil
}

// DeliveryReport starts a goroutine that continually receives from the deliveryChan
// channel and logs the success or failure of each message delivery.
func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Println("Delivery failed:", ev.TopicPartition)
			} else {
				log.Println("Delivered message to topic:", ev.TopicPartition)
			}
		}
	}
}
