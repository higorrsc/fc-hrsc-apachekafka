package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "apache-kafka-kafka-1:9092",
		"client.id":         "go-app-consumer",
		"group.id":          "go-app-group",
		"auto.offset.reset": "earliest",
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	c.SubscribeTopics([]string{"teste"}, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			log.Println(string(msg.Value), msg.TopicPartition)
		} else {
			log.Println(err.Error())
		}
	}
}
