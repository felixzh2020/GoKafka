package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
)

func main() {
	config := sarama.NewConfig()
	// ack级别
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	// 设置分区策略
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	msg := &sarama.ProducerMessage{}
	msg.Topic = "test"
	msg.Value = sarama.StringEncoder("123")

	producer, err := sarama.NewSyncProducer([]string{"felixzh:6667"}, config)
	if err != nil {
		log.Print(err)
		os.Exit(0)
	}
	defer func(producer sarama.SyncProducer) {
		err := producer.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(producer)

	message, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Println(err)
		os.Exit(0)
	}
	log.Printf("Message:%d Offset:%d", message, offset)

}
