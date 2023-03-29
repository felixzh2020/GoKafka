package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"sync"
)

func main() {
	// 消费者实例
	consumer, err := sarama.NewConsumer([]string{"felixzh:6667"}, nil)
	if err != nil {
		log.Print(err)
		os.Exit(0)
	}
	// 所有分区
	partitionList, err := consumer.Partitions("test")
	if err != nil {
		log.Println(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	// 遍历消费所有分区
	for partition := range partitionList {
		consumer, err := consumer.ConsumePartition("test", int32(partition), sarama.OffsetOldest)
		if err != nil {
			log.Println(err)
		}

		defer consumer.AsyncClose()

		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range consumer.Messages() {
				log.Printf("Partition:%d Offset:%d Key:%v Value:%v", msg.Partition, msg.Offset, msg.Key, msg.Value)
			}
		}(consumer)
	}
	wg.Wait()
}
