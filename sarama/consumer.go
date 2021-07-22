package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"time"
)

func consumePartition() {
	conf := sarama.NewConfig()
	conf.Version = sarama.V2_8_0_0
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, conf)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("my_topic", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("sarama consumed message with key = %s, value = \"%s\", offset = %d\n",
				string(msg.Key), string(msg.Value), msg.Offset)
			consumed++
		case <-signals:
			time.Sleep(time.Second * 2) // waiting here to stop the producer. This is not necessary.
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)

}
