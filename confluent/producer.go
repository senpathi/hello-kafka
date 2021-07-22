package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"
)

func produce() {
	producer, err := kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers":     "localhost:9092",
			"request.required.acks": "-1",
		},
	)
	if err != nil {
		log.Fatalln(err)
	}

	defer producer.Close()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	ticker := time.NewTicker(time.Second * 2)
	produced := 0
	topic := "my_topic"

	//receive message delivery report to this channel
	deliveryChan := make(chan kafka.Event, 10000)

producerLoop:
	for {
		select {
		case <-ticker.C:
			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(strconv.Itoa(produced)),
				Value:          []byte("confluent producer message number " + strconv.Itoa(produced)),
				Timestamp:      time.Now(),
				Headers:        []kafka.Header{{Key: "header_key", Value: []byte("header_value")}},
			}

			err := producer.Produce(msg, deliveryChan)
			if err != nil {
				log.Printf("FAILED to produce message: %s\n", err)
			}

			// from waiting the message's delivery report makes the producer's write synchronous
			e := <-deliveryChan
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				log.Printf("FAILED to send message: %s\n", err)
			} else {
				log.Printf("> confluent message sent to partition %d at offset %d\n",
					m.TopicPartition.Partition, m.TopicPartition.Offset)
			}
			produced++

		case <-signals:
			break producerLoop
		}
	}

	// Wait for message deliveries before shutting down
	producer.Flush(15 * 1000)
}
