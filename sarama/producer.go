package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"
)

//this produce function creates a sync producer and produce messages in every 2 seconds.
//if you need a producer function to produce data only when you calling it, you need to init and save
//producer reference in one time call function and reuse it to produce messages. write a separate producer function to
//produce message using above producer reference.
//for that you can use code snippet inside "case <-ticker.C"

func produce() {
	conf := sarama.NewConfig()
	conf.Version = sarama.V2_8_0_0
	conf.Producer.Return.Errors = true             // this must be true for sync producer
	conf.Producer.Return.Successes = true          // this must be true for sync producer
	conf.Producer.RequiredAcks = sarama.WaitForAll // wait for all makes sure the reliability of the produced message

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, conf)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	ticker := time.NewTicker(time.Second * 2)
	produced := 0

producerLoop:
	for {
		select {
		case <-ticker.C:
			msg := &sarama.ProducerMessage{
				Topic:     "my_topic",
				Key:       sarama.StringEncoder(strconv.Itoa(produced)),
				Value:     sarama.StringEncoder("sarama producer message number " + strconv.Itoa(produced)),
				Timestamp: time.Now(),
				Headers: []sarama.RecordHeader{
					{
						Key:   []byte("header_key"),
						Value: []byte("header_value"),
					},
				},
			}
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf("FAILED to send message: %s\n", err)
			} else {
				log.Printf("> sarama message sent to partition %d at offset %d\n", partition, offset)
			}
			produced++

		case <-signals:
			break producerLoop
		}
	}
}
