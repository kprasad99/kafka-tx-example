package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
)

type TxKafka struct {
	bootstrapServers string
	input            *kafka.Producer
}

const retryNum = 3

func main() {

	tx := &TxKafka{
		bootstrapServers: "localhost:19092,localhost:29092,localhost:39092",
	}

	// create topics if missing
	httpInputTopic := "kp.go.input1"
	tx.createTopicIfMissing(&httpInputTopic, 5, 2)
	httpProcessedTopic := "kp.go.tx.topic"
	tx.createTopicIfMissing(&httpProcessedTopic, 5, 2)

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": tx.bootstrapServers,
	})
	if err != nil {
		log.Panicln("Failed to get new producer", err)
	}
	defer p.Close()
	tx.input = p

	go tx.topicTx(&httpInputTopic, &httpProcessedTopic, "kp.group1", "kp.tx.1")

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/send", tx.handleHttpReq)
	http.ListenAndServe(":8080", r)

}

func (tx TxKafka) handleHttpReq(w http.ResponseWriter, r *http.Request) {
	topic := "kp.go.input1"
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("data")
	repeatStr := r.URL.Query().Get("repeat")
	repeat := 1
	if repeatStr != "" {
		repeat, _ = strconv.Atoi(repeatStr)
	}
	log.Println("Sending data ", value, " with key ", key, " to kafka with repeat order of ", repeat)
	var err error
	for i := 0; i < repeat; i++ {
		err = tx.input.Produce(&kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
			TopicPartition: kafka.TopicPartition{
				Topic: &topic,
			},
		}, nil)
		if err != nil {
			break
		}
	}

	if err == nil {
		w.Write([]byte("Sent message to kafka"))
	} else {
		fmt.Fprintf(w, "Failed while sending data to kafka %v", err)
	}

}

func (tx TxKafka) createTopicIfMissing(topic *string, numParts int, replicationFactor int) {

	log.Println("Bootstrap servers ", tx.bootstrapServers)
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": tx.bootstrapServers})
	defer a.Close()
	if err != nil {
		log.Panicf("Failed to create Admin client: %s\n", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		log.Panicln("ParseDuration(60s)")
	}

	results, err := a.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             *topic,
			NumPartitions:     numParts,
			ReplicationFactor: replicationFactor}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}
	log.Println("Create topic ", *topic, " with result ", results)

}

func (tx TxKafka) topicTx(inTopic *string, outTopic *string, groupId string, txId string) {

	partitionPositionMap := make(map[int32]kafka.TopicPartition)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	topics := []string{*inTopic}
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	defer close(sigchan)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": tx.bootstrapServers,
		"group.id":          groupId,
		"isolation.level":   "read_committed",
	})
	defer func() {
		if c != nil {
			c.Close()
		}
	}()

	if err != nil {
		log.Panicln("Failed to create consumer for topic ", *inTopic, " with error", err)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": tx.bootstrapServers,
		"transactional.id":  txId,
		// if transaction is set idempotency is set by default
		//	"enable.idempotence": true,
	})

	defer func() {
		if p != nil {
			p.Close()
		}
	}()

	if err != nil {
		log.Panicln("Failed to create producer for topic ", *outTopic, " with error", err)
	}

	log.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)

	//	go producerListener(p, p.Events())

	run := true
	err = p.InitTransactions(ctx)
	if err != nil {
		log.Panicln("Failed to init transaction", err)
	}

	err = p.BeginTransaction()
	if err != nil {
		log.Panicln("Failed to start transaction", err)
	}
	ticker := time.NewTicker(1000 * time.Millisecond)

	for run {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating consumer\n", sig)
			run = false
		case <-ticker.C:
			commitTx(&ctx, c, p, partitionPositionMap)
			if err != nil {
				log.Printf("Failed to commit "+
					"transaction: %v\n", err)
				return
			}
			p.BeginTransaction()
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				log.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					log.Printf("%% Headers: %v\n", e.Headers)
				}
				err = p.Produce(&kafka.Message{
					Key:           e.Key,
					Value:         e.Value,
					Headers:       e.Headers,
					Timestamp:     e.Timestamp,
					TimestampType: e.TimestampType,
					Opaque:        e.Opaque,
					TopicPartition: kafka.TopicPartition{
						Topic:     outTopic,
						Partition: kafka.PartitionAny,
					},
				}, nil)
				if err == nil {
					position, _ := getConsumerPosition(c, e.TopicPartition.Partition, *inTopic)
					partitionPositionMap[e.TopicPartition.Partition] = position
					commitTx(&ctx, c, p, partitionPositionMap)
				} else {
					log.Println("Failed while sending ", err.Error())
				}
			case kafka.Error:
				log.Printf("%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
}

func commitTx(ctx *context.Context, consumer *kafka.Consumer, producer *kafka.Producer, partitionOffsets map[int32]kafka.TopicPartition) error {
	cgmd, err := consumer.GetConsumerGroupMetadata()
	if err != nil {
		log.Printf("Failed to get Consumer Group "+
			"Metadata %v\n", err)
		return err
	}
	partitions := getAllPartitionPositions(partitionOffsets)
	for i := 0; i < retryNum; i++ {
		err = producer.SendOffsetsToTransaction(nil, partitions, cgmd)
		if err != nil {
			log.Printf("SendOffsetsToTransaction() "+
				"failed: %s\n", err)
			if err.(kafka.Error).IsRetriable() {
				if i == retryNum-1 {
					log.Printf("SendOffsetsToTransaction() "+
						"failed with max retry %d times: %s\n", retryNum, err)
					return err
				}
				time.Sleep(3 * time.Second)
				continue
			} else if err.(kafka.Error).TxnRequiresAbort() {
				err = producer.AbortTransaction(nil)
				if err != nil {
					log.Printf("AbortTransaction() "+
						"failed: %s\n", err)
					return err
				}
				rewindConsumerPosition(consumer)
				return nil
			} else {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

// getConsumerPosition gets consumer position according to partition id
func getConsumerPosition(consumer *kafka.Consumer, partition int32,
	topic string) (kafka.TopicPartition, error) {
	position, err := consumer.Position([]kafka.TopicPartition{{
		Topic: &topic, Partition: partition}})
	if err != nil {
		log.Printf("Failed to get position %s\n", err)
		return kafka.TopicPartition{}, err
	} else if len(position) == 0 {
		err = fmt.Errorf("the position doesn't contain any element")
		return kafka.TopicPartition{}, err
	}
	return position[0], nil
}

// getAllPartitionPositions get offsets for all partitions
func getAllPartitionPositions(positionMap map[int32]kafka.TopicPartition) []kafka.TopicPartition {
	partitionPosition := make([]kafka.TopicPartition, 0, len(positionMap))

	for _, v := range positionMap {
		partitionPosition = append(partitionPosition, v)
	}
	return partitionPosition
}

// rewindConsumerPosition Rewinds consumer's position to the
// pre-transaction offset
func rewindConsumerPosition(c *kafka.Consumer) error {
	assignment, err := c.Assignment()
	if err != nil {
		log.Printf("Assignment() failed: %s\n", err)
		return err
	}

	committed, err := c.Committed(assignment, 30*1000 /* 30s */)
	if err != nil {
		log.Printf("Committed() failed: %s\n", err)
		return err
	}

	for _, tp := range committed {
		if tp.Offset < 0 {
			tp.Offset = kafka.OffsetBeginning
		}
		err := c.Seek(tp, 1)
		if err != nil {
			log.Printf("Seek() failed: %s\n", err)
			return err
		}
	}
	return nil
}

func producerListener(p *kafka.Producer, deliveryChan <-chan kafka.Event) {

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	defer close(sigchan)
	run := true
	log.Println("Starting producer status listener")
	for run {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating producer\n", sig)
			run = false
		case evt := <-deliveryChan:
			log.Println("Starting to listen for events")
			m := evt.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				log.Printf("Delivered message to topic %s [%d] at offset %v\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}
		}
	}

}

/**
Examples for producer
**/
func producer() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "myTopic"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
