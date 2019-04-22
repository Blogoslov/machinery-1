package brokers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	stan "github.com/nats-io/go-nats-streaming"
)

// NATSBroker represents an NATS broker
type NATSBroker struct {
	Broker
	common.NATSConnector
	processingWG sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
}

// NewNATSBroker creates new NATSBroker instance
func NewNATSBroker(cnf *config.Config) Interface {
	return &NATSBroker{Broker: New(cnf), NATSConnector: common.NATSConnector{}}
}

// StartConsuming enters a loop and waits for incoming messages
func (b *NATSBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)

	conn, channel, subscription, err := b.Connect(
		b.cnf.Broker,
		b.cnf.NATS.ClusterID,
		b.cnf.NATS.ClientID,
		b.cnf.TLSConfig,
		b.cnf.DefaultQueue, // topic name
	)
	if err != nil {
		b.retryFunc(b.retryStopChan)
		return b.retry, err
	}
	defer b.Close(subscription, conn)

	log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

	if err := b.consume(channel, concurrency, taskProcessor); err != nil {
		return b.retry, err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.retry, nil
}

// StopConsuming quits the loop
func (b *NATSBroker) StopConsuming() {
	b.stopConsuming()

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()
}

// Publish places a new message on the default queue
func (b *NATSBroker) Publish(signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	AdjustRoutingKey(b, signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			delayMs := int64(signature.ETA.Sub(now) / time.Millisecond)

			return b.delay(signature, delayMs)
		}
	}

	conn, _, subscription, err := b.Connect(
		b.cnf.Broker,
		b.cnf.NATS.ClusterID,
		b.cnf.NATS.ClientID,
		b.cnf.TLSConfig,
		signature.RoutingKey, // topic name
	)
	if err != nil {
		return err
	}
	defer b.Close(subscription, conn)

	if err := conn.Publish(signature.RoutingKey, msg); err != nil {
		return err
	}

	//confirmed := <-confirmsChan

	//if confirmed.Ack {
	return nil
	//}

	//return fmt.Errorf("Failed delivery of delivery tag: %v", confirmed.DeliveryTag)*/
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *NATSBroker) consume(deliveries <-chan *stan.Msg, concurrency int, taskProcessor TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error)

	for {
		select {
		case d := <-deliveries:
			if concurrency > 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.stopChan:
			return nil
		}
	}

	return nil
}

// consumeOne processes a single message using TaskProcessor
func (b *NATSBroker) consumeOne(delivery *stan.Msg, taskProcessor TaskProcessor) error {
	if len(delivery.Data) == 0 {
		return errors.New("Received an empty message") // RabbitMQ down?
	}

	// Unmarshal message body into signature struct
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery.Data))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return NewErrCouldNotUnmarshaTaskSignature(delivery.Data, err)
	}

	// If the task is not registered, we nack it and requeue,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		/*if !delivery.Redelivered {
			log.INFO.Printf("Task not registered with this worker. Requeing message: %s", delivery.Data)
		}*/
		log.INFO.Printf("Task not registered with this worker -- Look at me")
		return nil
	}

	log.INFO.Printf("Received new message: %s", delivery.Data)

	err := taskProcessor.Process(signature)
	delivery.Ack()
	return err
}

// delay a task by delayDuration miliseconds, the way it works is a new queue
// is created without any consumers, the message is then published to this queue
// with appropriate ttl expiration headers, after the expiration, it is sent to
// the proper queue with consumers
func (b *NATSBroker) delay(signature *tasks.Signature, delayMs int64) error {
	if delayMs <= 0 {
		return errors.New("Cannot delay task by 0ms")
	}

	message, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// It's necessary to redeclare the queue each time (to zero its TTL timer).
	queueName := fmt.Sprintf(
		"delay.%d.%s.%s",
		delayMs, // delay duration in mileseconds
	)
	conn, _, subscription, err := b.Connect(
		b.cnf.Broker,
		b.cnf.NATS.ClusterID,
		b.cnf.NATS.ClientID,
		b.cnf.TLSConfig,
		queueName, // topic name
	)
	if err != nil {
		return err
	}
	defer b.Close(subscription, conn)

	if err := conn.Publish(queueName, message); err != nil {
		return err
	}

	return nil
}
