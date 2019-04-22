package common

import (
	"crypto/tls"
	"fmt"
	"math/rand"
	"time"

	stan "github.com/nats-io/go-nats-streaming"
)

// NATSConnector ...
type NATSConnector struct{}

// Connect opens a connection to Nats, declares an exchange, opens a channel,
// declares and binds the queue and enables publish notifications
func (ac *NATSConnector) Connect(URL, clusterID, clientID string, tlsConfig *tls.Config, queueName string) (stan.Conn, <-chan *stan.Msg, stan.Subscription, error) {
	// Connect to server
	conn, channel, subscription, err := ac.Open(URL, clusterID, clientID, tlsConfig, queueName)

	if err != nil {
		return nil, nil, nil, err
	}

	return conn, channel, subscription, nil
}

// DeleteQueue deletes a queue by name
func (ac *NATSConnector) DeleteQueue(sub stan.Subscription) error {
	err := sub.Unsubscribe()

	return err
}

// InspectQueue provides information about a specific queue
/*func (*NATSConnector) InspectQueue(channel *amqp.Channel, queueName string) (*amqp.Queue, error) {
	queueState, err := channel.QueueInspect(queueName)
	if err != nil {
		return nil, fmt.Errorf("Queue inspect error: %s", err)
	}

	return &queueState, nil
}*/

// Open new Nats connection
func (ac *NATSConnector) Open(URL, clusterID, clientID string, tlsConfig *tls.Config, queueName string) (stan.Conn, <-chan *stan.Msg, stan.Subscription, error) {
	clientID = fmt.Sprintf("client_%d", rand.Intn(100))

	// Connect
	// From amqp docs: DialTLS will use the provided tls.Config when it encounters an tls://nats scheme
	// and will dial a plain connection when it encounters an nats:// scheme.
	conn, err := stan.Connect(clusterID, clientID, stan.NatsURL(URL))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("Dial error: %s", err)
	}

	// Subscribe with manual ack mode, and set AckWait to 60 seconds
	aw, _ := time.ParseDuration("60s")

	// Channel Subscriber
	channel := make(chan *stan.Msg, 64)
	subscription, err := conn.Subscribe(queueName, func(m *stan.Msg) {
		channel <- m
	}, stan.SetManualAckMode(), stan.AckWait(aw))

	return conn, channel, subscription, nil
}

// Close connection
func (ac *NATSConnector) Close(subscription stan.Subscription, conn stan.Conn) error {
	if subscription != nil {
		if err := subscription.Unsubscribe(); err != nil {
			return fmt.Errorf("Close channel error: %s", err)
		}
	}

	if conn != nil {
		conn.Close()
	}

	return nil
}
