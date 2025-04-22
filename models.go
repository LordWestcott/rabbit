package rabbit

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

// Message represents a message to be published to RabbitMQ
type message struct {
	topicName  string
	routingKey string
	payload    interface{}
	ttl        int // milliseconds
	priority   uint8
}

// MessageHandler is a function that processes received messages
// On error, the message is nacked and requeued
type MessageHandler func(payload []byte) error

// MessageHandlerWithContext is a function that processes received messages with context
// On error, the message is nacked and requeued
type MessageHandlerWithContext func(ctx context.Context, payload []byte) error

// listener represents a topic subscription
type listener struct {
	topicName  string
	routingKey string
	queueName  string
	channel    *amqp.Channel
	handler    MessageHandler
	ctx        context.Context
	cancel     context.CancelFunc
}

// PubSubManagerOptions configures the behavior of the PubSubManager
type PubSubManagerOptions struct {
	MaxReconnectAttempts int
	ReconnectBackoff     time.Duration
	PublishConfirms      bool
}

// QueueConfig provides configuration options for RabbitMQ queues
type QueueConfig struct {
	Name                 string
	Durable              bool
	AutoDelete           bool
	Exclusive            bool
	TTL                  int // Queue TTL in milliseconds
	MaxLength            int // Maximum number of messages
	MaxLengthBytes       int // Maximum size in bytes
	DeadLetterExchange   string
	DeadLetterRoutingKey string
}

// HealthStatus represents the current health state of the PubSubManager
type HealthStatus struct {
	Connected         bool
	ActiveListeners   int
	ReconnectAttempts int
	LastError         string
	CircuitState      string
}
