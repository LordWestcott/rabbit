package rabbit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// PubSubManager handles the connection to RabbitMQ and provides methods
// for publishing messages in a thread-safe manner
type PubSubManager struct {
	channelPoolSize      int
	conn                 *amqp.Connection
	channelPool          []*amqp.Channel
	channelMutex         []*sync.Mutex
	exchanges            map[string]string
	msgChan              chan message
	ctx                  context.Context
	cancel               context.CancelFunc
	listeners            map[string]*listener
	listenersMutex       sync.Mutex
	connectionMutex      sync.Mutex
	reconnecting         bool
	maxReconnectAttempts int
	reconnectBackoff     time.Duration
	publishConfirms      bool
	circuitBreaker       *CircuitBreaker
}

// NewPubSubManager creates a new PubSubManager with default options
func NewPubSubManager(channelPoolSize int) *PubSubManager {
	opts := PubSubManagerOptions{
		MaxReconnectAttempts: 10,
		ReconnectBackoff:     time.Second * 2,
		PublishConfirms:      false,
	}
	return NewPubSubManagerWithOptions(channelPoolSize, opts)
}

// NewPubSubManagerWithOptions creates a new PubSubManager with custom options
func NewPubSubManagerWithOptions(channelPoolSize int, opts PubSubManagerOptions) *PubSubManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &PubSubManager{
		channelPoolSize:      channelPoolSize,
		exchanges:            make(map[string]string),
		msgChan:              make(chan message, 100),
		ctx:                  ctx,
		cancel:               cancel,
		listeners:            make(map[string]*listener),
		maxReconnectAttempts: opts.MaxReconnectAttempts,
		reconnectBackoff:     opts.ReconnectBackoff,
		publishConfirms:      opts.PublishConfirms,
	}
}

var amqpDial = amqp.Dial

// Start initializes the connection to RabbitMQ and sets up the channel pool
func (p *PubSubManager) Start(url string) error {
	if url == "" {
		return errors.New("RabbitMQ URL is not set")
	}

	var err error
	p.conn, err = amqpDial(url)
	if err != nil {
		return err
	}

	p.channelPool = make([]*amqp.Channel, p.channelPoolSize)
	p.channelMutex = make([]*sync.Mutex, p.channelPoolSize)

	for i := 0; i < p.channelPoolSize; i++ {
		ch, err := p.conn.Channel()
		if err != nil {
			// Close previously created channels
			for j := 0; j < i; j++ {
				p.channelPool[j].Close()
			}
			p.conn.Close()
			return err
		}

		p.channelPool[i] = ch
		p.channelMutex[i] = &sync.Mutex{}
	}

	// Initialize circuit breaker
	p.circuitBreaker = NewCircuitBreaker(5, 30*time.Second)

	// Start the message publishing worker
	if p.publishConfirms {
		p.EnablePublishConfirms()
		go p.messageWorkerWithConfirms()
	} else {
		go p.messageWorker()
	}

	// Set up connection monitoring
	go p.monitorConnection(url)

	return nil
}

// initTopic declares a topic exchange (private method for internal use)
func (p *PubSubManager) initTopic(topicName string) error {
	if _, exists := p.exchanges[topicName]; exists {
		// Topic already initialized
		return nil
	}

	// Use the first channel with a lock for declaring the exchange
	p.channelMutex[0].Lock()
	defer p.channelMutex[0].Unlock()

	err := p.channelPool[0].ExchangeDeclare(
		topicName, // exchange name
		"topic",   // exchange type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		return err
	}

	p.exchanges[topicName] = "topic"
	return nil
}

// handleChannelFailure recreates a failed channel
func (p *PubSubManager) handleChannelFailure(index int) {
	p.channelMutex[index].Lock()
	defer p.channelMutex[index].Unlock()

	// Close the failed channel if it exists
	if p.channelPool[index] != nil {
		p.channelPool[index].Close()
	}

	// Check if connection exists
	if p.conn == nil {
		return
	}

	// Create a new channel
	ch, err := p.conn.Channel()
	if err != nil {
		return
	}

	p.channelPool[index] = ch
}

// Close stops the worker and closes all channels and the connection
func (p *PubSubManager) Close() error {
	// Cancel the context to stop all workers
	p.cancel()

	// Stop all listeners
	p.listenersMutex.Lock()
	errs := []string{}
	for id, listener := range p.listeners {
		listener.cancel()
		if err := listener.channel.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("listener %s: %s", id, err.Error()))
		}
	}
	if len(errs) > 0 {
		return errors.New("failed to close listeners: " + strings.Join(errs, ", "))
	}
	p.listeners = make(map[string]*listener)
	p.listenersMutex.Unlock()

	// Close all channels
	for i, ch := range p.channelPool {
		p.channelMutex[i].Lock()
		ch.Close()
		p.channelMutex[i].Unlock()
	}

	// Close the connection
	if p.conn != nil {
		p.conn.Close()
	}

	return nil
}

// InitTopic is a public method to explicitly initialize a topic exchange
func (p *PubSubManager) InitTopic(topicName string) error {
	return p.initTopic(topicName)
}

func (p *PubSubManager) attemptReconnect(url string) {
	p.connectionMutex.Lock()
	if p.reconnecting {
		p.connectionMutex.Unlock()
		return
	}
	p.reconnecting = true
	p.connectionMutex.Unlock()

	defer func() {
		p.connectionMutex.Lock()
		p.reconnecting = false
		p.connectionMutex.Unlock()
	}()

	// Implement exponential backoff
	backoff := p.reconnectBackoff

	for i := 0; i < p.maxReconnectAttempts; i++ {
		// Try to reconnect
		conn, err := amqp.Dial(url)
		if err == nil {
			// Reconnected successfully
			p.rebuildAfterReconnect(conn)
			return
		}

		select {
		case <-time.After(backoff):
			// Increase backoff for next attempt
			backoff = time.Duration(float64(backoff) * 1.5)
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *PubSubManager) rebuildAfterReconnect(conn *amqp.Connection) {
	p.connectionMutex.Lock()
	defer p.connectionMutex.Unlock()

	// Close old resources
	for _, ch := range p.channelPool {
		if ch != nil {
			ch.Close()
		}
	}

	// Set new connection
	if p.conn != nil {
		p.conn.Close()
	}
	p.conn = conn

	// Create a new channel pool
	newChannelPool := make([]*amqp.Channel, p.channelPoolSize)

	// Try to rebuild all channels
	channelCreationErrors := 0
	for i := 0; i < p.channelPoolSize; i++ {
		ch, err := p.conn.Channel()
		if err != nil {
			channelCreationErrors++
			continue
		}
		newChannelPool[i] = ch
	}

	// Only replace the pool if we have at least one working channel
	if channelCreationErrors < p.channelPoolSize {
		// Close old channels first
		for _, ch := range p.channelPool {
			if ch != nil {
				ch.Close()
			}
		}
		p.channelPool = newChannelPool
	} else {
		// Close the new connection and channels
		for _, ch := range newChannelPool {
			if ch != nil {
				ch.Close()
			}
		}
		conn.Close()
		return
	}

	// Redeclare exchanges
	for name, exchangeType := range p.exchanges {
		p.channelPool[0].ExchangeDeclare(
			name,
			exchangeType,
			true,
			false,
			false,
			false,
			nil,
		)
	}

	// Rebuild listeners
	p.rebuildListeners()
}
