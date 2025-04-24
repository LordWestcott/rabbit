package rabbit

import (
	"context"
	"fmt"

	"github.com/streadway/amqp"
)

// ListenOpts is the options for listening to a topic.
// TopicName is the name of the topic to listen to.
// RoutingKey is the routing key to listen to.
// QueueName is the name of the queue to listen to.
// Handler is the handler to process the messages.
// ConcurrentHandlers is the number of concurrent handlers to run.
type ListenOpts struct {
	TopicName          string
	RoutingKey         string
	QueueName          string
	Handler            MessageHandler
	ConcurrentHandlers int
}

// Listen starts listening for messages on a topic with a specific routing key pattern
// and processes them with the provided handler function.
func (p *PubSubManager) Listen(opts ListenOpts) (string, error) {
	// Set default concurrent handlers to 1 if invalid value provided
	if opts.ConcurrentHandlers <= 0 {
		opts.ConcurrentHandlers = 1
	}

	// Create a unique listener ID
	listenerID := fmt.Sprintf("%s-%s-%s", opts.TopicName, opts.RoutingKey, opts.QueueName)

	// Check if listener already exists
	p.listenersMutex.Lock()
	if _, exists := p.listeners[listenerID]; exists {
		p.listenersMutex.Unlock()
		return "", fmt.Errorf("listener already exists with ID: %s", listenerID)
	}
	p.listenersMutex.Unlock()

	// Ensure the topic exists
	if err := p.InitTopic(opts.TopicName); err != nil {
		return "", err
	}

	// Create a new channel specifically for this listener
	ch, err := p.conn.Channel()
	if err != nil {
		return "", err
	}

	// Declare a queue to receive messages
	q, err := ch.QueueDeclare(
		opts.QueueName, // Queue name
		true,           // Durable
		false,          // Delete when unused
		false,          // Exclusive
		false,          // No-wait
		nil,            // Arguments
	)
	if err != nil {
		ch.Close()
		return "", err
	}

	// Bind the queue to the exchange with the routing key
	err = ch.QueueBind(
		q.Name,          // Queue name
		opts.RoutingKey, // Routing key
		opts.TopicName,  // Exchange
		false,           // No-wait
		nil,             // Arguments
	)
	if err != nil {
		ch.Close()
		return "", err
	}

	// Create a context for this consumer
	listenerCtx, listenerCancel := context.WithCancel(p.ctx)

	// Store the listener info
	p.listenersMutex.Lock()
	p.listeners[listenerID] = &listener{
		topicName:  opts.TopicName,
		routingKey: opts.RoutingKey,
		queueName:  opts.QueueName,
		channel:    ch,
		handler:    opts.Handler,
		ctx:        listenerCtx,
		cancel:     listenerCancel,
	}
	p.listenersMutex.Unlock()

	// Start consuming messages with specified number of concurrent handlers
	for i := 0; i < opts.ConcurrentHandlers; i++ {
		go p.consumeMessages(listenerCtx, ch, opts.QueueName, opts.Handler, listenerID)
	}

	return listenerID, nil
}

// consumeMessages handles consuming messages from a queue and processes them with the handler
func (p *PubSubManager) consumeMessages(ctx context.Context, ch *amqp.Channel, queueName string, handler MessageHandler, listenerID string) error {
	msgs, err := ch.Consume(
		queueName, // Queue
		"",        // Consumer
		false,     // Auto-Ack
		false,     // Exclusive
		false,     // No-local
		false,     // No-Wait
		nil,       // Args
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				// Get listener info for recovery
				p.listenersMutex.Lock()
				listener := p.listeners[listenerID]
				p.listenersMutex.Unlock()

				if listener != nil {
					// Only trigger rebuild from one goroutine to avoid multiple rebuilds
					p.listenersMutex.Lock()
					if listener.channel == ch {
						p.rebuildListeners()
					}
					p.listenersMutex.Unlock()
				}
				return nil
			}

			// Process the message with the handler
			err := handler(msg.Body)
			if err != nil {
				// Nack the message to be requeued
				if err := msg.Nack(false, true); err != nil {
					//TODO: Handle error.
				}
			} else {
				// Ack the message
				if err := msg.Ack(false); err != nil {
					//TODO: Handle error.
				}
			}
		}
	}
}

// StopListener stops a listener and cleans up resources
func (p *PubSubManager) StopListener(listenerID string) error {
	p.listenersMutex.Lock()
	defer p.listenersMutex.Unlock()

	listener, exists := p.listeners[listenerID]
	if !exists {
		return fmt.Errorf("listener not found: %s", listenerID)
	}

	// Cancel the listener's context
	listener.cancel()

	// Close the channel
	if err := listener.channel.Close(); err != nil {
		//TODO: Handle error.
		// Continue anyway to cleanup the listener
	}

	// Remove from the map
	delete(p.listeners, listenerID)

	return nil
}

// rebuildListeners recreates all listeners after a connection failure
func (p *PubSubManager) rebuildListeners() {
	p.listenersMutex.Lock()
	listeners := make(map[string]*listener)
	for id, l := range p.listeners {
		listeners[id] = l
	}
	p.listenersMutex.Unlock()

	for id, l := range listeners {
		// Create new channel for listener
		ch, err := p.conn.Channel()
		if err != nil {
			//TODO: Handle error.
			continue
		}

		// Declare queue
		q, err := ch.QueueDeclare(
			l.queueName,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			//TODO: Handle error.
			continue
		}

		// Bind queue
		err = ch.QueueBind(
			q.Name,
			l.routingKey,
			l.topicName,
			false,
			nil,
		)
		if err != nil {
			//TODO: Handle error.
			continue
		}

		// Update the listener with the new channel
		p.listenersMutex.Lock()
		if listener, exists := p.listeners[id]; exists {
			// Cancel previous context if it exists
			if listener.cancel != nil {
				listener.cancel()
			}

			// Create new context for this consumer
			listenerCtx, listenerCancel := context.WithCancel(p.ctx)

			// Update listener with new channel and context
			listener.channel = ch
			listener.ctx = listenerCtx
			listener.cancel = listenerCancel

			// Start the consumer again
			go p.consumeMessages(listenerCtx, ch, q.Name, listener.handler, id)
		}
		p.listenersMutex.Unlock()
	}
}

// ListenerExists checks if a listener with the given parameters already exists
func (p *PubSubManager) ListenerExists(topicName, routingKey, queueName string) bool {
	listenerID := fmt.Sprintf("%s-%s-%s", topicName, routingKey, queueName)

	p.listenersMutex.Lock()
	defer p.listenersMutex.Unlock()

	_, exists := p.listeners[listenerID]
	return exists
}

// GetActiveListeners returns a list of active listener IDs
func (p *PubSubManager) GetActiveListeners() []string {
	p.listenersMutex.Lock()
	defer p.listenersMutex.Unlock()

	listenerIDs := make([]string, 0, len(p.listeners))
	for id := range p.listeners {
		listenerIDs = append(listenerIDs, id)
	}

	return listenerIDs
}

// GetListenerInfo returns information about a specific listener
func (p *PubSubManager) GetListenerInfo(listenerID string) (map[string]string, error) {
	p.listenersMutex.Lock()
	defer p.listenersMutex.Unlock()

	listener, exists := p.listeners[listenerID]
	if !exists {
		return nil, fmt.Errorf("listener not found: %s", listenerID)
	}

	return map[string]string{
		"topicName":  listener.topicName,
		"routingKey": listener.routingKey,
		"queueName":  listener.queueName,
	}, nil
}

// SetQoS sets the prefetch count for RabbitMQ channels
func (p *PubSubManager) SetQoS(prefetchCount int) error {
	for i, ch := range p.channelPool {
		p.channelMutex[i].Lock()
		err := ch.Qos(prefetchCount, 0, false)
		p.channelMutex[i].Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
