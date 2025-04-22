package rabbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/streadway/amqp"
)

// PublishMessage is the main public method for sending messages
func (p *PubSubManager) PublishMessage(topicName, routingKey string, payload interface{}) error {
	// Check if context is done before proceeding
	select {
	case <-p.ctx.Done():
		return fmt.Errorf("context canceled, publisher shutting down")
	default:
		// Continue with publishing
	}

	if !p.circuitBreaker.AllowRequest() {
		return errors.New("circuit breaker open")
	}

	select {
	case p.msgChan <- message{
		topicName:  topicName,
		routingKey: routingKey,
		payload:    payload,
	}:
		return nil
	default:
		return errors.New("message buffer is full")
	}
}

// PublishMessageWithTTL is a public method for sending messages with a TTL
func (p *PubSubManager) PublishMessageWithTTL(topicName, routingKey string, payload interface{}, ttlMs int) error {
	// Similar to PublishMessage but with TTL
	select {
	case p.msgChan <- message{
		topicName:  topicName,
		routingKey: routingKey,
		payload:    payload,
		ttl:        ttlMs,
	}:
		return nil
	default:
		return errors.New("message buffer is full")
	}
}

// PublishWithPriority is a public method for sending messages with a priority
func (p *PubSubManager) PublishWithPriority(topicName, routingKey string, payload interface{}, priority uint8) error {
	// Similar to PublishMessage but with priority
	select {
	case p.msgChan <- message{
		topicName:  topicName,
		routingKey: routingKey,
		payload:    payload,
		priority:   priority,
	}:
		return nil
	default:
		return errors.New("message buffer is full")
	}
}

// messageWorker is an internal method that processes messages from the channel
func (p *PubSubManager) messageWorker() {
	channelIndex := 0
	poolSize := len(p.channelPool)

	for {
		select {
		case <-p.ctx.Done():
			return

		case msg, ok := <-p.msgChan:
			if !ok {
				return
			}

			// Ensure the topic exists
			if _, exists := p.exchanges[msg.topicName]; !exists {
				if err := p.initTopic(msg.topicName); err != nil {
					continue
				}
			}

			// Get channel and lock
			currentChannel := channelIndex
			channelIndex = (channelIndex + 1) % poolSize

			p.channelMutex[currentChannel].Lock()

			// Convert payload to JSON
			jsonBody, err := json.Marshal(msg.payload)
			if err != nil {
				p.channelMutex[currentChannel].Unlock()
				continue
			}

			// Publish the message
			publishing := amqp.Publishing{
				ContentType:  "application/json",
				Body:         jsonBody,
				DeliveryMode: amqp.Persistent,
			}

			if msg.ttl > 0 {
				publishing.Expiration = strconv.Itoa(msg.ttl)
			}

			if msg.priority > 0 {
				publishing.Priority = msg.priority
			}

			err = p.channelPool[currentChannel].Publish(
				msg.topicName,
				msg.routingKey,
				false,
				false,
				publishing,
			)

			p.channelMutex[currentChannel].Unlock()

			if err != nil {
				// Handle channel failure - recreate the channel
				p.handleChannelFailure(currentChannel)

				p.circuitBreaker.RecordFailure()
			} else {
				p.circuitBreaker.RecordSuccess()
			}
		}
	}
}

// EnablePublishConfirms enables publisher confirm mode on all channels
func (p *PubSubManager) EnablePublishConfirms() {
	p.publishConfirms = true

	// Set up confirms on all channels
	for i, ch := range p.channelPool {
		if ch != nil {
			p.channelMutex[i].Lock()
			ch.Confirm(false) // noWait = false
			p.channelMutex[i].Unlock()
		}
	}
}

// Modify messageWorker to use confirms
func (p *PubSubManager) messageWorkerWithConfirms() {
	channelIndex := 0
	poolSize := len(p.channelPool)

	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, ok := <-p.msgChan:
			if !ok {
				return
			}

			// Ensure topic exists logic...

			currentChannel := channelIndex
			channelIndex = (channelIndex + 1) % poolSize
			p.channelMutex[currentChannel].Lock()

			// Set up confirms
			confirms := p.channelPool[currentChannel].NotifyPublish(make(chan amqp.Confirmation, 1))

			// Marshal payload
			jsonBody, err := json.Marshal(msg.payload)
			if err != nil {
				p.channelMutex[currentChannel].Unlock()
				continue
			}

			// Create publishing with all options
			publishing := amqp.Publishing{
				ContentType:  "application/json",
				Body:         jsonBody,
				DeliveryMode: amqp.Persistent,
			}

			if msg.ttl > 0 {
				publishing.Expiration = strconv.Itoa(msg.ttl)
			}

			if msg.priority > 0 {
				publishing.Priority = msg.priority
			}

			// Publish with confirmation
			err = p.channelPool[currentChannel].Publish(
				msg.topicName,
				msg.routingKey,
				false,
				false,
				publishing,
			)

			if err != nil {
				p.channelMutex[currentChannel].Unlock()
				continue
			}

			// Wait for confirmation
			confirmation := <-confirms
			if confirmation.Ack {
				p.circuitBreaker.RecordSuccess()
			} else {
				p.circuitBreaker.RecordFailure()
			}

			p.channelMutex[currentChannel].Unlock()
		}
	}
}
