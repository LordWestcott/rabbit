package rabbit

import (
	"context"
	"sync"
	"time"
)

// BatchPublisher collects messages and publishes them in batches
type BatchPublisher struct {
	manager       *PubSubManager
	topic         string
	routingKey    string
	messages      []interface{}
	batchSize     int
	batchInterval time.Duration
	mu            sync.Mutex
	timer         *time.Timer
	ctx           context.Context
}

// NewBatchPublisher creates a new batch publisher
func NewBatchPublisher(manager *PubSubManager, topic, routingKey string, batchSize int, interval time.Duration) *BatchPublisher {
	bp := &BatchPublisher{
		manager:       manager,
		topic:         topic,
		routingKey:    routingKey,
		messages:      make([]interface{}, 0, batchSize),
		batchSize:     batchSize,
		batchInterval: interval,
		ctx:           manager.ctx,
	}

	bp.timer = time.AfterFunc(interval, func() {
		select {
		case <-bp.ctx.Done():
			return
		default:
			bp.publishBatch()
		}
	})

	return bp
}

// Add adds a message to the batch
func (bp *BatchPublisher) Add(message interface{}) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.messages = append(bp.messages, message)

	if len(bp.messages) >= bp.batchSize {
		// Stop the timer so it doesn't fire while we're publishing
		bp.timer.Stop()
		go bp.publishBatch() // Run in goroutine to avoid blocking the caller
	}
}

// publishBatch publishes all collected messages
func (bp *BatchPublisher) publishBatch() {
	bp.mu.Lock()
	if len(bp.messages) == 0 {
		bp.timer.Reset(bp.batchInterval)
		bp.mu.Unlock()
		return
	}

	// Get batch of messages and reset
	messages := bp.messages
	bp.messages = make([]interface{}, 0, bp.batchSize)
	bp.timer.Reset(bp.batchInterval)
	bp.mu.Unlock()

	// Publish each message in the batch
	for _, msg := range messages {
		if err := bp.manager.PublishMessage(bp.topic, bp.routingKey, msg); err != nil {
			// Consider adding retries or tracking failed messages
			//TODO: Add retries.
		}
	}
}

// Close stops the timer and publishes any pending messages
func (bp *BatchPublisher) Close() {
	bp.timer.Stop()

	select {
	case <-bp.ctx.Done():
		// Context already canceled, don't publish
		return
	default:
		bp.publishBatch() // Publish any remaining messages
	}
}
