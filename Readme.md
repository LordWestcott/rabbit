# RabbitMQ PubSub Package

## Overview

This package provides a robust, feature-rich wrapper around the RabbitMQ AMQP client for Go. It offers connection management, automatic reconnection, publisher confirms, batch publishing, circuit breaking and more. The package is designed to be used in production microservice environments where reliability and resilience are critical.

## Installation

To use this package, import it in your Go code:

~~~go
import "product-service/pkg/pubsub"
~~~

Ensure that the RabbitMQ client is also installed:

~~~bash
go get github.com/streadway/amqp
~~~

## Basic Usage

### Initializing the PubSub Manager

~~~go
import (
    "product-service/pkg/logs"
    "product-service/pkg/pubsub"
)

// Create a logger
logger := logs.NewServiceLogger("my-service")

// Create a PubSub manager with a channel pool of size 3
pubsubManager := pubsub.NewPubSubManager(logger, 3)

// Start the manager with the RabbitMQ connection URL
err := pubsubManager.Start("amqp://guest:guest@localhost:5672/")
if err != nil {
    logger.Fatalf("Failed to connect to RabbitMQ: %v", err)
}

// Gracefully close the connection when done
defer pubsubManager.Close()
~~~

### Publishing Messages

~~~go
// Define a message payload struct
type OrderCreated struct {
    OrderID    string `json:"order_id"`
    CustomerID string `json:"customer_id"`
    Amount     float64 `json:"amount"`
    Timestamp  int64 `json:"timestamp"`
}

// Create a message
order := OrderCreated{
    OrderID:    "12345",
    CustomerID: "customer-123",
    Amount:     99.99,
    Timestamp:  time.Now().Unix(),
}

// Publish the message
err := pubsubManager.PublishMessage("orders", "order.created", order)
if err != nil {
    logger.Errorf("Failed to publish message: %v", err)
}
~~~

### Consuming Messages

~~~go
// Define a message handler function - a returned error will NAck the Message, and it will be requeued.
func handleOrderCreated(payload []byte) error {
    var order OrderCreated
    if err := json.Unmarshal(payload, &order); err != nil {
        return err
    }
    
    fmt.Printf("Received order: %s for $%.2f\n", order.OrderID, order.Amount)
    return nil
}

// Start listening for messages
listenerId, err := pubsubManager.Listen(
    "orders",          // topic/exchange name
    "order.created.*", // routing key pattern
    "order-service",   // queue name
    handleOrderCreated, // handler function
)

if err != nil {
    logger.Errorf("Failed to start listener: %v", err)
}
~~~

## Configuration Options

### Advanced PubSubManager Setup

~~~go
// Create a PubSubManager with custom options
opts := pubsub.PubSubManagerOptions{
    MaxReconnectAttempts: 20,              // Maximum reconnection attempts
    ReconnectBackoff:     time.Second * 5, // Initial backoff duration
    PublishConfirms:      true,            // Enable publisher confirms
}

pubsubManager := pubsub.NewPubSubManagerWithOptions(logger, 5, opts)
~~~

### Queue Configuration

~~~go
// Define queue configuration
queueConfig := pubsub.QueueConfig{
    Name:                 "high-priority-orders",
    Durable:              true,
    AutoDelete:           false,
    Exclusive:            false,
    TTL:                  60000,                // 60 seconds message TTL
    MaxLength:            1000,                 // Max 1000 messages
    DeadLetterExchange:   "orders.dead-letter",
    DeadLetterRoutingKey: "order.expired",
}

// Declare the queue
queueName, err := pubsubManager.DeclareQueueWithConfig(queueConfig)
if err != nil {
    logger.Errorf("Failed to declare queue: %v", err)
}

// Bind queue to exchange
err = pubsubManager.BindQueue(queueName, "order.priority.high", "orders")
if err != nil {
    logger.Errorf("Failed to bind queue: %v", err)
}
~~~

## Advanced Features

### Batch Publishing

For high-throughput scenarios where you need to publish many messages efficiently:

~~~go
// Create a batch publisher with batch size 100 and maximum interval 5 seconds
batchPublisher := pubsub.NewBatchPublisher(
    pubsubManager,
    "metrics", 
    "system.cpu", 
    100,             // batch size
    5*time.Second,   // flush interval
)

// Add messages to the batch
for i := 0; i < 1000; i++ {
    metric := MetricData{
        Name:  "cpu_usage",
        Value: rand.Float64() * 100,
        Time:  time.Now().Unix(),
    }
    batchPublisher.Add(metric)
}

// Close the batch publisher to ensure all messages are sent
batchPublisher.Close()
~~~

### Message TTL and Priority

~~~go
// Publish a message with 30 second TTL
pubsubManager.PublishMessageWithTTL(
    "notifications", 
    "user.login", 
    loginEvent, 
    30000, // TTL in milliseconds
)

// Publish a high-priority message
pubsubManager.PublishWithPriority(
    "orders", 
    "order.urgent", 
    urgentOrder, 
    9, // Priority (0-9, higher is more prioritized)
)
~~~

### Setting QoS (Prefetch)

~~~go
// Set prefetch to 10 messages per consumer
err := pubsubManager.SetQoS(10)
if err != nil {
    logger.Errorf("Failed to set QoS: %v", err)
}
~~~

## Resilience Features

### Health Checking

~~~go
// Get current health status
status := pubsubManager.HealthCheck()

if !status.Connected {
    logger.Warn("Not connected to RabbitMQ broker")
}

if status.CircuitState == "open" {
    logger.Warn("Circuit breaker is open, requests being rejected")
}

fmt.Printf("Active listeners: %d\n", status.ActiveListeners)
~~~

### The Circuit Breaker Pattern

The package implements a circuit breaker that automatically prevents overwhelming the RabbitMQ server during failures. This helps your application gracefully handle RabbitMQ outages without cascading failures.

When too many message publishing attempts fail consecutively, the circuit will "open" and return errors immediately without attempting to publish. After a timeout period, the circuit transitions to "half-open" state allowing a test request. If successful, normal operation resumes.

## Architecture

The package is split into several logical components:

1. **Connection Management** - Handles connection to RabbitMQ, reconnection, and channel pooling
2. **Publishers** - Message publishing with configurable options
3. **Consumers** - Message consumption and handler management
4. **Circuit Breaker** - Failure detection and service protection
5. **Batch Publishing** - Efficiently handle high-volume publishing
6. **Queue Management** - Queue declaration and binding

## Error Handling

The package provides comprehensive error handling. Most functions return errors that should be checked. Additionally, failed message publishing is logged, and the circuit breaker protects against cascading failures.

~~~go
// Example of proper error handling
err := pubsubManager.PublishMessage("orders", "order.created", order)
if err != nil {
    if err.Error() == "circuit breaker open" {
        // Handle service degradation gracefully
        logger.Warn("Message rejected by circuit breaker, system in degraded state")
        
        // Store message for later retry or use fallback mechanism
        saveForRetry(order)
    } else {
        logger.Errorf("Failed to publish message: %v", err)
    }
}
~~~

## Automatic Recovery

The package automatically handles:

1. Connection failures with exponential backoff reconnection
2. Channel failures with automatic recreation
3. Consumer recovery after reconnection
4. Exchange redeclaration after reconnection

These features ensure your application remains operational even during network issues or RabbitMQ server restarts.

## Best Practices

1. Always check errors returned by the package methods
2. Properly close the PubSubManager when shutting down your application
3. Use batch publishing for high-volume scenarios
4. Implement appropriate error handling, especially for circuit breaker events
5. Configure reconnection parameters based on your environment's reliability
6. Use message TTLs for time-sensitive information

