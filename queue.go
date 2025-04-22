package rabbit

import "github.com/streadway/amqp"

// DeclareQueueWithConfig creates a queue with the specified configuration
func (p *PubSubManager) DeclareQueueWithConfig(config QueueConfig) (string, error) {
	p.channelMutex[0].Lock()
	defer p.channelMutex[0].Unlock()

	args := amqp.Table{}

	if config.TTL > 0 {
		args["x-message-ttl"] = config.TTL
	}

	if config.MaxLength > 0 {
		args["x-max-length"] = config.MaxLength
	}

	if config.MaxLengthBytes > 0 {
		args["x-max-length-bytes"] = config.MaxLengthBytes
	}

	if config.DeadLetterExchange != "" {
		args["x-dead-letter-exchange"] = config.DeadLetterExchange
		if config.DeadLetterRoutingKey != "" {
			args["x-dead-letter-routing-key"] = config.DeadLetterRoutingKey
		}
	}

	q, err := p.channelPool[0].QueueDeclare(
		config.Name,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		false, // no-wait
		args,
	)

	if err != nil {
		return "", err
	}

	return q.Name, nil
}

// DeclareQueue is a simplified method to declare a durable queue
func (p *PubSubManager) DeclareQueue(name string, durable bool) (string, error) {
	return p.DeclareQueueWithConfig(QueueConfig{
		Name:       name,
		Durable:    durable,
		AutoDelete: false,
		Exclusive:  false,
	})
}

// BindQueue binds a queue to an exchange with a routing key
func (p *PubSubManager) BindQueue(queueName, routingKey, exchangeName string) error {
	p.channelMutex[0].Lock()
	defer p.channelMutex[0].Unlock()

	return p.channelPool[0].QueueBind(
		queueName,    // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
}

// PurgeQueue removes all messages from a queue
func (p *PubSubManager) PurgeQueue(queueName string) (int, error) {
	p.channelMutex[0].Lock()
	defer p.channelMutex[0].Unlock()

	return p.channelPool[0].QueuePurge(
		queueName, // queue name
		false,     // no-wait
	)
}

// DeleteQueue removes a queue and its contents
func (p *PubSubManager) DeleteQueue(queueName string) (int, error) {
	p.channelMutex[0].Lock()
	defer p.channelMutex[0].Unlock()

	return p.channelPool[0].QueueDelete(
		queueName, // queue name
		false,     // if unused
		false,     // if empty
		false,     // no-wait
	)
}
