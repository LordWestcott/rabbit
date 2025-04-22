package rabbit

import (
	"time"

	"github.com/streadway/amqp"
)

// HealthCheck returns the current health status of the PubSubManager
func (p *PubSubManager) HealthCheck() HealthStatus {
	p.connectionMutex.Lock()
	defer p.connectionMutex.Unlock()

	status := HealthStatus{
		Connected: p.conn != nil,
	}

	p.listenersMutex.Lock()
	status.ActiveListeners = len(p.listeners)
	p.listenersMutex.Unlock()

	// Add circuit breaker state
	switch p.circuitBreaker.state {
	case StateClosed:
		status.CircuitState = "closed"
	case StateOpen:
		status.CircuitState = "open"
	case StateHalfOpen:
		status.CircuitState = "half-open"
	}

	return status
}

// monitorConnection checks the connection status and attempts to reconnect if needed
func (p *PubSubManager) monitorConnection(url string) {
	closeChan := make(chan *amqp.Error)
	p.conn.NotifyClose(closeChan)

	for {
		select {
		case <-p.ctx.Done():
			return

		case _, ok := <-closeChan:
			if !ok {
				return
			}

			// Prevent multiple reconnection attempts
			p.connectionMutex.Lock()
			if p.reconnecting {
				p.connectionMutex.Unlock()
				continue
			}
			p.reconnecting = true
			p.connectionMutex.Unlock()

			// Attempt to reconnect with backoff
			reconnectBackoff := p.reconnectBackoff
			for attempt := 1; attempt <= p.maxReconnectAttempts; attempt++ {

				conn, err := amqp.Dial(url)
				if err == nil {

					// Rebuild everything with the new connection
					p.rebuildAfterReconnect(conn)

					// Reset for next disconnection
					p.connectionMutex.Lock()
					p.reconnecting = false
					p.connectionMutex.Unlock()

					// Update notification channel
					closeChan = make(chan *amqp.Error)
					p.conn.NotifyClose(closeChan)

					break
				}
				time.Sleep(reconnectBackoff)

				// Increase backoff for next attempt, up to a reasonable maximum
				if reconnectBackoff < 30*time.Second {
					reconnectBackoff *= 2
				}
			}

			// If we've exhausted all attempts
			p.connectionMutex.Lock()
			if p.reconnecting {
				p.reconnecting = false
			}
			p.connectionMutex.Unlock()
		}
	}
}
