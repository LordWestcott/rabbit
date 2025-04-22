package rabbit

import (
	"sync"
	"time"
)

// CircuitBreaker implements the circuit breaker pattern to prevent overwhelming
// the RabbitMQ service when it's experiencing issues
type CircuitBreaker struct {
	state            int // 0=closed, 1=open, 2=half-open
	failureCount     int
	failureThreshold int
	resetTimeout     time.Duration
	lastFailure      time.Time
	mutex            sync.RWMutex
}

const (
	StateClosed = iota
	StateOpen
	StateHalfOpen
)

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: threshold,
		resetTimeout:     timeout,
	}
}

// AllowRequest checks if a request should be allowed based on the circuit state
func (cb *CircuitBreaker) AllowRequest() bool {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	if cb.state == StateClosed {
		return true
	}

	if cb.state == StateOpen {
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			// Transition to half-open state
			cb.mutex.RUnlock()
			cb.mutex.Lock()
			cb.state = StateHalfOpen
			cb.mutex.Unlock()
			cb.mutex.RLock()
			return true
		}
		return false
	}

	// Half-open state - allow one request
	return true
}

// RecordSuccess records a successful request and potentially closes the circuit
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if cb.state == StateHalfOpen {
		cb.state = StateClosed
		cb.failureCount = 0
	}
}

// RecordFailure records a failed request and potentially opens the circuit
func (cb *CircuitBreaker) RecordFailure() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.failureCount++
	cb.lastFailure = time.Now()

	if cb.state == StateHalfOpen || cb.failureCount >= cb.failureThreshold {
		cb.state = StateOpen
	}
}
