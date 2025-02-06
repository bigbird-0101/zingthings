package core

import "sync"

var (
	DefaultEventBus = NewEventBus()
)

type (
	Event    interface{}
	EventBus struct {
		lock        sync.RWMutex
		subscribers map[string][]func(event Event)
	}
)

func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[string][]func(event Event)),
		lock:        sync.RWMutex{},
	}
}

func (e *EventBus) Subscribe(eventType string, f func(event Event)) {
	e.lock.Lock()
	defer e.lock.Unlock()

	// 确保 eventType 存在
	if _, exists := e.subscribers[eventType]; !exists {
		e.subscribers[eventType] = []func(event Event){}
	}

	e.subscribers[eventType] = append(e.subscribers[eventType], f)
}

func (e *EventBus) Publish(topicType string, event Event) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	for _, subscriber := range e.subscribers[topicType] {
		subscriber(event)
	}
}
