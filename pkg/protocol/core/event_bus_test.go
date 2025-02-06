package core

import (
	"fmt"
	"sync"
	"testing"
)

func TestEventBus_Subscribe(t *testing.T) {
	type fields struct {
		lock        sync.RWMutex
		subscribers map[string][]func(event Event)
	}
	type args struct {
		topic string
		f     func(event Event)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "test",
			fields: fields{
				lock:        sync.RWMutex{},
				subscribers: make(map[string][]func(event Event))},
			args: args{topic: "test", f: func(event Event) {
				fmt.Println("test", event)
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &EventBus{
				lock:        tt.fields.lock,
				subscribers: tt.fields.subscribers,
			}
			e.Subscribe(tt.args.topic, tt.args.f)
			e.Publish(tt.args.topic, "aaaa")
		})
	}
}
