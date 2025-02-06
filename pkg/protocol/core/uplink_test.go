package core

import "testing"

func TestNewUpMessage(t *testing.T) {
	message := NewUpMessage(Header{}, 123)
	if message == nil {
		t.Error("message is nil")
	}
}
