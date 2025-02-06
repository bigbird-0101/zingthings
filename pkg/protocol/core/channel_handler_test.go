package core

import "testing"

func TestChannelHandler(t *testing.T) {
	a := &SimplePredicateChannelHandler{}
	if a == nil {
		t.Error()
	}
}
