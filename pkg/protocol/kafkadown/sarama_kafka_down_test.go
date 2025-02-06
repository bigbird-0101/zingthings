package kafkadown

import (
	"context"
	"testing"
	"zingthings/pkg/util/loggerfactory"
)

func TestPattern(t *testing.T) {
	down := NewSaramaKafkaDown(loggerfactory.GetLogger(), context.Background())
	topics := down.filterTopics([]string{"zthings.a.b.down", "zthings.ab.c.down"})
	if len(topics) != 1 {
		t.Error("filter topics error")
	}
}
