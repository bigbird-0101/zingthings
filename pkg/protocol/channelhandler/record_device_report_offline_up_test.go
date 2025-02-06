package channelhandler

import (
	"context"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"sync"
	"testing"
	"time"
	"zingthings/pkg/util/loggerfactory"
)

func TestLocalMemoryReportRecord_Start(t *testing.T) {
	type fields struct {
		pool   *sync.Map
		limit  int
		logger *zap.Logger
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{"test", fields{pool: &sync.Map{}, limit: 100, logger: loggerfactory.GetLogger()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LocalMemoryReportRecord{
				pool:   tt.fields.pool,
				limit:  tt.fields.limit,
				logger: tt.fields.logger,
				ticker: time.NewTicker(time.Duration(1) * time.Second),
			}
			l.SaveRecord(&Record{
				DeviceId:  "testDeviceId",
				Timestamp: time.Now().UnixMilli() - 5*60*1000 - 1,
			})
			l.Start()
			for l.size() != 0 {
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestRedisReportRecord_Start(t *testing.T) {
	type fields struct {
		redisClient *redis.Client
		logger      *zap.Logger
		ctx         context.Context
		key         string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{"test", fields{redisClient: redis.NewClient(&redis.Options{Addr: "10.82.14.78:6379", Password: "123456"}),
			logger: loggerfactory.GetLogger(), ctx: context.Background(), key: "test"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisReportRecord{
				redisClient: tt.fields.redisClient,
				logger:      tt.fields.logger,
				ctx:         tt.fields.ctx,
				key:         tt.fields.key,
				ticker:      time.NewTicker(time.Duration(1) * time.Second),
			}
			r.Start()
			r.SaveRecord(&Record{
				DeviceId:  "testDeviceId",
				Timestamp: time.Now().UnixMilli() - 5*60*1000 - 1,
			})
			var a = 0
			for {
				value, _ := r.redisClient.HLen(tt.fields.ctx, "test").Result()
				if value != 0 {
					time.Sleep(time.Millisecond)
					a++
				} else {
					if a == 0 {
						t.Error("redis hlen fail")
					}
					break
				}
			}
		})
	}
}
