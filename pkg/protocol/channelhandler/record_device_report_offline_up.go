package channelhandler

import (
	"context"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"strconv"
	"sync"
	"time"
	"zingthings/pkg/common"
	"zingthings/pkg/protocol/core"
)

type (
	Record struct {
		DeviceId  core.DeviceId `json:"device_id"`
		Timestamp int64         `json:"timestamp"`
	}

	RecordChannelHandler struct {
		*core.SimplePredicateChannelHandler
		logger       *zap.Logger
		reportRecord ReportRecord
	}
	ReportRecord interface {
		Start()
		SaveRecord(record *Record)
	}
	LocalMemoryReportRecord struct {
		pool   *sync.Map
		limit  int
		logger *zap.Logger
		ticker *time.Ticker
	}
	RedisReportRecord struct {
		redisClient *redis.Client
		logger      *zap.Logger
		ctx         context.Context
		key         string
		ticker      *time.Ticker
	}
)

func init() {
	core.RegisterChannelHandler(&core.ChannelHandlerRegister{
		ChannelHandlerType: "recordDeviceReportOffline",
		Setup: func(setupContext *core.SetupContext) error {
			err := core.ChannelHandlerPipelineCommon.AddLast("recordDeviceReportOffline",
				NewRecordDeviceReportOfflineChannelHandler(setupContext.Context, setupContext.Logger))
			return err
		},
	})
}

func NewRecordDeviceReportOfflineChannelHandler(ctx context.Context, logger *zap.Logger) *RecordChannelHandler {
	reportRecord := getReportRecord(ctx, logger)
	reportRecord.Start()
	return &RecordChannelHandler{
		logger:       logger.Named("RecordDeviceReportOfflineChannelHandler"),
		reportRecord: reportRecord,
		SimplePredicateChannelHandler: core.NewAsyncSimpleUpChannelHandler(func(context core.ChannelHandlerContext, message *core.Message) {
			if "" != message.Header.ProtocolType && core.IsNotLongConnection(message.Header.ProtocolType) {
				reportRecord.SaveRecord(&Record{DeviceId: message.Header.DeviceId, Timestamp: time.Now().UnixMicro()})
			}
		}),
	}
}

func getReportRecord(ctx context.Context, logger *zap.Logger) ReportRecord {
	//return &LocalMemoryReportRecord{
	//	pool:   &sync.Map{},
	//	limit:  20000,
	//	logger: logger.Named("RecordDeviceReportOfflineChannelHandler_LocalMemoryReportRecord"),
	//  ticker: time.NewTicker(time.Duration(5) * time.Second),
	//}
	return &RedisReportRecord{
		redisClient: redis.NewClient(&redis.Options{Addr: "10.82.27.124:6379", Password: ""}),
		logger:      logger.Named("RecordDeviceReportOfflineChannelHandler_RedisReportRecord"),
		ctx:         ctx,
		key:         "ReportRecordDeviceReportOffline",
		ticker:      time.NewTicker(time.Duration(5) * time.Second),
	}
}

func (l *LocalMemoryReportRecord) Start() {
	go func() {
		for {
			<-l.ticker.C
			current := time.Now().UnixMilli()
			l.pool.Range(func(key, value interface{}) bool {
				timeStamp := value.(int64)
				if current-timeStamp > l.getDeviceOfflineMillisecond() {
					core.DefaultEventBus.Publish(common.DeviceOffline, key)
					l.pool.Delete(key)
				}
				return true
			})
		}
	}()
}

func (l *LocalMemoryReportRecord) size() int {
	count := 0
	l.pool.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

func (l *LocalMemoryReportRecord) getDeviceOfflineMillisecond() int64 {
	return 5 * 60 * 1000
}

func (l *LocalMemoryReportRecord) SaveRecord(record *Record) {
	if l.size() >= l.limit {
		l.logger.Warn("report record limit ", zap.Int("limit", l.limit))
		return
	}
	l.pool.Store(record.DeviceId, record.Timestamp)
}

func (r *RedisReportRecord) getDeviceOfflineMillisecond() int64 {
	return 5 * 60 * 1000
}

func (r *RedisReportRecord) Start() {
	go func() {
		for {
			<-r.ticker.C
			var cursor uint64 = 0
			var err error
			for {
				current := time.Now().UnixMilli()
				var result []string
				result, cursor, err = r.redisClient.HScan(r.ctx, r.key, cursor, "", 0).Result()
				if err != nil {
					r.logger.Error("hscan error", zap.Error(err))
					return
				}

				for _, field := range result {
					s, err := r.redisClient.HGet(r.ctx, r.key, field).Result()
					if err != nil {
						r.logger.Error("hget error", zap.String("field", field), zap.Error(err))
						continue
					}
					timeStamp, err := strconv.Atoi(s)
					if err != nil {
						r.logger.Error("convert to int", zap.String("field", field), zap.Error(err))
						continue
					}
					if current-int64(timeStamp) > r.getDeviceOfflineMillisecond() {
						core.DefaultEventBus.Publish(common.DeviceOffline, field)
						r.redisClient.HDel(r.ctx, r.key, field)
					}
				}
				if cursor == 0 {
					break
				}
			}
		}
	}()
}

func (r *RedisReportRecord) SaveRecord(record *Record) {
	_, err := r.redisClient.HSet(r.ctx, r.key, string(record.DeviceId), record.Timestamp).Result()
	if err != nil {
		r.logger.Error("hset error", zap.Error(err))
	}
}
