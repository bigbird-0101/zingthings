package channelhandler

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
	"zingthings/pkg/common"
	"zingthings/pkg/protocol/core"
)

type (
	KafkaUpSaramaChannelHandler struct {
		*core.SimplePredicateChannelHandler
		producer sarama.SyncProducer
		logger   *zap.Logger
	}
)

func NewKafkaUpSaramaChannelHandler(logger *zap.Logger) *KafkaUpSaramaChannelHandler {
	named := logger.Named("KafkaUpSaramaChannelHandler")
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.RequiredAcks = sarama.WaitForLocal
	producerConfig.Producer.Retry.Max = 10
	producerConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"10.82.14.72:9092"}, producerConfig)
	if err != nil {
		named.Fatal("sarama.NewSyncProducer err", zap.Error(err))
	}
	var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	c := make(chan os.Signal, 1)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		producer.Close()
	}()
	return &KafkaUpSaramaChannelHandler{
		logger:   named,
		producer: producer,
		SimplePredicateChannelHandler: core.NewAsyncSimpleUpChannelHandler(
			func(handlerContext core.ChannelHandlerContext, message *core.Message) {
				logger.Debug("start send kafka message ")
				topic := string("zthings." + message.Header.DeviceGroupId)
				data := message.Data
				var responseDataChannel chan *core.Message
				if responseData, ok := data.(*core.ResponseData); ok {
					data = responseData.Data
					responseDataChannel = responseData.Response
				}
				if temp, ok := data.(string); ok {
					bytes := []byte(temp)
					if json.Valid(bytes) {
						data = bytes
					}
				}
				sendMessage, i, err := producer.SendMessage(&sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.ByteEncoder(data.([]byte)),
				})
				logger.Debug("send kafka message ", zap.Int64("i", i), zap.Int32("sendMessage", sendMessage))
				if err != nil {
					core.DefaultEventBus.Publish(common.DeviceReportFailed, message)
					logger.Error("KafkaUpChannelHandler write up message error", zap.Error(err))
					return
				}
				if "" == message.Header.DeviceId {
					message.Header.DeviceId = "testDeviceId"
				}
				if nil != responseDataChannel {
					responseDataChannel <- message
				}
				core.DefaultEventBus.Publish(common.DeviceReportSuccess, message)
				logger.Debug("end send kafka message ")
			}),
	}
}
