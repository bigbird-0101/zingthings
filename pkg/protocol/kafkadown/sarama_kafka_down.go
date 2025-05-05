package kafkadown

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"zingthings/pkg/common"
	"zingthings/pkg/protocol/config"
	"zingthings/pkg/protocol/core"
)

type (
	SaramaKafkaDown struct {
		consumer            *Consumer
		topicsPatternRegexp *regexp.Regexp
		configCore          *config.Config
		logger              *zap.Logger
		context             context.Context
	}
	Consumer struct {
		*core.DownLink
		ready         chan bool
		logger        *zap.Logger
		consumerGroup sarama.ConsumerGroup
	}
	DownMessage struct {
		DeviceId           core.DeviceId          `json:"deviceId"`
		DownLinkType       string                 `json:"downLinkType,omitempty"`
		DownLinkIdentifier string                 `json:"downLinkIdentifier,omitempty"`
		Data               map[string]interface{} `json:"data"`
	}
)

func (c Consumer) Setup(session sarama.ConsumerGroupSession) error {
	c.logger.With(
		zap.String("memberID", session.MemberID()),
		zap.Int32("generationID", session.GenerationID()),
		zap.String("claims", fmt.Sprintf("%v", session.Claims())),
	).Info("consumer group session setup")
	c.ready <- true
	return nil
}

func (c Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	c.logger.With(
		zap.String("memberID", session.MemberID()),
		zap.Int32("generationID", session.GenerationID()),
		zap.String("claims", fmt.Sprintf("%v", session.Claims())),
	).Info("consumer group session cleanup")
	return nil
}

func (c Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			if msg != nil {
				topic := msg.Topic
				deviceGroupId := strings.Split(topic, ".")[1]
				message := &DownMessage{}
				err2 := json.Unmarshal(msg.Value, message)
				if err2 != nil {
					c.logger.Error("convert json error", zap.Error(err2))
					continue
				}
				metadata := make(map[string]interface{})
				metadata["downLinkType"] = message.DownLinkType
				metadata["downLinkIdentifier"] = message.DownLinkIdentifier
				go func() {
					m := &core.Message{
						Header: core.Header{
							DeviceId:      message.DeviceId,
							DeviceGroupId: core.DeviceGroupId(deviceGroupId),
						},
						Metadata: metadata,
						Data:     message.Data,
					}
					err := c.DownLink.DownLink(m)
					if err != nil {
						c.logger.Error("downLink failed", zap.Error(err))
					}
					core.DefaultEventBus.Publish(common.DeviceDownLinkSuccess, m)
				}()
				session.MarkMessage(msg, "")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func NewSaramaKafkaDown(logger *zap.Logger, context context.Context, configCore *config.Config) *SaramaKafkaDown {
	configNew := sarama.NewConfig()
	configNew.Version = sarama.V2_8_1_0
	kafka := configCore.Kafka
	client, err := sarama.NewConsumerGroup(kafka.Broker.List,
		kafka.Consumer.Group, configNew)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}
	re, err := regexp.Compile(kafka.Consumer.TopicPattern)
	if err != nil {
		log.Panicf("Error compiling topics regex: %v", err)
	}
	return &SaramaKafkaDown{
		logger:              logger.Named("sarama_kafka_down"),
		topicsPatternRegexp: re,
		context:             context,
		configCore:          configCore,
		consumer: &Consumer{
			ready:         make(chan bool),
			consumerGroup: client,
			logger:        logger.Named("sarama_kafka_consumer_down"),
			DownLink:      core.NewDownLink(),
		},
	}
}

func (s *SaramaKafkaDown) Start() {
	keepRunning := true
	ctx, cancel := context.WithCancel(s.context)
	configNew := sarama.NewConfig()
	newClient, err := sarama.NewClient(s.configCore.Kafka.Broker.List, configNew)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Get all the Topic
	topics, err := newClient.Topics()

	topics = s.filterTopics(topics)

	go func() {
		defer wg.Done()
		for {
			if len(topics) == 0 {
				s.consumer.ready <- true
				return
			}
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err2 := s.consumer.consumerGroup.Consume(ctx, topics, s.consumer); err2 != nil {
				if errors.Is(err2, sarama.ErrClosedConsumerGroup) {
					return
				}
				s.logger.Error("Error from consumer: %v", zap.Error(err))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				s.logger.Error("Context err from consumer: %v", zap.Error(ctx.Err()))
				return
			}
		}
	}()

	<-s.consumer.ready // Await till the consumer has been set up

	log.Println("Sarama consumer up and running!...")

	go s.refreshTopics(newClient, s.consumer.consumerGroup, topics)

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, os.Interrupt)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false

		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false

		case <-sigusr1:

		}
	}

	cancel()

	wg.Wait()
	if err = s.consumer.consumerGroup.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
	close(s.consumer.ready)
}

func EqualSlices(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}

	m1 := make(map[string]struct{})
	m2 := make(map[string]struct{})

	for _, v := range s1 {
		m1[v] = struct{}{}
	}

	for _, v := range s2 {
		m2[v] = struct{}{}
	}

	return reflect.DeepEqual(m1, m2)
}

func (s *SaramaKafkaDown) filterTopics(topics []string) []string {
	filteredTopics := make([]string, 0)
	for _, topic := range topics {
		if topic != "__consumer_offsets" && s.topicsPatternRegexp.MatchString(topic) {
			filteredTopics = append(filteredTopics, topic)
		}
	}
	return filteredTopics
}

func (s *SaramaKafkaDown) refreshTopics(client sarama.Client, prevConsumerGroup sarama.ConsumerGroup, topicsOld []string) {
	ticker := time.NewTicker(5 * time.Second)

	for {
		<-ticker.C

		if err := client.RefreshMetadata(); err != nil {
			s.logger.Error("Error refreshing metadata:", zap.Error(err))
			continue
		}

		topics, err := client.Topics()
		if err != nil {
			s.logger.Error("Error refreshing topics:", zap.Error(err))
			continue
		}

		filteredTopics := s.filterTopics(topics) // filter "__consumer_offsets"
		sort.Strings(filteredTopics)
		s.logger.Debug("All Topics ", zap.Any("Topics", filteredTopics))

		if !EqualSlices(filteredTopics, topicsOld) {
			topicsOld = filteredTopics

			if prevConsumerGroup != nil {
				err := prevConsumerGroup.Close()
				if err != nil {
					s.logger.Error("Error closing prev consumer group:", zap.Error(err))
				}
			}

			newConsumer := &Consumer{
				ready:    make(chan bool),
				DownLink: core.NewDownLink(),
				logger:   s.logger.Named("kafka_sarama_new_consumer"),
			}

			newConsumerGroup, err := sarama.NewConsumerGroupFromClient(s.configCore.Kafka.Consumer.Group, client)
			if err != nil {
				s.logger.Error("Error creating new consumer group: %v", zap.Error(err))
				return
			}

			defer func(newConsumerGroup sarama.ConsumerGroup) {
				err := newConsumerGroup.Close()
				if err != nil {
					s.logger.Error("Error closing new consumer group: %v", zap.Error(err))
				}
			}(newConsumerGroup)
			consumerGroupOld := s.consumer.consumerGroup
			newConsumer.consumerGroup = newConsumerGroup
			oldConsumer := s.consumer
			close(oldConsumer.ready)
			if consumerGroupOld != nil {
				_ = consumerGroupOld.Close()
			}
			s.consumer = newConsumer
			go func() {
				ctx, cancel := context.WithCancel(s.context)
				defer cancel()

				wg := &sync.WaitGroup{}
				wg.Add(1)

				// start Consume
				go func() {
					defer wg.Done()
					if err2 := newConsumerGroup.Consume(ctx, filteredTopics, newConsumer); err2 != nil {
						s.logger.Error("Error from consumer:", zap.Error(err2))
					}
				}()
				<-s.consumer.ready
				wg.Wait()
			}()
		}
	}
}
