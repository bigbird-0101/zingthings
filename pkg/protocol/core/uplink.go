package core

import (
	"go.uber.org/zap"
	"zingthings/pkg/util/loggerfactory"
)

type (
	UpLink struct {
		ChannelManager ChannelManager
		logger         *zap.Logger
	}
)

func NewUpMessage(header Header, data interface{}) *Message {
	return &Message{
		Header: header,
		Data:   data,
	}
}

func NewUpLink() *UpLink {
	return &UpLink{
		ChannelManager: DefaultChannelManagerCommon,
		logger:         loggerfactory.GetLogger(),
	}
}

func (up *UpLink) UpLink(message *Message) error {
	channel := up.ChannelManager.Get(string(message.Header.DeviceId))
	if channel == nil {
		ChannelHandlerPipelineCommon.SendUpStream(message)
		return nil
	}
	up.logger.Info("start SendUpStream ")
	channel.SendUpStream(message)
	up.logger.Info("end SendUpStream ")
	return nil
}
