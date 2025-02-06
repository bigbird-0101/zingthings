package channelhandler

import (
	"zingthings/pkg/common"
	"zingthings/pkg/protocol/core"
)

type DeviceOnlineChannelHandler struct {
	*core.SimplePredicateChannelHandler
}

func NewDeviceOnlineChannelHandler() *DeviceOnlineChannelHandler {
	return &DeviceOnlineChannelHandler{
		SimplePredicateChannelHandler: core.NewAsyncSimpleUpChannelHandler(func(context core.ChannelHandlerContext, message *core.Message) {
			if "" != message.Header.ProtocolType && core.IsNotLongConnection(message.Header.ProtocolType) {
				core.DefaultEventBus.Publish(common.DeviceOnline, message)
			}
		}),
	}
}
