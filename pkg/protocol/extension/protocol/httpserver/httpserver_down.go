package httpserver

import (
	"go.uber.org/zap"
	"zingthings/pkg/protocol/core"
)

type Down struct {
	*ProtocolImpl
	*core.SimplePredicateChannelHandler
}

func NewHttpServerDownChannelHandler(protocol *ProtocolImpl) *Down {
	return &Down{
		ProtocolImpl: protocol,
		SimplePredicateChannelHandler: core.NewAsyncSimpleDownProtocolTypeChannelHandler(core.HttpServer, func(context core.ChannelHandlerContext, message *core.Message) {
			protocol.logger.Info("http server down link message", zap.Any("message", message))
		}),
	}
}
