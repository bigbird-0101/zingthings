package core

import (
	"context"
	"go.uber.org/zap"
	"sync"
	"zingthings/pkg/protocol/config"
)

type (
	SimplePredicateChannelHandler struct {
		ProtocolType  ProtocolType
		PredicateDown func(message *Message) bool
		PredicateUp   func(message *Message) bool
		DealDown      func(context ChannelHandlerContext, message *Message)
		DealUp        func(context ChannelHandlerContext, message *Message)
	}
	ChannelHandlerRegisterMap struct {
		channelHandlerRegisterMap map[string]*ChannelHandlerRegister
		lock                      sync.RWMutex
	}
)

var (
	ChannelHandlerRegisterCore = &ChannelHandlerRegisterMap{
		channelHandlerRegisterMap: make(map[string]*ChannelHandlerRegister),
		lock:                      sync.RWMutex{},
	}
)

func NewSimpleAllChannelHandler(dealDown func(context ChannelHandlerContext, message *Message),
	dealUp func(context ChannelHandlerContext, message *Message)) *SimplePredicateChannelHandler {
	return &SimplePredicateChannelHandler{
		PredicateDown: func(message *Message) bool {
			return true
		},
		PredicateUp: func(message *Message) bool {
			return true
		},
		DealDown: dealDown,
		DealUp:   dealUp,
	}
}

func NewAsyncSimpleAllChannelHandler(dealDown func(context ChannelHandlerContext, message *Message),
	dealUp func(context ChannelHandlerContext, message *Message)) *SimplePredicateChannelHandler {
	return NewSimpleAllChannelHandler(func(context ChannelHandlerContext, message *Message) {
		go dealDown(context, message)
	}, func(context ChannelHandlerContext, message *Message) {
		go dealUp(context, message)
	})
}

func NewAsyncSimpleUpChannelHandler(dealUp func(context ChannelHandlerContext, message *Message)) *SimplePredicateChannelHandler {
	return &SimplePredicateChannelHandler{
		PredicateDown: func(message *Message) bool {
			return false
		},
		PredicateUp: func(message *Message) bool {
			return true
		},
		DealUp: func(context ChannelHandlerContext, message *Message) {
			go dealUp(context, message)
		},
	}
}

func NewAsyncSimpleUpProtocolTypeChannelHandler(protocolType ProtocolType, dealUp func(context ChannelHandlerContext, message *Message)) *SimplePredicateChannelHandler {
	return NewSimpleUpProtocolTypeChannelHandler(protocolType, func(context ChannelHandlerContext, message *Message) {
		go dealUp(context, message)
	})
}

func NewSimpleUpProtocolTypeChannelHandler(protocolType ProtocolType, dealUp func(context ChannelHandlerContext, message *Message)) *SimplePredicateChannelHandler {
	return &SimplePredicateChannelHandler{
		ProtocolType: protocolType,
		PredicateDown: func(message *Message) bool {
			return false
		},
		PredicateUp: func(message *Message) bool {
			if message.Header.ProtocolType != "" && protocolType == message.Header.ProtocolType {
				return true
			}
			return false
		},
		DealUp: dealUp,
	}
}

func NewSimpleUpChannelHandler(dealUp func(context ChannelHandlerContext, message *Message)) *SimplePredicateChannelHandler {
	return &SimplePredicateChannelHandler{
		PredicateDown: func(message *Message) bool {
			return false
		},
		PredicateUp: func(message *Message) bool {
			return true
		},
		DealUp: dealUp,
	}
}

func NewAsyncSimpleDownProtocolTypeChannelHandler(protocolType ProtocolType, dealDown func(context ChannelHandlerContext, message *Message)) *SimplePredicateChannelHandler {
	return NewSimpleDownProtocolTypeChannelHandler(protocolType, func(context ChannelHandlerContext, message *Message) {
		go dealDown(context, message)
	})
}

func NewSimpleDownProtocolTypeChannelHandler(protocolType ProtocolType, dealDown func(context ChannelHandlerContext, message *Message)) *SimplePredicateChannelHandler {
	return &SimplePredicateChannelHandler{
		ProtocolType: protocolType,
		PredicateDown: func(message *Message) bool {
			if message.Header.ProtocolType != "" && protocolType == message.Header.ProtocolType {
				return true
			}
			return false
		},
		PredicateUp: func(message *Message) bool {
			return false
		},
		DealDown: dealDown,
	}
}

func NewSimpleDownChannelHandler(dealDown func(context ChannelHandlerContext, message *Message)) *SimplePredicateChannelHandler {
	return &SimplePredicateChannelHandler{
		PredicateDown: func(message *Message) bool {
			return true
		},
		PredicateUp: func(message *Message) bool {
			return false
		},
		DealDown: dealDown,
	}
}

func (s *SimplePredicateChannelHandler) OnChannelDownStream(context ChannelHandlerContext, data interface{}) {
	message, ok := data.(*Message)
	if ok {
		if nil != s.PredicateDown && !s.PredicateDown(message) {
			context.SendDownStream(data)
			return
		}
		if nil == s.DealDown {
			return
		}
		s.DealDown(context, message)
	}
	context.SendDownStream(data)
}

func (s *SimplePredicateChannelHandler) OnChannelUpStream(context ChannelHandlerContext, data interface{}) {
	message, ok := data.(*Message)
	if ok {
		if nil != s.PredicateUp && !s.PredicateUp(message) {
			context.SendUpStream(data)
			return
		}
		if nil == s.DealUp {
			return
		}
		s.DealUp(context, message)
	}
	context.SendUpStream(data)
}

func RegisterChannelHandler(channelHandlerRegister *ChannelHandlerRegister) {
	ChannelHandlerRegisterCore.lock.Lock()
	defer ChannelHandlerRegisterCore.lock.Unlock()
	ChannelHandlerRegisterCore.channelHandlerRegisterMap[channelHandlerRegister.ChannelHandlerType] = channelHandlerRegister
}

func GetRegisterChannelHandler() []*ChannelHandlerRegister {
	ChannelHandlerRegisterCore.lock.RLock()
	defer ChannelHandlerRegisterCore.lock.RUnlock()
	registerMap := ChannelHandlerRegisterCore.channelHandlerRegisterMap
	registers := make([]*ChannelHandlerRegister, 0, len(ChannelHandlerRegisterCore.channelHandlerRegisterMap))
	for _, v := range registerMap {
		registers = append(registers, v)
	}
	return registers
}

func InitChannelHandler(ctx context.Context, logger *zap.Logger, config *config.Config) {
	handlers := GetRegisterChannelHandler()
	for _, handler := range handlers {
		err := handler.Setup(&SetupContext{
			Logger:  logger,
			Context: ctx,
			Config:  config,
		})
		if err != nil {
			logger.Error("add common channel handler fail", zap.Error(err))
			return
		}
	}
}
