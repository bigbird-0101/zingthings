package core

type (
	SimplePredicateChannelHandler struct {
		ProtocolType  ProtocolType
		PredicateDown func(message *Message) bool
		PredicateUp   func(message *Message) bool
		DealDown      func(context ChannelHandlerContext, message *Message)
		DealUp        func(context ChannelHandlerContext, message *Message)
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
