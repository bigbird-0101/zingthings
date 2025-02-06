package core

import (
	"errors"
	"sync"
)

var DefaultPipelineFactoryCommon = &DefaultPipelineFactory{}

type (
	DefaultChannelHandlerContext struct {
		next                *DefaultChannelHandlerContext
		prev                *DefaultChannelHandlerContext
		name                string
		handler             ChannelHandler
		canHandleDownStream bool
		canHandleUpStream   bool
	}
	DefaultChannelHandlerPipeline struct {
		head       *DefaultChannelHandlerContext
		tail       *DefaultChannelHandlerContext
		lock       sync.RWMutex
		channelMap map[string]*DefaultChannelHandlerContext
	}
	DefaultPipelineFactory struct {
	}
)

func (d DefaultPipelineFactory) Create() ChannelHandlerPipeline {
	pipeline := NewDefaultChannelHandlerPipeline()
	return pipeline
}

func NewDefaultChannelHandlerPipeline() *DefaultChannelHandlerPipeline {
	return &DefaultChannelHandlerPipeline{
		channelMap: map[string]*DefaultChannelHandlerContext{},
	}
}

func (p *DefaultChannelHandlerContext) Name() string {
	return p.name
}

func (p *DefaultChannelHandlerContext) Handler() ChannelHandler {
	return p.handler
}

func (p *DefaultChannelHandlerContext) CanHandleDownStream() bool {
	return p.canHandleDownStream
}

func (p *DefaultChannelHandlerContext) CanHandleUpStream() bool {
	return p.canHandleUpStream
}

func (p *DefaultChannelHandlerContext) getUpStreamChannelHandlerContext(ctx *DefaultChannelHandlerContext) *DefaultChannelHandlerContext {
	next := ctx.next
	if next == nil {
		return nil
	}
	for !next.canHandleUpStream {
		next = next.next
		if next == nil {
			return nil
		}
	}
	return next
}
func (p *DefaultChannelHandlerContext) getDownStreamChannelHandlerContext(ctx *DefaultChannelHandlerContext) *DefaultChannelHandlerContext {
	pre := ctx.prev
	if pre == nil {
		return nil
	}
	for !pre.canHandleDownStream {
		pre = pre.prev
		if pre == nil {
			return nil
		}
	}
	return pre
}

func (p *DefaultChannelHandlerContext) SendUpStream(data interface{}) {
	up := p.getUpStreamChannelHandlerContext(p)
	if up == nil {
		return
	}
	var context ChannelHandlerContext = up
	channel, ok := context.Handler().(ChannelUpStreamHandler)
	if !ok {
		return
	}
	channel.OnChannelUpStream(context, data)
}

func (p *DefaultChannelHandlerContext) SendDownStream(data interface{}) {
	down := p.getDownStreamChannelHandlerContext(p)
	if down == nil {
		return
	}
	var context ChannelHandlerContext = down
	if context == nil {
		return
	}
	channel, ok := context.Handler().(ChannelDownStreamHandler)
	if !ok {
		return
	}
	channel.OnChannelDownStream(context, data)
}

func canHandleDownStream(handler ChannelHandler) bool {
	_, ok := (handler).(ChannelDownStreamHandler)
	return ok
}

func canHandleUpStream(handler ChannelHandler) bool {
	_, ok := (handler).(ChannelUpStreamHandler)
	return ok
}

func (p *DefaultChannelHandlerPipeline) AddFirst(name string, handler ChannelHandler) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if len(p.channelMap) == 0 {
		ctx := &DefaultChannelHandlerContext{
			name:                name,
			handler:             handler,
			next:                nil,
			prev:                nil,
			canHandleDownStream: canHandleDownStream(handler),
			canHandleUpStream:   canHandleUpStream(handler),
		}
		p.head = ctx
		p.tail = ctx
		p.channelMap[name] = ctx
		return nil
	} else {
		if ok := p.checkName(name); ok {
			return errors.New(name + " is exists ")
		}
		oldHead := p.head
		ctx := &DefaultChannelHandlerContext{
			name:                name,
			handler:             handler,
			next:                oldHead,
			prev:                nil,
			canHandleDownStream: canHandleDownStream(handler),
			canHandleUpStream:   canHandleUpStream(handler),
		}
		oldHead.prev = ctx
		p.head = ctx
		p.channelMap[name] = ctx
		return nil
	}
}

func (p *DefaultChannelHandlerPipeline) AddLast(name string, handler ChannelHandler) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if len(p.channelMap) == 0 {
		ctx := &DefaultChannelHandlerContext{
			name:                name,
			handler:             handler,
			next:                nil,
			prev:                nil,
			canHandleDownStream: canHandleDownStream(handler),
			canHandleUpStream:   canHandleUpStream(handler),
		}
		p.head = ctx
		p.tail = ctx
		p.channelMap[name] = ctx
		return nil
	} else {
		if ok := p.checkName(name); ok {
			return errors.New(name + " is exists ")
		}
		oldTail := p.tail
		ctx := &DefaultChannelHandlerContext{
			name:                name,
			handler:             handler,
			next:                nil,
			prev:                oldTail,
			canHandleDownStream: canHandleDownStream(handler),
			canHandleUpStream:   canHandleUpStream(handler),
		}
		oldTail.next = ctx
		p.tail = ctx
		p.channelMap[name] = ctx
		return nil
	}
}

func (p *DefaultChannelHandlerPipeline) Get(name string) ChannelHandler {
	p.lock.Lock()
	defer p.lock.Unlock()
	if value, ok := p.channelMap[name]; ok {
		return value.handler
	}
	return nil
}

func (p *DefaultChannelHandlerPipeline) GetFirst() ChannelHandler {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return &p.head.handler
}

func (p *DefaultChannelHandlerPipeline) GetLast() ChannelHandler {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return &p.tail.handler
}

func (p *DefaultChannelHandlerPipeline) RemoveFirst() ChannelHandler {
	p.lock.Lock()
	defer p.lock.Unlock()
	oldHead := p.head
	next := p.head.next
	p.head = next
	if next != nil {
		next.prev = nil
	}
	oldHead.next = nil
	delete(p.channelMap, oldHead.name)
	if len(p.channelMap) == 0 {
		p.head = nil
		p.tail = nil
	}
	return oldHead.handler
}

func (p *DefaultChannelHandlerPipeline) RemoveLast() ChannelHandler {
	p.lock.Lock()
	defer p.lock.Unlock()
	oldTail := p.tail
	prev := p.tail.prev
	if prev != nil {
		prev.next = nil
	}
	p.tail = prev
	oldTail.prev = nil
	delete(p.channelMap, oldTail.name)
	if len(p.channelMap) == 0 {
		p.head = nil
		p.tail = nil
	}
	return oldTail.handler
}

func (p *DefaultChannelHandlerPipeline) checkName(name string) bool {
	if _, ok := p.channelMap[name]; ok {
		return ok
	}
	return false
}

func (p *DefaultChannelHandlerPipeline) Replace(oldName string, newName string, handler ChannelHandler) (ChannelHandler, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if ok := p.checkName(newName); ok {
		return nil, errors.New(newName + " is exists ")
	}
	if ok := p.checkName(oldName); !ok {
		return nil, errors.New(oldName + " is not exists ")
	}
	if len(p.channelMap) == 0 {
		return nil, nil
	}
	next := p.head
	for next != nil {
		if next.name == oldName {
			prev := next.prev
			nextNext := next.next
			ctx := &DefaultChannelHandlerContext{
				name:                newName,
				handler:             handler,
				next:                nextNext,
				prev:                prev,
				canHandleDownStream: canHandleDownStream(handler),
				canHandleUpStream:   canHandleUpStream(handler),
			}
			if prev != nil {
				prev.next = ctx
			}
			if nextNext != nil {
				nextNext.prev = ctx
			}
			p.channelMap[newName] = ctx
			return next.handler, nil
		}
		next = next.next
	}
	return nil, nil
}

func (p *DefaultChannelHandlerPipeline) getUpStreamChannelHandlerContext(ctx *DefaultChannelHandlerContext) *DefaultChannelHandlerContext {
	next := ctx
	if next == nil {
		return nil
	}
	for !next.canHandleUpStream {
		next = next.next
		if next == nil {
			return nil
		}
	}
	return next
}
func (p *DefaultChannelHandlerPipeline) getDownStreamChannelHandlerContext(ctx *DefaultChannelHandlerContext) *DefaultChannelHandlerContext {
	pre := ctx
	if pre == nil {
		return nil
	}
	for !pre.canHandleDownStream {
		pre = pre.prev
		if pre == nil {
			return nil
		}
	}
	return pre
}

func (p *DefaultChannelHandlerPipeline) SendUpStream(data interface{}) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	context := p.getUpStreamChannelHandlerContext(p.head)
	if context == nil {
		return
	}
	var contextChannel ChannelHandlerContext = context
	channel, ok := context.Handler().(ChannelUpStreamHandler)
	if !ok {
		return
	}
	channel.OnChannelUpStream(contextChannel, data)
}

func (p *DefaultChannelHandlerPipeline) SendDownStream(data interface{}) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	context := p.getDownStreamChannelHandlerContext(p.tail)
	if context == nil {
		return
	}
	var contextChannel ChannelHandlerContext = context
	channel, ok := context.Handler().(ChannelDownStreamHandler)
	if !ok {
		return
	}
	channel.OnChannelDownStream(contextChannel, data)
}
