package core

import "sync"

const (
	ProtocolTypeKey   = "ProtocolType"
	NetworkConnection = "NetworkConnection"
)

type (
	DefaultChannel struct {
		protocol     Protocol
		pipeline     ChannelHandlerPipeline
		factory      ChannelFactory
		ProtocolType ProtocolType
		lock         sync.RWMutex
		attachMap    map[string]interface{}
	}
	DefaultChannelFactory struct {
	}
	DefaultChannelManager struct {
		channelManagerMap map[string]Channel
		lock              sync.RWMutex
	}
)

var DefaultChannelManagerCommon = NewChannelManager()
var DefaultChannelFactoryCommon = &DefaultChannelFactory{}

func NewChannelManager() *DefaultChannelManager {
	return &DefaultChannelManager{
		channelManagerMap: make(map[string]Channel),
	}
}
func (cm *DefaultChannelManager) Register(channelName string, channel Channel) Channel {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	cm.channelManagerMap[channelName] = channel
	return channel
}

func (cm *DefaultChannelManager) Get(channelName string) Channel {
	cm.lock.RLock()
	defer cm.lock.RUnlock()
	channel, ok := cm.channelManagerMap[channelName]
	if ok {
		return channel
	}
	return nil
}

func (cm *DefaultChannelManager) RegisterIfAbsent(channelName string, channel Channel) Channel {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	value, ok := cm.channelManagerMap[channelName]
	if !ok {
		cm.channelManagerMap[channelName] = channel
		return channel
	}
	return value
}

func (cm *DefaultChannelManager) Remove(channelName string) Channel {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	channel := cm.channelManagerMap[channelName]
	delete(cm.channelManagerMap, channelName)
	return channel
}

func (d *DefaultChannel) ChannelHandlerPipeline() ChannelHandlerPipeline {
	return d.pipeline
}

func (d *DefaultChannel) ChannelFactory() ChannelFactory {
	return d.factory
}

func (d *DefaultChannel) SendUpStream(data interface{}) {
	d.pipeline.SendUpStream(data)
}

func (d *DefaultChannel) SendDownStream(data interface{}) {
	d.pipeline.SendDownStream(data)
}

func (d *DefaultChannel) Attach(key string, value interface{}) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.attachMap[key] = value
}

func (d *DefaultChannel) GetAttach(key string) interface{} {
	d.lock.RLock()
	defer d.lock.RUnlock()
	i, _ := d.attachMap[key]
	return i
}

func (d *DefaultChannel) GetProtocol() Protocol {
	return d.protocol
}

func (d *DefaultChannelFactory) Create(pipeline ChannelHandlerPipeline, protocol Protocol) Channel {
	return &DefaultChannel{
		protocol:  protocol,
		pipeline:  pipeline,
		factory:   d,
		lock:      sync.RWMutex{},
		attachMap: make(map[string]interface{}),
	}
}
