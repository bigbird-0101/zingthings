package core

import (
	"go.uber.org/zap"
	"sync"
	"zingthings/pkg/common"
	"zingthings/pkg/protocol/config"
)

const (
	HttpClient = "HTTP_CLIENT"
	HttpServer = "HTTP_SERVER"
	TcpServer  = "TCP_SERVER"
	TcpClient  = "TCP_CLIENT"
)

type (
	GenericProtocol struct {
		Protocol
		ProtocolId             ProtocolId
		DeviceGroup            DeviceGroup
		DeviceInfos            []*DeviceInfo
		ChannelManager         ChannelManager
		ChannelFactory         ChannelFactory
		ChannelHandlerPipeline ChannelHandlerPipeline
		DownLink               *DownLink
		UpLink                 *UpLink
	}

	DefaultProtocolManager struct {
		ProtocolMap map[ProtocolId]Protocol
		lock        sync.RWMutex
	}

	ProtocolSetupContext struct {
		Protocol GenericProtocol
		Logger   *zap.Logger
		Config   *config.Config
	}

	Setup func(context *ProtocolSetupContext) Protocol

	ProtocolInitRegister struct {
		ProtocolType ProtocolType
		Setup        Setup
	}
	ProtocolInitRegisterManager struct {
		protocolInitRegisterMap map[ProtocolType]ProtocolInitRegister
	}
)

var (
	DefaultProtocolManagerCommon       = NewProtocolManager()
	ChannelHandlerPipelineCommon       = DefaultPipelineFactoryCommon.Create()
	DefaultProtocolInitRegisterManager = &ProtocolInitRegisterManager{
		protocolInitRegisterMap: make(map[ProtocolType]ProtocolInitRegister),
	}
)

func NewGenericProtocol() GenericProtocol {
	return GenericProtocol{
		ChannelManager:         DefaultChannelManagerCommon,
		ChannelFactory:         DefaultChannelFactoryCommon,
		ChannelHandlerPipeline: ChannelHandlerPipelineCommon,
		DownLink:               NewDownLink(),
		UpLink:                 NewUpLink(),
	}
}

func NewProtocolManager() *DefaultProtocolManager {
	return &DefaultProtocolManager{
		ProtocolMap: make(map[ProtocolId]Protocol),
		lock:        sync.RWMutex{},
	}
}

func (d *DefaultProtocolManager) Register(key ProtocolId, protocol Protocol) Protocol {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.ProtocolMap[key] = protocol
	return protocol
}

func (d *DefaultProtocolManager) Get(key ProtocolId) Protocol {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.ProtocolMap[key]
}

func (d *DefaultProtocolManager) RegisterIfAbsent(key ProtocolId, protocol Protocol) Protocol {
	d.lock.Lock()
	defer d.lock.Unlock()
	p, ok := d.ProtocolMap[key]
	if !ok {
		d.ProtocolMap[key] = protocol
		return protocol
	}
	return p
}

func (d *DefaultProtocolManager) Remove(key ProtocolId) Protocol {
	d.lock.Lock()
	defer d.lock.Unlock()
	protocol := d.ProtocolMap[key]
	delete(d.ProtocolMap, key)
	return protocol
}

func (d *DefaultProtocolManager) GetAll() []Protocol {
	d.lock.RLock()
	defer d.lock.RUnlock()
	protocols := make([]Protocol, 0)
	for _, v := range d.ProtocolMap {
		protocols = append(protocols, v)
	}
	return protocols
}

// IsNotLongConnection 非长连接
func IsNotLongConnection(protocolType ProtocolType) bool {
	switch protocolType {
	case HttpClient, HttpServer, TcpClient:
		return true
	case TcpServer:
		return false
	}
	return true
}

func (nw *ProtocolWrapper) Stop() {
	nw.Protocol.Stop()
	DefaultEventBus.Publish(common.ProtocolUnDeploySuccess, nw.Protocol)
}

// Start 实现 Node 接口的方法
func (nw *ProtocolWrapper) Start(ctx *NodeContext) {
	go func() {
		nw.Protocol.Start(ctx)
		DefaultEventBus.Publish(common.ProtocolDeploySuccess, nw.Protocol)
	}()
}

func (nw *ProtocolWrapper) GetProtocolType() ProtocolType {
	return nw.Protocol.GetProtocolType()
}

func (nw *ProtocolWrapper) GetProtocolId() ProtocolId {
	return nw.Protocol.GetProtocolId()
}

func RegisterProtocol(initRegister ProtocolInitRegister) {
	DefaultProtocolInitRegisterManager.protocolInitRegisterMap[initRegister.ProtocolType] = initRegister
}

func GetProtocolSetup(protocolType ProtocolType) Setup {
	v, ok := DefaultProtocolInitRegisterManager.protocolInitRegisterMap[protocolType]
	if !ok {
		return nil
	}
	return v.Setup
}

func InitProtocol(ctx *ProtocolSetupContext) Protocol {
	initRegister := DefaultProtocolInitRegisterManager.protocolInitRegisterMap[ctx.Protocol.GetProtocolType()]
	if initRegister.Setup != nil {
		return initRegister.Setup(ctx)
	}
	return nil
}
