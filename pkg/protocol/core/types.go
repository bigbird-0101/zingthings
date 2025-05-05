package core

import (
	"context"
	"encoding/json"
	"go.uber.org/zap"
	"net/http"
	"zingthings/pkg/protocol/config"
)

type (
	// ProtocolId unique identifier
	ProtocolId string
	// NodeContext node runtime context
	NodeContext struct {
		Properties map[string]interface{}
	}

	Node interface {
		// Start Node started
		Start(context *NodeContext)
		// Stop Node
		Stop()
	}

	DataCollector[K comparable, V any] interface {
		Execute(config map[K]V) error
	}

	ProtocolType string

	Protocol interface {
		Node
		GetProtocolType() ProtocolType
		GetProtocolId() ProtocolId
	}

	// ProtocolWrapper 创建一个包装器结构体
	ProtocolWrapper struct {
		Protocol    // 内部的实际 Node 实现
		DeviceGroup DeviceGroup
		DeviceInfos []*DeviceInfo
	}

	DeviceGroupId string
	DeviceId      string

	Header struct {
		DeviceGroupId DeviceGroupId
		DeviceId      DeviceId
		ProtocolType  ProtocolType
	}

	Message struct {
		Header   Header
		Metadata map[string]interface{}
		Data     interface{}
	}

	ResponseData struct {
		Data     interface{}
		Response chan *Message
	}

	DeviceGroup struct {
		DeviceGroupName string
		DeviceGroupId   DeviceGroupId
	}

	DeviceInfo struct {
		Properties  map[string]interface{}
		DeviceId    DeviceId
		DeviceName  string
		DeviceGroup DeviceGroup
	}

	SetupContext struct {
		Context context.Context
		Logger  *zap.Logger
		Config  *config.Config
	}

	ChannelHandler interface {
	}

	ChannelHandlerRegister struct {
		ChannelHandlerType string
		Setup              func(setupContext *SetupContext) error
	}

	ChannelDownStreamHandler interface {
		ChannelHandler
		OnChannelDownStream(context ChannelHandlerContext, data interface{})
	}
	ChannelUpStreamHandler interface {
		ChannelHandler
		OnChannelUpStream(context ChannelHandlerContext, data interface{})
	}

	ChannelHandlerContext interface {
		Name() string
		Handler() ChannelHandler
		CanHandleDownStream() bool
		CanHandleUpStream() bool
		SendUpStream(data interface{})
		SendDownStream(data interface{})
	}

	ChannelHandlerPipeline interface {
		AddFirst(name string, handler ChannelHandler) error
		AddLast(name string, handler ChannelHandler) error
		Get(name string) ChannelHandler
		GetFirst() ChannelHandler
		GetLast() ChannelHandler
		RemoveFirst() ChannelHandler
		RemoveLast() ChannelHandler
		Replace(oldName string, newName string, newHandler ChannelHandler) (ChannelHandler, error)
		SendUpStream(data interface{})
		SendDownStream(data interface{})
	}
	Channel interface {
		ChannelHandlerPipeline() ChannelHandlerPipeline
		ChannelFactory() ChannelFactory
		GetProtocol() Protocol
		Attach(key string, value interface{})
		GetAttach(key string) interface{}
		SendUpStream(data interface{})
		SendDownStream(data interface{})
	}
	ChannelFactory interface {
		Create(pipeline ChannelHandlerPipeline, protocol Protocol) Channel
	}
	PipelineFactory interface {
		Create() ChannelHandlerPipeline
	}

	ChannelManager interface {
		Register(key string, handler Channel) Channel
		Get(key string) Channel
		RegisterIfAbsent(key string, handler Channel) Channel
		Remove(key string) Channel
	}

	ProtocolManager interface {
		Register(key ProtocolId, protocol Protocol) Protocol
		Get(key ProtocolId) Protocol
		RegisterIfAbsent(key ProtocolId, protocol Protocol) Protocol
		Remove(key ProtocolId) Protocol
		GetAll() []Protocol
	}
)

const (
	PENDING = Status("PENDING")
	RUNNING = Status("RUNNING")
)

type (
	NodeInfo struct {
		Host      string `json:"host"`
		Port      int    `json:"port"`
		Timestamp int64  `json:"timestamp"`
	}

	ProtocolInfo struct {
		Id           ProtocolId   `json:"id"`
		Timestamp    int64        `json:"timestamp"`
		ProtocolType ProtocolType `json:"protocolType"`
		Address      string       `json:"address"`
	}

	Status string

	ProtocolStatus struct {
		Status Status `json:"status"`
	}

	ProtocolInfoDetail struct {
		DeviceGroup DeviceGroup   `json:"deviceGroup"`
		DeviceInfos []*DeviceInfo `json:"deviceInfos"`
	}

	DeployRequest struct {
		NumberIndex   int                      `json:"numberIndex"`
		ProtocolId    ProtocolId               `json:"protocolId"`
		DeviceInfoMap map[string][]*DeviceInfo `json:"deviceInfoMap"`
		Properties    map[string]interface{}   `json:"properties"`
		ProtocolType  ProtocolType             `json:"protocolType"`
	}
	UnDeployRequest struct {
		NumberIndex  int          `json:"numberIndex"`
		ProtocolId   ProtocolId   `json:"protocolId"`
		ProtocolType ProtocolType `json:"protocolType"`
		DeviceGroup  DeviceGroup  `json:"deviceGroup"`
	}
	Response struct {
		Data    interface{} `json:"data"`
		Code    int         `json:"code"`
		Message string      `json:"message"`
	}
)

func Ok(data interface{}) *Response {
	return &Response{
		Code:    1,
		Message: "ok",
		Data:    data,
	}
}

func (r *Response) ToBytes() ([]byte, error) {
	return json.Marshal(r)
}

func (r *Response) WriteStatus(statusCode int, w http.ResponseWriter) {
	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	toBytes, err2 := r.ToBytes()
	if err2 != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("{\"code\":0,\"message\":\"to byte fail\"}"))
		return
	}
	_, err := w.Write(toBytes)
	if err != nil {
		return
	}
}

func (r *Response) WriteError(w http.ResponseWriter) {
	r.WriteStatus(http.StatusInternalServerError, w)
}

func (r *Response) WriteOk(w http.ResponseWriter) {
	r.WriteStatus(http.StatusOK, w)
}

func Fail() *Response {
	return &Response{
		Code:    0,
		Message: "fail",
	}
}

func FailWithMessage(message string) *Response {
	return &Response{
		Code:    0,
		Message: message,
	}
}

func FailWithError(err error) *Response {
	return &Response{
		Code:    0,
		Message: err.Error(),
	}
}
