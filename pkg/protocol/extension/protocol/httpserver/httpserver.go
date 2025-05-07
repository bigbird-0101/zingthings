package httpserver

import (
	"context"
	"errors"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strconv"
	"zingthings/pkg/protocol/core"
)

const DefaultPort = 8787
const success = "{\"code\":1,\"message\":\"ok\"}"

type (
	ProtocolImpl struct {
		core.GenericProtocol
		logger     *zap.Logger
		httpServer *http.Server
	}
)

func init() {
	core.RegisterProtocol(core.ProtocolInitRegister{
		ProtocolType: core.HttpServer,
		Setup: func(context *core.ProtocolSetupContext) core.Protocol {
			return NewHttpServerProtocol(context.Logger, context.Protocol)
		},
	})
}

func NewHttpServerProtocol(logger *zap.Logger, protocol core.GenericProtocol) *ProtocolImpl {
	return &ProtocolImpl{
		GenericProtocol: protocol,
		logger:          logger.Named("HttpServerProtocol"),
		httpServer:      &http.Server{},
	}
}

func (httpServer *ProtocolImpl) GetProtocolId() core.ProtocolId {
	return httpServer.ProtocolId
}

func (httpServer *ProtocolImpl) getHandler() http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/{deviceGroupId}/{deviceId}/propertyReport", httpServer.propertyReport)
	return r
}

func (httpServer *ProtocolImpl) propertyReport(response http.ResponseWriter, request *http.Request) {
	body := request.Body
	vars := mux.Vars(request)
	deviceId := vars["deviceId"]
	deviceGroupId := vars["deviceGroupId"]
	defer body.Close()
	all, err := io.ReadAll(body)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	checkSuccess := false
	for _, k := range httpServer.DeviceInfos {
		if k.DeviceId == core.DeviceId(deviceId) && (core.DeviceGroupId(deviceGroupId) ==
			httpServer.DeviceGroup.DeviceGroupId || core.DeviceGroupId(deviceGroupId) == k.DeviceGroup.DeviceGroupId) {
			checkSuccess = true
			k.DeviceGroup = httpServer.DeviceGroup
			break
		}
	}
	if !checkSuccess {
		http.Error(response, "Device Not Found", http.StatusNotFound)
		return
	}
	message := core.NewUpMessage(core.Header{
		ProtocolType:  httpServer.GetProtocolType(),
		DeviceId:      core.DeviceId(deviceId),
		DeviceGroupId: httpServer.DeviceGroup.DeviceGroupId,
	}, all)
	httpServer.logger.Info("start up message")
	err = httpServer.UpLink.UpLink(message)
	httpServer.logger.Info("start up  message success")
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	httpServer.writeSuccessResponse(response)
}

func (httpServer *ProtocolImpl) writeSuccessResponse(response http.ResponseWriter) {
	response.WriteHeader(http.StatusOK)
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, err := response.Write([]byte(success))
	if err != nil {
		return
	}
}

func (httpServer *ProtocolImpl) Start(context *core.NodeContext) {
	port := context.GetPort(DefaultPort)
	httpServer.httpServer.Addr = ":" + strconv.Itoa(port)
	httpServer.httpServer.Handler = httpServer.getHandler()
	go func() {
		err := httpServer.httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpServer.logger.Error("start httpServer server error", zap.Error(err))
			return
		}
	}()
	err := httpServer.ChannelHandlerPipeline.AddFirst("httpServerDown", NewHttpServerDownChannelHandler(httpServer))
	if err != nil {
		return
	}
	channel := httpServer.ChannelFactory.Create(httpServer.ChannelHandlerPipeline, httpServer)
	channel.Attach(core.ProtocolTypeKey, core.HttpServer)
	for _, info := range httpServer.DeviceInfos {
		httpServer.ChannelManager.Register(string(info.DeviceId), channel)
	}
	httpServer.logger.Info("start httpServer", zap.Int("port", port))
}

func (httpServer *ProtocolImpl) Stop() {
	httpServer.logger.Info("stop httpServer")
	err := httpServer.httpServer.Shutdown(context.TODO())
	if err != nil {
		httpServer.logger.Error("stop httpServer server error", zap.Error(err))
		return
	}
	for _, info := range httpServer.DeviceInfos {
		httpServer.ChannelManager.Remove(string(info.DeviceId))
	}
}

func (httpServer *ProtocolImpl) GetProtocolType() core.ProtocolType {
	return core.HttpServer
}
