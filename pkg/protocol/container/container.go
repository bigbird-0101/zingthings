package container

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"zingthings/pkg/common"
	"zingthings/pkg/protocol/config"
	"zingthings/pkg/protocol/core"
	httpClientProtocol "zingthings/pkg/protocol/extension/protocol/httpclient"
	httpServerProtocol "zingthings/pkg/protocol/extension/protocol/httpserver"
	tcpServerProtocol "zingthings/pkg/protocol/extension/protocol/tcpserver"
	"zingthings/pkg/protocol/kafkadown"
	"zingthings/pkg/protocol/register"
	"zingthings/pkg/util/httpserver"
)

const (
	DefaultPort = "8776"
)

type (
	Container struct {
		ProtocolManager core.ProtocolManager
		logger          *zap.Logger
		ctx             context.Context
		etcdRegister    *register.EtcdRegister
	}
)

func GetContainerServerPort() int {
	deployPort := os.Getenv("DeployPort")
	if "" == deployPort {
		deployPort = DefaultPort
	}
	port, err := strconv.Atoi(deployPort)
	if err != nil {
		panic(err)
	}
	return port
}

func Server(ctx context.Context, logger *zap.Logger, config *config.Config) {
	deploy := NewContainer(logger, core.DefaultProtocolManagerCommon, ctx)
	core.InitChannelHandler(ctx, logger, config)
	go kafkadown.NewSaramaKafkaDown(logger, ctx, config).Start()
	serverPort := GetContainerServerPort()
	etcdRegister := register.NewEtcdRegister(ctx, logger, serverPort)
	deploy.etcdRegister = etcdRegister
	go etcdRegister.Start()
	var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	c := make(chan os.Signal, 1)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		for _, p := range core.DefaultProtocolManagerCommon.GetAll() {
			p.Stop()
		}
	}()
	httpserver.StartServer(ctx, logger, "Container", serverPort, deploy.getHandler())
}

func (d *Container) getHandler() http.Handler {
	router := mux.NewRouter()
	router.HandleFunc("/deploy", d.dealDeployRequest).Methods("POST")
	router.HandleFunc("/protocolStatus", d.getProtocolStatus).Methods("POST")
	router.HandleFunc("/undeploy", d.dealUnDeployRequest).Methods("POST")
	router.HandleFunc("/protocolAll", d.protocolAll).Methods("GET")
	return router
}

func (d *Container) protocolAll(response http.ResponseWriter, r *http.Request) {
	all := core.DefaultProtocolManagerCommon.GetAll()
	response.WriteHeader(http.StatusOK)
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	marshal, err := json.Marshal(core.Response{Data: all, Code: 1, Message: "成功"})
	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		_, err2 := response.Write([]byte(err.Error()))
		if err2 != nil {
			return
		}
		return
	}
	_, err = response.Write(marshal)
	if err != nil {
		return
	}
	return
}
func (d *Container) dealUnDeployRequest(response http.ResponseWriter, r *http.Request) {

	all, err := io.ReadAll(r.Body)
	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		_, err2 := response.Write([]byte(err.Error()))
		if err2 != nil {
			return
		}
		return
	}
	request := &core.UnDeployRequest{}
	err = json.Unmarshal(all, request)
	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		_, err2 := response.Write([]byte(err.Error()))
		if err2 != nil {
			return
		}
		return
	}
	d.logger.Info("deal unDeploy request", zap.Any("request", request))
	protocolId := ""
	if strings.Contains(string(request.ProtocolId), ":") {
		protocolId = string(request.ProtocolId)
	} else {
		protocolId = strings.Join([]string{string(request.ProtocolId), string(request.DeviceGroup.DeviceGroupId),
			strconv.Itoa(request.NumberIndex)}, ":")
	}
	protocol := core.DefaultProtocolManagerCommon.Get(core.ProtocolId(protocolId))
	if protocol != nil {
		protocol.Stop()
	} else {
		d.logger.Info("protocol not found", zap.Any("request", request))
	}
	response.WriteHeader(http.StatusOK)
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, err = response.Write([]byte("{\"code\":1,\"message\":\"ok\"}"))
	if err != nil {
		return
	}
	return
}
func (d *Container) dealDeployRequest(response http.ResponseWriter, r *http.Request) {
	all, err := io.ReadAll(r.Body)
	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		_, err2 := response.Write([]byte(err.Error()))
		if err2 != nil {
			return
		}
		return
	}
	request := &core.DeployRequest{}
	err = json.Unmarshal(all, request)
	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		_, err2 := response.Write([]byte(err.Error()))
		if err2 != nil {
			return
		}
		return
	}
	d.logger.Info("deal deploy request", zap.Any("request", request))
	err = d.Deploy(request)
	if err != nil {
		response.WriteHeader(http.StatusBadRequest)
		_, err3 := response.Write([]byte(err.Error()))
		if err3 != nil {
			return
		}
		return
	}
	response.WriteHeader(http.StatusOK)
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, err = response.Write([]byte("{\"code\":1,\"message\":\"ok\"}"))
	if err != nil {
		return
	}
	return
}

func NewContainer(logger *zap.Logger, protocolManager core.ProtocolManager, ctx context.Context) *Container {
	return &Container{
		ProtocolManager: protocolManager,
		logger:          logger.Named("protocol-container"),
		ctx:             ctx,
	}
}

func (d *Container) Deploy(request *core.DeployRequest) error {
	protocolType := request.ProtocolType
	for group, info := range request.DeviceInfoMap {
		protocolId := ""
		if strings.Contains(string(request.ProtocolId), ":") {
			protocolId = string(request.ProtocolId)
		} else {
			protocolId = strings.Join([]string{string(request.ProtocolId),
				group, strconv.Itoa(request.NumberIndex)}, ":")
		}
		get := d.ProtocolManager.Get(core.ProtocolId(protocolId))
		if nil != get {
			d.logger.Info("deploy already success", zap.Any("ProtocolType", request.ProtocolType),
				zap.String("protocolId", protocolId))
			return nil
		}
		deviceGroup := core.DeviceGroup{DeviceGroupId: core.DeviceGroupId(group)}
		protocol, err := d.getProtocol(protocolType, core.ProtocolId(protocolId),
			deviceGroup, info)
		if err != nil {
			return err
		}
		properties := request.Properties
		nodeContext := &core.NodeContext{
			Properties: properties,
		}
		nodeContext.SetContext(d.ctx)
		protocolWrapper := &core.ProtocolWrapper{
			Protocol:    protocol,
			DeviceGroup: deviceGroup,
			DeviceInfos: info,
		}
		protocol.Start(nodeContext)
		core.DefaultEventBus.Publish(common.ProtocolDeploySuccess, protocolWrapper)
		d.ProtocolManager.RegisterIfAbsent(core.ProtocolId(protocolId), protocolWrapper)
	}
	d.logger.Info("protocol-container success")
	return nil
}

func (d *Container) getProtocol(protocolType core.ProtocolType, protocolId core.ProtocolId, group core.DeviceGroup,
	info []*core.DeviceInfo) (core.Protocol, error) {
	protocol := core.NewGenericProtocol()
	protocol.ProtocolId = protocolId
	protocol.DeviceGroup = group
	protocol.DeviceInfos = info
	switch protocolType {
	case core.HttpClient:
		return httpClientProtocol.NewHttpClientProtocol(d.logger, protocol), nil
	case core.HttpServer:
		return httpServerProtocol.NewHttpServerProtocol(d.logger, protocol), nil
	case core.TcpServer:
		return tcpServerProtocol.NewTcpServerProtocol(d.logger, protocol), nil
	}
	return nil, errors.New(string(protocolType + " protocol type not support"))
}

func (d *Container) getProtocolStatus(response http.ResponseWriter, request *http.Request) {
	body := request.Body
	bytes, err := io.ReadAll(body)
	if err != nil {
		d.logger.Error("read body fail", zap.Error(err))
		core.FailWithError(err).WriteStatus(http.StatusBadRequest, response)
		return
	}
	requestBody := &core.ProtocolInfo{}
	err = json.Unmarshal(bytes, requestBody)
	if err != nil {
		core.FailWithError(err).WriteStatus(http.StatusBadRequest, response)
		return
	}
	address := d.etcdRegister.GetNodeInfo().BuildAddress()
	requestBody.Address = address
	buildKey := requestBody.BuildKey()
	protocol := d.ProtocolManager.Get(requestBody.Id)
	if nil != protocol && d.etcdRegister.KeyExists(buildKey) {
		core.Ok(core.ProtocolStatus{Status: core.RUNNING}).WriteOk(response)
	} else {
		core.Ok(core.ProtocolStatus{Status: core.PENDING}).WriteOk(response)
	}
}
