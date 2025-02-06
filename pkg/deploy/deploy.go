package deploy

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strconv"
	"time"
	"zingthings/pkg/protocol/container/client"
	"zingthings/pkg/protocol/core"
	"zingthings/pkg/util/env"
	"zingthings/pkg/util/httpserver"
	"zingthings/pkg/util/network"
)

const (
	DefaultPort = 8775
)

type (
	Deploy struct {
		coordinator *Coordinator
		logger      *zap.Logger
	}
	Request struct {
		core.DeployRequest
		//部署的个数
		Number int `json:"number"`
	}
)

func Server(ctx context.Context, logger *zap.Logger) {
	port := env.GetEnvOrDefault[int]("DeployPort", DefaultPort)
	deploy := NewDeploy(ctx, logger, port)
	deploy.Start()
	httpserver.StartServer(ctx, logger, "deploy", port, deploy.getHandler())
}

func NewDeploy(ctx context.Context, logger *zap.Logger, port int) *Deploy {
	ip, err := network.GetLocalIP()
	if err != nil {
		panic(err)
	}
	return &Deploy{
		coordinator: NewCoordinator(ctx, logger, &core.NodeInfo{Host: ip, Port: port, Timestamp: time.Now().UnixMilli()}),
		logger:      logger.Named("Deploy"),
	}
}

func (deploy *Deploy) Start() {
	deploy.coordinator.Start()
}

func (deploy *Deploy) getHandler() http.Handler {
	router := mux.NewRouter()
	router.HandleFunc("/deploy", deploy.dealDeployRequest).Methods("POST")
	return router
}

func (deploy *Deploy) dealDeployRequest(response http.ResponseWriter, request *http.Request) {
	requestBody := &Request{}
	all, err := io.ReadAll(request.Body)
	if err != nil {
		deploy.logger.Error("read body fail", zap.Error(err))
		response.WriteHeader(http.StatusBadRequest)
		_, err3 := response.Write([]byte(err.Error()))
		if err3 != nil {
			return
		}
		return
	}
	err = json.Unmarshal(all, requestBody)
	if err != nil {
		deploy.logger.Error("Unmarshal fail", zap.Error(err))
		response.WriteHeader(http.StatusBadRequest)
		_, err3 := response.Write([]byte(err.Error()))
		if err3 != nil {
			return
		}
		return
	}
	number := 0
	if requestBody.Number <= 0 {
		number = 1
	} else {
		number = requestBody.Number
	}

	alive := deploy.coordinator.getAllAlive()
	if len(alive) == 0 {
		response.WriteHeader(http.StatusServiceUnavailable)
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, err = response.Write([]byte("{\"code\":0,\"message\":\"无可用的节点提供部署\"}"))
		if err != nil {
			return
		}
		return
	}
	type DeployDeviceGroup struct {
		DeviceGroupId core.DeviceGroupId
		index         int
	}
	if len(alive)-number > 0 {
		for i := 0; i < number; i++ {
			for deviceGroupId := range requestBody.DeployRequest.DeviceInfoMap {
				protocolInfoTemp := &core.ProtocolInfo{ProtocolType: requestBody.DeployRequest.ProtocolType,
					Id: core.ProtocolId(string(requestBody.DeployRequest.ProtocolId) + ":" + deviceGroupId + ":" +
						strconv.Itoa(i)), Address: "", Timestamp: time.Now().UnixMilli()}
				if _, ok := deploy.coordinator.checkProtocolAlreadyAlive(protocolInfoTemp); !ok {
					requestObj := &Request{
						DeployRequest: core.DeployRequest{
							DeviceInfoMap: map[string][]*core.DeviceInfo{
								deviceGroupId: requestBody.DeployRequest.DeviceInfoMap[deviceGroupId],
							},
							NumberIndex:  i,
							Properties:   requestBody.DeployRequest.Properties,
							ProtocolId:   requestBody.DeployRequest.ProtocolId,
							ProtocolType: requestBody.DeployRequest.ProtocolType,
						},
					}
					deploy.doDealDeploy(deploy.coordinator.getAlive(), requestObj, i)
				} else {
					deploy.logger.Info("already deploy", zap.Any("protocolInfo", protocolInfoTemp))
				}
			}
		}
	} else {
		for i := range alive {
			for deviceGroupId := range requestBody.DeployRequest.DeviceInfoMap {
				protocolInfoTemp := &core.ProtocolInfo{ProtocolType: requestBody.DeployRequest.ProtocolType,
					Id: core.ProtocolId(string(requestBody.DeployRequest.ProtocolId) + ":" + deviceGroupId + ":" +
						strconv.Itoa(i)), Address: "", Timestamp: time.Now().UnixMilli()}
				if _, ok := deploy.coordinator.checkProtocolAlreadyAlive(protocolInfoTemp); !ok {
					requestObj := &Request{
						DeployRequest: core.DeployRequest{
							DeviceInfoMap: map[string][]*core.DeviceInfo{
								deviceGroupId: requestBody.DeployRequest.DeviceInfoMap[deviceGroupId],
							},
							NumberIndex:  i,
							Properties:   requestBody.DeployRequest.Properties,
							ProtocolId:   requestBody.DeployRequest.ProtocolId,
							ProtocolType: requestBody.DeployRequest.ProtocolType,
						},
					}
					deploy.doDealDeploy(deploy.coordinator.getAlive(), requestObj, i)
				} else {
					deploy.logger.Info("already deploy", zap.Any("protocolInfo", protocolInfoTemp))
				}
			}
		}
	}
	response.WriteHeader(http.StatusOK)
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, err = response.Write([]byte("{\"code\":1,\"message\":\"ok\"}"))
	if err != nil {
		return
	}
	return
}

func (deploy *Deploy) doDealDeploy(nodeInfo *core.NodeInfo, request *Request, index int) {
	makeClient := client.MakeClient(deploy.logger, fmt.Sprintf("http://%s:%d", nodeInfo.Host, nodeInfo.Port))
	marshal, err := json.Marshal(request)
	if err != nil {
		deploy.logger.Error("marshal fail", zap.Error(err))
		return
	}
	deployRequest := &core.DeployRequest{}
	err = json.Unmarshal(marshal, deployRequest)
	deployRequest.NumberIndex = index
	deployResponse, err6 := makeClient.Deploy(deployRequest)
	if err6 != nil {
		deploy.logger.Error("deploy fail", zap.Error(err6))
		return
	}
	deploy.logger.Info("deploy success", zap.Any("response", deployResponse))
}
