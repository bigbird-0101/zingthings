package httpclient

import (
	"encoding/json"
	"github.com/hashicorp/go-retryablehttp"
	githubError "github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"zingthings/pkg/protocol/core"
)

type (
	Protocol struct {
		core.GenericProtocol
		httpClient  *retryablehttp.Client
		logger      *zap.Logger
		nodeContext *core.NodeContext
	}

	HttpRequest struct {
		METHOD string            `json:"method"`
		PATH   string            `json:"path"`
		Header map[string]string `json:"header"`
		BODY   []byte            `json:"BODY"`
		QUERY  map[string]string `json:"QUERY"`
	}
)

func init() {
	core.RegisterProtocol(core.ProtocolInitRegister{
		ProtocolType: core.HttpClient,
		Setup: func(context *core.ProtocolSetupContext) core.Protocol {
			return NewHttpClientProtocol(context.Logger, context.Protocol)
		},
	})
}

func NewHttpClientProtocol(logger *zap.Logger, protocol core.GenericProtocol) *Protocol {
	return &Protocol{
		GenericProtocol: protocol,
		httpClient:      retryablehttp.NewClient(),
		logger:          logger.Named("HttpProtocol"),
	}
}

func (http *Protocol) Start(context *core.NodeContext) {
	http.nodeContext = context
	channel := http.ChannelFactory.Create(http.ChannelHandlerPipeline, http)
	channel.Attach(core.ProtocolTypeKey, core.HttpClient)
	for _, info := range http.DeviceInfos {
		http.ChannelManager.Register(string(info.DeviceId), channel)
	}
}

func (http *Protocol) Stop() {
	for _, info := range http.DeviceInfos {
		http.ChannelManager.Remove(string(info.DeviceId))
	}
}

func (http *Protocol) Execute(config map[string]interface{}) error {
	executionTimeInterval := 5000
	if value, ok := config["executionTimeInterval"]; ok {
		executionTimeInterval = value.(int)
	}
	needRequests := make([]*core.DeviceInfo, 0)
	for _, deviceInfo := range http.DeviceInfos {
		if value, ok := deviceInfo.Properties["executionTimeInterval"]; ok && value == executionTimeInterval {
			needRequests = append(needRequests, deviceInfo)
		}
	}
	for _, deviceInfo := range needRequests {
		httpRequests := deviceInfo.Properties["httpRequests"].([]string)
		for _, httpRequest := range httpRequests {
			var request HttpRequest
			err := json.Unmarshal([]byte(httpRequest), &request)
			if err != nil {
				return githubError.Wrap(err, "to json error")
			}
			go http.dealRequest(request, deviceInfo)
		}
	}
	return nil
}

func (http *Protocol) dealRequest(request HttpRequest, deviceInfo *core.DeviceInfo) {
	deviceInfo.DeviceGroup = http.DeviceGroup
	baseUrl, ok := http.nodeContext.Properties["baseUrl"]
	if !ok {
		http.logger.Error("get base url error")
		return
	}
	switch request.METHOD {
	case "GET":
		get, err := http.httpClient.Get(baseUrl.(string) + request.PATH)
		if err != nil {
			http.logger.Error("error tapping function service address", zap.Error(err))
		}
		defer get.Body.Close()
		all, err := io.ReadAll(get.Body)
		if err != nil {
			http.logger.Error("error tapping function service address json to struct", zap.Error(err))
		}
		err = http.UpLink.UpLink(core.NewUpMessage(core.Header{
			ProtocolType:  core.HttpClient,
			DeviceId:      deviceInfo.DeviceId,
			DeviceGroupId: deviceInfo.DeviceGroup.DeviceGroupId,
		}, all))
		if err != nil {
			http.logger.Error("error tapping function service address uplink error", zap.Error(err))
		}
	}
}

func (http *Protocol) GetProtocolType() core.ProtocolType {
	return core.HttpClient
}

func (http *Protocol) GetProtocolId() core.ProtocolId {
	return http.ProtocolId
}
