package httpserver

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"zingthings/pkg/protocol/core"
	"zingthings/pkg/util/loggerfactory"
)

func TestPropertyReport(t *testing.T) {
	genericProtocol := core.NewGenericProtocol()
	genericProtocol.ProtocolId = "testProtocolId"
	genericProtocol.DeviceGroup = core.DeviceGroup{
		DeviceGroupId:   "testGroupId",
		DeviceGroupName: "testGroupName",
	}
	genericProtocol.DeviceInfos = []*core.DeviceInfo{
		{
			DeviceId:    "testDeviceId",
			DeviceName:  "testDeviceName",
			DeviceGroup: genericProtocol.DeviceGroup,
		},
	}
	exceptValue := "{\"a\":\"b\"}"
	upData := make(chan string, 1)
	protocol := NewHttpServerProtocol(loggerfactory.GetLogger(), genericProtocol)
	err := protocol.ChannelHandlerPipeline.AddFirst("httpServerUpDown", core.NewSimpleUpChannelHandler(
		func(context core.ChannelHandlerContext, message *core.Message) {
			upData <- string(message.Data.([]byte))
		}))
	if err != nil {
		return
	}
	protocol.Start(&core.NodeContext{})
	defer protocol.Stop()

	client := &http.Client{}
	resp, err := client.Post("http://localhost:"+strconv.Itoa(DefaultPort)+"/testGroupId/testDeviceId/propertyReport",
		"application/json", strings.NewReader(exceptValue))
	if err != nil {
		t.Error(err)
	}
	if resp.StatusCode != 200 {
		t.Error("response status code not 200")
	}
	result := <-upData
	if result != exceptValue {
		t.Error(result)
	}
	defer resp.Body.Close()
}
