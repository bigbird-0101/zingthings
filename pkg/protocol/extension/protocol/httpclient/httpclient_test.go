package httpclient

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"zingthings/pkg/protocol/core"
	"zingthings/pkg/util/loggerfactory"
)

type (
	TestChannelHandler struct {
		core.SimplePredicateChannelHandler
		call bool
	}
)

func TestHttpClientProtocol(t *testing.T) {
	genericProtocol := core.NewGenericProtocol()
	genericProtocol.ProtocolId = "testProtocolId"
	genericProtocol.DeviceGroup = core.DeviceGroup{
		DeviceGroupId:   "testDeviceGroupId",
		DeviceGroupName: "testDeviceGroupName",
	}
	genericProtocol.DeviceInfos = []*core.DeviceInfo{
		{
			DeviceId:   "testDeviceId",
			DeviceName: "testDeviceName",
		},
	}
	protocol := NewHttpClientProtocol(loggerfactory.GetLogger(), genericProtocol)

	handler := &TestChannelHandler{
		call: false,
	}
	deal := func(context core.ChannelHandlerContext, message *core.Message) {
		handler.call = true
	}
	handler.PredicateDown = func(message *core.Message) bool {
		return true
	}
	handler.PredicateUp = func(message *core.Message) bool {
		return true
	}
	handler.DealUp = deal
	handler.DealDown = deal
	err12 := protocol.ChannelHandlerPipeline.AddFirst("testHttpDownChannelHandler", handler)
	if err12 != nil {
		t.Error(err12)
	}
	context := core.NodeContext{
		Properties: make(map[string]interface{}),
	}
	protocol.Start(&context)

	uplink := core.NewUpLink()
	err1 := uplink.UpLink(&core.Message{
		Header: core.Header{
			DeviceId:      "testDeviceId",
			DeviceGroupId: "testDeviceGroupId",
			ProtocolType:  core.HttpClient,
		},
		Data: []byte{1},
	})
	if err1 != nil {
		t.Error(err1)
	}
	downLink := core.NewDownLink()
	err := downLink.DownLink(&core.Message{
		Header: core.Header{
			DeviceId:      "testDeviceId",
			DeviceGroupId: "testDeviceGroupId",
			ProtocolType:  core.HttpClient,
		},
		Data: []byte{1},
	})
	if err != nil {
		t.Error(err)
	}
	if !handler.call {
		t.Error("TestDownChannelHandler failed")
	}
}

func TestExecute(t *testing.T) {
	// 创建一个虚拟的 HTTP 服务器
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{\"data\":\"success\"}"))
	}))
	defer ts.Close()
	genericProtocol := core.NewGenericProtocol()
	genericProtocol.ProtocolId = "testProtocolId"
	genericProtocol.DeviceGroup = core.DeviceGroup{
		DeviceGroupId:   "testGroupId",
		DeviceGroupName: "testGroupName",
	}
	properties := make(map[string]interface{})
	properties["executionTimeInterval"] = 5000
	httpRequest := &HttpRequest{
		METHOD: "GET",
		PATH:   "/m1/4599062-4248632-default/data-security/datasource/getAllList",
	}
	marshal, _ := json.Marshal(httpRequest)
	httpArray := []string{string(marshal)}
	properties["httpRequests"] = httpArray
	genericProtocol.DeviceInfos = []*core.DeviceInfo{
		{
			DeviceId:    "testDeviceId",
			DeviceName:  "testDeviceName",
			DeviceGroup: genericProtocol.DeviceGroup,
			Properties:  properties,
		},
	}
	protocol := NewHttpClientProtocol(loggerfactory.GetLogger(), genericProtocol)
	handler := &TestChannelHandler{call: false}
	c := make(chan *core.Message, 1)
	deal := func(context core.ChannelHandlerContext, message *core.Message) {
		handler.call = true
		c <- message
	}
	err12 := protocol.ChannelHandlerPipeline.AddFirst("testHttpDownChannelHandler", core.NewSimpleAllChannelHandler(deal, deal))
	if err12 != nil {
		t.Error(err12)
	}
	p := make(map[string]interface{})
	p["baseUrl"] = ts.URL
	context := core.NodeContext{
		Properties: p,
	}
	protocol.Start(&context)
	err := protocol.Execute(map[string]interface{}{"test": "test"})
	if err != nil {
		t.Error(err)
	}
	message := <-c
	if !handler.call {
		t.Error("TestDownChannelHandler failed")
	}
	if message.Data == nil {
		t.Error("TestDownChannelHandler failed")
	}
	if string(message.Data.([]byte)) != "{\"data\":\"success\"}" {
		t.Error("TestDownChannelHandler failed")
	}
}
