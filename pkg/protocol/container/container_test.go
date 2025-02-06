package container

import (
	"encoding/json"
	"fmt"
	"testing"
	"zingthings/pkg/protocol/core"
	"zingthings/pkg/util/loggerfactory"
)

func TestDeploy_Deploy(t *testing.T) {
	common := core.DefaultProtocolManagerCommon
	deploy := &Container{
		ProtocolManager: core.DefaultProtocolManagerCommon,
		logger:          loggerfactory.GetLogger(),
	}
	mapTemp := make(map[string][]*core.DeviceInfo)
	key := core.DeviceGroup{
		DeviceGroupId:   "testDeviceGroupId",
		DeviceGroupName: "testDeviceGroupName",
	}
	mapTemp["testDeviceGroupId"] = []*core.DeviceInfo{{
		DeviceId:    "testDeviceGroupId",
		DeviceName:  "testDeviceGroupName",
		DeviceGroup: key,
	}}
	request := &Request{
		ProtocolId:    "test",
		ProtocolType:  core.HttpClient,
		Properties:    make(map[string]interface{}),
		DeviceInfoMap: mapTemp,
	}
	err := deploy.Deploy(request)
	if err != nil {
		t.Errorf("err=%v", err)
	}
	get := common.Get("test" + "/testDeviceGroupId")
	if get == nil {
		t.Error("get=nil")
	}
	marshal, err := json.Marshal(request)
	fmt.Println(string(marshal), err)
}
