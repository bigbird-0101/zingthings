package events

import (
	"zingthings/pkg/common"
	"zingthings/pkg/protocol/core"
)

func InitEventListener() {
	core.DefaultEventBus.Subscribe(common.DeviceOnline, func(event core.Event) {

	})
	core.DefaultEventBus.Subscribe(common.DeviceOffline, func(event core.Event) {

	})
	core.DefaultEventBus.Subscribe(common.DeviceReportSuccess, func(event core.Event) {

	})
	core.DefaultEventBus.Subscribe(common.DeviceDownLinkSuccess, func(event core.Event) {

	})
	core.DefaultEventBus.Subscribe(common.DeviceReportFailed, func(event core.Event) {

	})
}
