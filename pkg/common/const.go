package common

const (
	RootPath             = "/zingthings"
	Node                 = "/nodes"
	Deploy               = "/deploys"
	Recover              = "/recover"
	Normal               = "/normal"
	CompletePath         = RootPath + Node + Normal
	CompleteDeploy       = RootPath + Deploy
	CompleteRecover      = RootPath + Node + Recover
	CompleteProtocol     = RootPath + "/protocols"
	CompleteProtocolNode = CompleteProtocol + Node
	CompleteProtocolInfo = CompleteProtocol + "/info"
)

// 事件
const (
	DeviceOnline            = "DeviceOnline"
	DeviceOffline           = "DeviceOffline"
	DeviceReportSuccess     = "DeviceReportSuccess"
	DeviceDownLinkSuccess   = "DeviceDownLinkSuccess"
	DeviceReportFailed      = "DeviceReportFailed"
	ProtocolDeploySuccess   = "ProtocolDeploySuccess"
	ProtocolUnDeploySuccess = "ProtocolUnDeploySuccess"
	ProtocolDeployFailed    = "ProtocolDeployFailed"
)
