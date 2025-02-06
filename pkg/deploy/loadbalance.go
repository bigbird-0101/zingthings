package deploy

import (
	"zingthings/pkg/protocol/core"
)

type (
	LoadBalance interface {
		selectNode(nodeInfos []*core.NodeInfo) *core.NodeInfo
	}
)
