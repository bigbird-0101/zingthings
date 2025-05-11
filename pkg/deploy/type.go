package deploy

import (
	"context"
	"go.uber.org/zap"
	"sync"
	"zingthings/pkg/protocol/core"
)

type (
	Coordinator struct {
		Ctx              context.Context
		Logger           *zap.Logger
		LoadBalance      LoadBalance
		CurrentNodeInfo  *core.NodeInfo
		AllNodeInfos     []*core.NodeInfo
		AllNodeInfoLock  *sync.RWMutex
		Lock             sync.Locker
		LoadBalanceIndex int
	}
)
