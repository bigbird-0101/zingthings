package deploy

import (
	"context"
	"encoding/json"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"sync"
	"time"
	"zingthings/pkg/common"
	"zingthings/pkg/protocol/core"
)

type (
	CoordinatorWithDB struct {
		Coordinator
		DB         *gorm.DB
		etcdClient *clientv3.Client
	}
)

func NewCoordinatorWithDB(ctx context.Context, logger *zap.Logger, nodeInfo *core.NodeInfo, db *gorm.DB) *CoordinatorWithDB {
	coordinator := Coordinator{
		AllNodeInfoLock:  &sync.RWMutex{},
		AllNodeInfos:     make([]*core.NodeInfo, 0),
		Ctx:              ctx,
		CurrentNodeInfo:  nodeInfo,
		LoadBalanceIndex: 0,
		Lock:             &sync.Mutex{},
		Logger:           logger.Named("Coordinator"),
	}
	return &CoordinatorWithDB{
		Coordinator: coordinator,
		DB:          db,
	}
}

func (coordinator *CoordinatorWithDB) Start() error {
	go coordinator.register()
	go coordinator.fault()
	return nil
}

func (coordinator *CoordinatorWithDB) register() {
	marshal, err := json.Marshal(coordinator.CurrentNodeInfo)
	if err != nil {
		panic(err)
	}
	grant, err := coordinator.etcdClient.Grant(coordinator.Ctx, 10)
	if err != nil {
		panic(err)
	}
	_, err = coordinator.etcdClient.Put(coordinator.Ctx, coordinator.CurrentNodeInfo.BuildDeployKey(), string(marshal), clientv3.WithLease(grant.ID))
	if err != nil {
		return
	}
	go func() {
		ctx, cancelFunc := context.WithCancel(coordinator.Ctx)
		defer cancelFunc()
		defer coordinator.etcdClient.Revoke(ctx, grant.ID)
		ticker := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				coordinator.Logger.Info("keep alive cancel")
				return
			case <-ticker.C:
				response, err2 := coordinator.etcdClient.KeepAlive(ctx, grant.ID)
				if err2 != nil {
					coordinator.Logger.Error("keep alive", zap.Error(err2))
					return
				}
				go func() {
					cancel, c := context.WithCancel(ctx)
					defer c()
					for {
						select {
						case <-cancel.Done():
							return
						case responseA := <-response:
							if responseA != nil && responseA.TTL == 0 {
								return
							}
						}
					}
				}()
			}
		}
	}()

	go func() {
		ctx, cancelFunc := context.WithCancel(coordinator.Ctx)
		defer cancelFunc()
		deployNode := coordinator.etcdClient.Watch(ctx, common.CompleteDeploy, clientv3.WithPrefix(), clientv3.WithPrevKV())
		for {
			select {
			case <-ctx.Done():
				coordinator.Logger.Info("watch deploy node done")
				return
			case deployInfo := <-deployNode:
				coordinator.Logger.Info("watch deploy node info")
				coordinator.AllNodeInfoLock.Lock()
				for _, event := range deployInfo.Events {
					switch event.Type {
					case mvccpb.PUT:
						coordinator.Logger.Info("put deploy node")
						nodeInfo := &core.NodeInfo{}
						err2 := json.Unmarshal(event.Kv.Value, nodeInfo)
						if err2 != nil {
							continue
						}
						coordinator.Logger.Info("put deploy node", zap.Any("nodeInfo", nodeInfo))
						alreadyExists := false
						for _, info := range coordinator.AllNodeInfos {
							if info.Host == nodeInfo.Host && info.Port == nodeInfo.Port {
								alreadyExists = true
								info.Timestamp = nodeInfo.Timestamp
								break
							} else {
								alreadyExists = false
							}
						}
						if !alreadyExists {
							coordinator.AllNodeInfos = append(coordinator.AllNodeInfos, nodeInfo)
							coordinator.Logger.Info("add deploy node", zap.Any("nodeInfo-len", len(coordinator.AllNodeInfos)))
						}
					case mvccpb.DELETE:
						nodeInfo := &core.NodeInfo{}
						err2 := json.Unmarshal(event.PrevKv.Value, nodeInfo)
						if err2 != nil {
							continue
						}
						allNodeInfosNew := make([]*core.NodeInfo, 0)
						for _, info := range coordinator.AllNodeInfos {
							if info.Host != nodeInfo.Host || info.Port != nodeInfo.Port {
								allNodeInfosNew = append(allNodeInfosNew, info)
							}
						}
						coordinator.AllNodeInfos = allNodeInfosNew
					}
				}
				coordinator.AllNodeInfoLock.Unlock()
				coordinator.Logger.Info("deploy update done", zap.Any("nodeInfo-len", len(coordinator.AllNodeInfos)))
			}
		}
	}()
}

func (coordinator *CoordinatorWithDB) fault() {

}
