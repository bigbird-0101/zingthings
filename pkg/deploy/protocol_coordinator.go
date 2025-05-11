package deploy

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"time"
	"zingthings/pkg/common"
	"zingthings/pkg/protocol/container/client"
	"zingthings/pkg/protocol/core"
	"zingthings/pkg/util/etcd"
	"zingthings/pkg/util/splice"
)

type (
	CoordinatorEtcd struct {
		Coordinator
		client *clientv3.Client
	}
)

func NewCoordinatorEtcd(ctx context.Context, logger *zap.Logger, nodeInfo *core.NodeInfo) *CoordinatorEtcd {
	newClient := etcd.NewClient(ctx, logger)
	coordinator := Coordinator{
		AllNodeInfoLock:  &sync.RWMutex{},
		AllNodeInfos:     make([]*core.NodeInfo, 0),
		Ctx:              ctx,
		CurrentNodeInfo:  nodeInfo,
		LoadBalanceIndex: 0,
		Lock:             &sync.Mutex{},
		Logger:           logger.Named("Coordinator"),
	}
	return &CoordinatorEtcd{
		Coordinator: coordinator,
		client:      newClient,
	}
}

func (coordinator *CoordinatorEtcd) Start() error {
	group := &sync.WaitGroup{}
	group.Add(1)
	go coordinator.register(group)
	go coordinator.fault(group)
	return nil
}

func (coordinator *CoordinatorEtcd) register(group *sync.WaitGroup) {
	defer group.Done()
	marshal, err := json.Marshal(coordinator.CurrentNodeInfo)
	if err != nil {
		panic(err)
	}
	grant, err := coordinator.client.Grant(coordinator.Ctx, 10)
	if err != nil {
		panic(err)
	}
	_, err = coordinator.client.Put(coordinator.Ctx, coordinator.CurrentNodeInfo.BuildDeployKey(), string(marshal), clientv3.WithLease(grant.ID))
	if err != nil {
		panic(err)
	}
	go func() {
		ctx, cancelFunc := context.WithCancel(coordinator.Ctx)
		defer cancelFunc()
		defer coordinator.client.Revoke(ctx, grant.ID)
		ticker := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				coordinator.Logger.Info("keep alive cancel")
				return
			case <-ticker.C:
				response, err2 := coordinator.client.KeepAlive(ctx, grant.ID)
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
	//获取所有节点
	go func() {
		get, err2 := coordinator.client.Get(coordinator.Ctx, common.CompleteDeploy, clientv3.WithPrefix())
		if err2 != nil {
			coordinator.Logger.Error("get", zap.Error(err2))
			return
		}
		if get.Count > 0 {
			for _, kv := range get.Kvs {
				nodeInfo := &core.NodeInfo{}
				err2 = json.Unmarshal(kv.Value, nodeInfo)
				if err2 != nil {
					continue
				}
				coordinator.AllNodeInfos = append(coordinator.AllNodeInfos, nodeInfo)
				coordinator.Logger.Info("get deploy node", zap.Any("nodeInfo-len", len(coordinator.AllNodeInfos)))
			}
		}
	}()
	go func() {
		for {
			getResponse, err2 := coordinator.client.Get(coordinator.Ctx, common.CompleteRecover, clientv3.WithPrefix())
			if err2 != nil {
				coordinator.Logger.Error("get recover ", zap.Error(err2))
				return
			}
			if getResponse.Count > 0 {
				for _, kv := range getResponse.Kvs {
					nodeInfo := &core.NodeInfo{}
					err2 = json.Unmarshal(kv.Value, nodeInfo)
					if err2 != nil {
						continue
					}
					nodeInfo.Timestamp = time.Now().UnixMilli()
					newNodeBytes, err3 := json.Marshal(nodeInfo)
					if err3 != nil {
						continue
					}
					_, err3 = coordinator.client.Put(coordinator.Ctx, nodeInfo.BuildRecoverKey(), string(newNodeBytes))
					if err3 != nil {
						continue
					}
				}
			}
			time.Sleep(5 * time.Second)
		}
	}()
	go func() {
		nodeInfosAlready := make([]string, 0)
		getResponse, err2 := coordinator.client.Get(coordinator.Ctx, common.CompleteProtocolNode, clientv3.WithPrefix())
		if err2 != nil {
			coordinator.Logger.Error("get protocol ", zap.Error(err2))
			return
		}
		if getResponse.Count > 0 {
			for _, kv := range getResponse.Kvs {
				protocolInfo := &core.ProtocolInfo{}
				err2 = json.Unmarshal(kv.Value, protocolInfo)
				if splice.Contains(nodeInfosAlready, protocolInfo.Address) {
					continue
				} else {
					split := strings.Split(protocolInfo.Address, ":")
					atoi, err4 := strconv.Atoi(split[1])
					if err4 != nil {
						continue
					}
					n := &core.NodeInfo{
						Host:      split[0],
						Port:      atoi,
						Timestamp: time.Now().UnixMilli(),
					}
					bytesResult, err3 := json.Marshal(n)
					if err3 != nil {
						continue
					}
					_, err3 = coordinator.client.Put(coordinator.Ctx, n.BuildRecoverKey(), string(bytesResult))
					if err3 != nil {
						continue
					}
					nodeInfosAlready = append(nodeInfosAlready, protocolInfo.Address)
				}
			}
		}
	}()
	//监听有哪些deploy节点 维护节点列表
	go func() {
		ctx, cancelFunc := context.WithCancel(coordinator.Ctx)
		defer cancelFunc()
		deployNode := coordinator.client.Watch(ctx, common.CompleteDeploy, clientv3.WithPrefix(), clientv3.WithPrevKV())
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

func (coordinator *CoordinatorEtcd) fault(group *sync.WaitGroup) {
	group.Wait()
	ctx, cancelFunc := context.WithCancel(coordinator.Ctx)
	container := coordinator.client.Watch(ctx, common.CompletePath, clientv3.WithPrefix(), clientv3.WithPrevKV())
	protocol := coordinator.client.Watch(ctx, common.CompleteProtocol, clientv3.WithPrefix(), clientv3.WithPrevKV())
	recoverNode := coordinator.client.Watch(ctx, common.CompleteRecover, clientv3.WithPrefix())
	go func() {
		defer cancelFunc()
		for {
			select {
			case <-ctx.Done():
				return
			case containerInfo := <-container:
				for _, v := range containerInfo.Events {
					switch v.Type {
					case mvccpb.PUT:
						//如果节点被删除 或停掉则 将 里面的正在运行的协议调度到其他节点
						nodeInfoValue := &core.NodeInfo{}
						err := json.Unmarshal(v.Kv.Value, nodeInfoValue)
						if err != nil {
							coordinator.Logger.Error("unmarshal nodeInfoValue", zap.Error(err))
							continue
						}
						get, err := coordinator.client.Get(coordinator.Ctx, common.CompleteRecover, clientv3.WithPrefix())
						if err != nil {
							coordinator.Logger.Error("get nodeInfo", zap.Error(err))
							continue
						}
						if get.Count > 0 {
							for _, dv := range get.Kvs {
								needRecoverNode := &core.NodeInfo{}
								needRecoverNode.Timestamp = time.Now().UnixMilli()
								err7 := json.Unmarshal(dv.Value, needRecoverNode)
								if err7 != nil {
									coordinator.Logger.Error("unmarshal nodeInfo", zap.Error(err7))
									continue
								}
								//触发容错恢复
								marshal, _ := json.Marshal(needRecoverNode)
								_, _ = coordinator.client.Put(coordinator.Ctx, needRecoverNode.BuildRecoverKey(), string(marshal))
							}
						}
					case mvccpb.DELETE:
						//如果节点被删除 或停掉则 将 里面的正在运行的协议调度到其他节点
						nodeInfoValue := &core.NodeInfo{}
						err := json.Unmarshal(v.PrevKv.Value, nodeInfoValue)
						if err != nil {
							coordinator.Logger.Error("unmarshal nodeInfoValue", zap.Error(err))
							continue
						}
						key := nodeInfoValue.BuildProtocolKey()
						get, err := coordinator.client.Get(coordinator.Ctx, key, clientv3.WithPrefix())
						if err != nil {
							coordinator.Logger.Error("get nodeInfo", zap.Error(err))
							continue
						}
						if get.Count > 0 {
							nodeInfoValue.Timestamp = time.Now().UnixMilli()
							marshal, _ := json.Marshal(nodeInfoValue)
							_, err8 := coordinator.client.Put(coordinator.Ctx, nodeInfoValue.BuildRecoverKey(), string(marshal))
							if err8 != nil {
								coordinator.Logger.Error("put nodeInfo", zap.Error(err))
								return
							}
						}
					}
				}
			case protocolInfo := <-protocol:
				coordinator.AllNodeInfoLock.RLock()
				for _, v := range protocolInfo.Events {
					switch v.Type {
					case mvccpb.DELETE:
						protocolInfoValue := &core.ProtocolInfo{}
						err := json.Unmarshal(v.PrevKv.Value, protocolInfoValue)
						if err != nil {
							coordinator.Logger.Error("unmarshal ProtocolInfo", zap.Error(err))
							continue
						}
						key := protocolInfoValue.BuildNodeKey()
						get, err := coordinator.client.Get(coordinator.Ctx, key, clientv3.WithCountOnly())
						if err != nil {
							coordinator.Logger.Error("get", zap.Error(err))
							continue
						}
						if get.Count > 0 {
							allNodeCount := len(coordinator.AllNodeInfos)
							hashCode := stringToHashCode(string(protocolInfoValue.Id))
							currentSlot := coordinator.getCurrentSlot()
							if allNodeCount == 0 {
								continue
							}
							u := hashCode % uint64(allNodeCount)
							coordinator.Logger.Info("need dispatch", zap.String("protocolInfo",
								string(protocolInfoValue.Id)), zap.Int("current slot", currentSlot),
								zap.Int("allNodeCount", allNodeCount), zap.Any("计算后的值", u))
							if u != uint64(currentSlot) {
								coordinator.Logger.Info("not need dispatch not current slot", zap.Int("currentSlot", currentSlot))
								continue
							} else {
								coordinator.dispatchToAlive(protocolInfoValue)
							}
						}
					}
				}
				coordinator.AllNodeInfoLock.RUnlock()
			case recoverNodeInfo := <-recoverNode:
				coordinator.Logger.Info("start fault recoverNodeInfo")
				coordinator.AllNodeInfoLock.RLock()
				for _, v := range recoverNodeInfo.Events {
					switch v.Type {
					case mvccpb.PUT:
						//如果节点被删除 或停掉则 将 里面的正在运行的协议调度到其他节点
						nodeInfoValue := &core.NodeInfo{}
						err := json.Unmarshal(v.Kv.Value, nodeInfoValue)
						if err != nil {
							coordinator.Logger.Error("unmarshal nodeInfoValue", zap.Error(err))
							continue
						}
						coordinator.Logger.Info("start deploy recover", zap.String("nodeInfo", nodeInfoValue.BuildAddress()))
						oneProtocol := coordinator.getScheduleOneProtocol(nodeInfoValue)
						if nil != oneProtocol {
							if _, ok := coordinator.dispatchToAlive(oneProtocol); !ok {
								coordinator.Logger.Warn("protocol dispatch failed")
							}
						} else {
							_, _ = coordinator.client.Delete(coordinator.Ctx, string(v.Kv.Key))
						}
					}
				}
				coordinator.AllNodeInfoLock.RUnlock()
				coordinator.Logger.Info("fault update done", zap.Any("nodeInfo-len", len(coordinator.AllNodeInfos)))
			}
		}
	}()
}

func (coordinator *CoordinatorEtcd) getScheduleOneProtocol(nodeInfoValue *core.NodeInfo) *core.ProtocolInfo {
	allNodeCount := len(coordinator.AllNodeInfos)
	if allNodeCount == 0 {
		return nil
	}
	needDispatch := make([]*mvccpb.KeyValue, 0)
	for _, value := range coordinator.buildNeedDispatchProtocol(nodeInfoValue) {
		needDispatch = append(needDispatch, value)
	}
	if len(needDispatch) > 0 {
		for _, dvd := range needDispatch {
			protocolInfo := &core.ProtocolInfo{}
			err7 := json.Unmarshal(dvd.Value, protocolInfo)
			if err7 != nil {
				coordinator.Logger.Error("unmarshal nodeInfo", zap.Error(err7))
				continue
			}
			//如果不是当前节点调度的数据就跳过
			hashCode := stringToHashCode(string(protocolInfo.Id))
			currentSlot := coordinator.getCurrentSlot()
			u := hashCode % uint64(allNodeCount)
			coordinator.Logger.Info("need dispatch", zap.String("protocolInfo",
				string(protocolInfo.Id)), zap.Int("current slot", currentSlot),
				zap.Int("allNodeCount", allNodeCount), zap.Any("计算后的值", u))
			if u != uint64(currentSlot) {
				coordinator.Logger.Info("not need dispatch not current slot", zap.Int("currentSlot", currentSlot))
				continue
			}
			return protocolInfo
		}
	}
	return nil
}

func (coordinator *CoordinatorEtcd) buildNeedDispatchProtocol(nodeInfoValue *core.NodeInfo) []*mvccpb.KeyValue {
	needDispatch := make([]*mvccpb.KeyValue, 0)
	key := nodeInfoValue.BuildProtocolKey()
	allNodeCount := len(coordinator.AllNodeInfos)
	response, _ := coordinator.client.Get(coordinator.Ctx, key, clientv3.WithPrefix())
	if response.Count > 0 {
		for _, dv := range response.Kvs {
			if len(needDispatch) == allNodeCount {
				break
			}
			protocolInfo := &core.ProtocolInfo{}
			err7 := json.Unmarshal(dv.Value, protocolInfo)
			if err7 != nil {
				coordinator.Logger.Error("unmarshal nodeInfo", zap.Error(err7))
				continue
			}
			responseTemp, err7 := coordinator.client.Get(coordinator.Ctx, protocolInfo.BuildKey(), clientv3.WithCountOnly())
			if err7 != nil {
				coordinator.Logger.Error("get nodeInfo", zap.Error(err7))
				continue
			}
			if len(needDispatch) == allNodeCount {
				return needDispatch
			}
			if responseTemp.Count == 0 {
				needDispatch = append(needDispatch, dv)
			}
		}
	}
	return needDispatch
}

func stringToHashCode(s string) uint64 {
	h := fnv.New64() // 使用 FNV-1a 算法
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0
	}
	return h.Sum64()
}

func (coordinator *Coordinator) getCurrentSlot() int {
	for i, v := range coordinator.AllNodeInfos {
		if v.Host == coordinator.CurrentNodeInfo.Host && v.Port == coordinator.CurrentNodeInfo.Port {
			return i
		}
	}
	return 0
}

func (coordinator *CoordinatorEtcd) checkProtocolAlreadyAlive(protocolInfoValue *core.ProtocolInfo) (*core.NodeInfo, bool) {
	alive := coordinator.getAllAlive()
	if nil == alive {
		return nil, false
	}
	for _, v := range alive {
		protocolInfoValueNew := &core.ProtocolInfo{}
		data, err := json.Marshal(protocolInfoValue)
		if err != nil {
			return nil, false
		}
		err = json.Unmarshal(data, protocolInfoValueNew)
		if err != nil {
			return nil, false
		}
		protocolInfoValueNew.Address = v.BuildAddress()
		buildKey := protocolInfoValueNew.BuildKey()
		get, err := coordinator.client.Get(coordinator.Ctx, buildKey, clientv3.WithCountOnly())
		if err != nil {
			return nil, false
		}
		if get.Count > 0 {
			return nil, true
		}
	}
	return nil, false
}

func (coordinator *CoordinatorEtcd) dispatchToAlive(protocolInfoValue *core.ProtocolInfo) (*core.NodeInfo, bool) {
	alive := coordinator.getAlive()
	if nil == alive {
		coordinator.Logger.Error("not alive node to dispatch", zap.Any("protocolInfo", protocolInfoValue))
		return nil, false
	}
	makeClient := client.MakeClient(coordinator.Logger, fmt.Sprintf("http://%s:%d", alive.Host, alive.Port))
	if protocolAlreadyAlive, alreadyAlive := coordinator.checkProtocolAlreadyAlive(protocolInfoValue); alreadyAlive {
		coordinator.doRemoveGarbageData(protocolInfoValue, alive)
		return protocolAlreadyAlive, alreadyAlive
	}
	count := 0
	for {
		getResponse, err := coordinator.client.Get(coordinator.Ctx, protocolInfoValue.BuildInfoKey())
		if err != nil {
			return nil, false
		}
		if getResponse.Count > 0 {
			keyValue := getResponse.Kvs[0]
			detail := &core.ProtocolInfoDetail{}
			err2 := json.Unmarshal(keyValue.Value, detail)
			if err2 != nil {
				return nil, false
			}
			deploy, err6 := makeClient.Deploy(&core.DeployRequest{
				ProtocolType: protocolInfoValue.ProtocolType,
				ProtocolId:   protocolInfoValue.Id,
				DeviceInfoMap: map[string][]*core.DeviceInfo{
					string(detail.DeviceGroup.DeviceGroupId): detail.DeviceInfos,
				},
			})
			if err6 != nil {
				return nil, false
			}
			coordinator.Logger.Info("dispatchToAlive response", zap.Any("response", deploy))
		} else {
			return nil, false
		}
		if count > 5 {
			return nil, false
		}
		//删除旧的key
		_, _, _ = coordinator.doRemoveGarbageData(protocolInfoValue, alive)
		protocolStatus, err := makeClient.GetProtocolStatus(protocolInfoValue)
		if err != nil {
			return nil, false
		}
		if protocolStatus == core.RUNNING {
			coordinator.Logger.Info("dispatchToAlive success", zap.Any("protocolInfo", protocolInfoValue))
			return alive, true
		} else {
			count++
			continue
		}
	}
}

func (coordinator *CoordinatorEtcd) doRemoveGarbageData(protocolInfoValue *core.ProtocolInfo, alive *core.NodeInfo) (*core.NodeInfo, bool, bool) {
	if alive.BuildAddress() != protocolInfoValue.Address {
		_, err7 := coordinator.client.Delete(coordinator.Ctx, protocolInfoValue.BuildNodeKey())
		if err7 != nil {
			return alive, false, true
		}
		//如果原来的那个节点存活那就 卸载原来的protocol
		get, err6 := coordinator.client.Get(coordinator.Ctx, protocolInfoValue.BuildAddressKey(), clientv3.WithCountOnly())
		if err6 != nil {
			return alive, false, true
		}
		if get.Count > 0 {
			makeClient := client.MakeClient(coordinator.Logger, fmt.Sprintf("http://%s", protocolInfoValue.Address))
			response, err8 := makeClient.UnDeploy(&core.UnDeployRequest{
				ProtocolId:   protocolInfoValue.Id,
				ProtocolType: protocolInfoValue.ProtocolType,
			})
			coordinator.Logger.Info("dispatchToAlive undeploy old node response", zap.Any("response", response))
			if err8 != nil {
				return alive, false, true
			}
		}
	}
	return nil, false, false
}

func (coordinator *CoordinatorEtcd) getAlive() *core.NodeInfo {
	return coordinator.selectNode(coordinator.getAllAlive())
}

func (coordinator *CoordinatorEtcd) next(nodes []*core.NodeInfo) *core.NodeInfo {
	coordinator.Lock.Lock()
	defer coordinator.Lock.Unlock()

	if len(nodes) == 0 {
		return nil // 或者你可以选择返回一个错误
	}
	if coordinator.LoadBalanceIndex >= len(nodes) {
		coordinator.LoadBalanceIndex = 0
	}
	current := nodes[coordinator.LoadBalanceIndex]
	coordinator.LoadBalanceIndex = (coordinator.LoadBalanceIndex + 1) % len(nodes)
	return current
}

func (coordinator *CoordinatorEtcd) selectNode(nodeInfos []*core.NodeInfo) *core.NodeInfo {
	if coordinator.LoadBalance == nil {
		return coordinator.next(nodeInfos)
	} else {
		return coordinator.LoadBalance.selectNode(nodeInfos)
	}
}

func (coordinator *CoordinatorEtcd) getAllAlive() []*core.NodeInfo {
	get, err := coordinator.client.Get(coordinator.Ctx, common.CompletePath, clientv3.WithPrefix())
	if err != nil {
		coordinator.Logger.Error("get alive failed", zap.Error(err))
		return nil
	}
	result := make([]*core.NodeInfo, 0)
	for _, v := range get.Kvs {
		nodeInfo := new(core.NodeInfo)
		errTemp := json.Unmarshal(v.Value, nodeInfo)
		if errTemp != nil {
			coordinator.Logger.Error("unmarshal failed", zap.Error(errTemp))
			continue
		}
		result = append(result, nodeInfo)
	}
	return result
}
