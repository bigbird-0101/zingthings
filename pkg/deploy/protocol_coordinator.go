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
	Coordinator struct {
		client           *clientv3.Client
		ctx              context.Context
		logger           *zap.Logger
		containerClient  *client.Client
		LoadBalance      LoadBalance
		currentNodeInfo  *core.NodeInfo
		allNodeInfos     []*core.NodeInfo
		allNodeInfoLock  *sync.RWMutex
		lock             sync.Locker
		loadBalanceIndex int
	}
)

func NewCoordinator(ctx context.Context, logger *zap.Logger, nodeInfo *core.NodeInfo) *Coordinator {
	newClient := etcd.NewClient(ctx, logger)
	return &Coordinator{
		client:           newClient,
		ctx:              ctx,
		logger:           logger.Named("coordinator"),
		currentNodeInfo:  nodeInfo,
		allNodeInfos:     make([]*core.NodeInfo, 0),
		allNodeInfoLock:  &sync.RWMutex{},
		lock:             &sync.Mutex{},
		loadBalanceIndex: 0,
	}
}

func (coordinator *Coordinator) Start() error {
	group := &sync.WaitGroup{}
	group.Add(1)
	go coordinator.register(group)
	go coordinator.fault(group)
	return nil
}

func (coordinator *Coordinator) register(group *sync.WaitGroup) {
	defer group.Done()
	marshal, err := json.Marshal(coordinator.currentNodeInfo)
	if err != nil {
		panic(err)
	}
	grant, err := coordinator.client.Grant(coordinator.ctx, 10)
	if err != nil {
		panic(err)
	}
	_, err = coordinator.client.Put(coordinator.ctx, coordinator.currentNodeInfo.BuildDeployKey(), string(marshal), clientv3.WithLease(grant.ID))
	if err != nil {
		panic(err)
	}
	go func() {
		ctx, cancelFunc := context.WithCancel(coordinator.ctx)
		defer cancelFunc()
		defer coordinator.client.Revoke(ctx, grant.ID)
		ticker := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				coordinator.logger.Info("keep alive cancel")
				return
			case <-ticker.C:
				response, err2 := coordinator.client.KeepAlive(ctx, grant.ID)
				if err2 != nil {
					coordinator.logger.Error("keep alive", zap.Error(err2))
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
		get, err2 := coordinator.client.Get(coordinator.ctx, common.CompleteDeploy, clientv3.WithPrefix())
		if err2 != nil {
			coordinator.logger.Error("get", zap.Error(err2))
			return
		}
		if get.Count > 0 {
			for _, kv := range get.Kvs {
				nodeInfo := &core.NodeInfo{}
				err2 = json.Unmarshal(kv.Value, nodeInfo)
				if err2 != nil {
					continue
				}
				coordinator.allNodeInfos = append(coordinator.allNodeInfos, nodeInfo)
				coordinator.logger.Info("get deploy node", zap.Any("nodeInfo-len", len(coordinator.allNodeInfos)))
			}
		}
	}()

	go func() {
		for {
			getResponse, err2 := coordinator.client.Get(coordinator.ctx, common.CompleteRecover, clientv3.WithPrefix())
			if err2 != nil {
				coordinator.logger.Error("get recover ", zap.Error(err2))
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
					_, err3 = coordinator.client.Put(coordinator.ctx, nodeInfo.BuildRecoverKey(), string(newNodeBytes))
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
		getResponse, err2 := coordinator.client.Get(coordinator.ctx, common.CompleteProtocolNode, clientv3.WithPrefix())
		if err2 != nil {
			coordinator.logger.Error("get protocol ", zap.Error(err2))
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
					_, err3 = coordinator.client.Put(coordinator.ctx, n.BuildRecoverKey(), string(bytesResult))
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
		ctx, cancelFunc := context.WithCancel(coordinator.ctx)
		defer cancelFunc()
		deployNode := coordinator.client.Watch(ctx, common.CompleteDeploy, clientv3.WithPrefix(), clientv3.WithPrevKV())
		for {
			select {
			case <-ctx.Done():
				coordinator.logger.Info("watch deploy node done")
				return
			case deployInfo := <-deployNode:
				coordinator.logger.Info("watch deploy node info")
				coordinator.allNodeInfoLock.Lock()
				for _, event := range deployInfo.Events {
					switch event.Type {
					case mvccpb.PUT:
						coordinator.logger.Info("put deploy node")
						nodeInfo := &core.NodeInfo{}
						err2 := json.Unmarshal(event.Kv.Value, nodeInfo)
						if err2 != nil {
							continue
						}
						coordinator.logger.Info("put deploy node", zap.Any("nodeInfo", nodeInfo))
						alreadyExists := false
						for _, info := range coordinator.allNodeInfos {
							if info.Host == nodeInfo.Host && info.Port == nodeInfo.Port {
								alreadyExists = true
								info.Timestamp = nodeInfo.Timestamp
								break
							} else {
								alreadyExists = false
							}
						}
						if !alreadyExists {
							coordinator.allNodeInfos = append(coordinator.allNodeInfos, nodeInfo)
							coordinator.logger.Info("add deploy node", zap.Any("nodeInfo-len", len(coordinator.allNodeInfos)))
						}
					case mvccpb.DELETE:
						nodeInfo := &core.NodeInfo{}
						err2 := json.Unmarshal(event.PrevKv.Value, nodeInfo)
						if err2 != nil {
							continue
						}
						allNodeInfosNew := make([]*core.NodeInfo, 0)
						for _, info := range coordinator.allNodeInfos {
							if info.Host != nodeInfo.Host || info.Port != nodeInfo.Port {
								allNodeInfosNew = append(allNodeInfosNew, info)
							}
						}
						coordinator.allNodeInfos = allNodeInfosNew
					}
				}
				coordinator.allNodeInfoLock.Unlock()
				coordinator.logger.Info("deploy update done", zap.Any("nodeInfo-len", len(coordinator.allNodeInfos)))
			}
		}
	}()
}

func (coordinator *Coordinator) fault(group *sync.WaitGroup) {
	group.Wait()
	ctx, cancelFunc := context.WithCancel(coordinator.ctx)
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
							coordinator.logger.Error("unmarshal nodeInfoValue", zap.Error(err))
							continue
						}
						get, err := coordinator.client.Get(coordinator.ctx, common.CompleteRecover, clientv3.WithPrefix())
						if err != nil {
							coordinator.logger.Error("get nodeInfo", zap.Error(err))
							continue
						}
						if get.Count > 0 {
							for _, dv := range get.Kvs {
								needRecoverNode := &core.NodeInfo{}
								needRecoverNode.Timestamp = time.Now().UnixMilli()
								err7 := json.Unmarshal(dv.Value, needRecoverNode)
								if err7 != nil {
									coordinator.logger.Error("unmarshal nodeInfo", zap.Error(err7))
									continue
								}
								//触发容错恢复
								marshal, _ := json.Marshal(needRecoverNode)
								_, _ = coordinator.client.Put(coordinator.ctx, needRecoverNode.BuildRecoverKey(), string(marshal))
							}
						}
					case mvccpb.DELETE:
						//如果节点被删除 或停掉则 将 里面的正在运行的协议调度到其他节点
						nodeInfoValue := &core.NodeInfo{}
						err := json.Unmarshal(v.PrevKv.Value, nodeInfoValue)
						if err != nil {
							coordinator.logger.Error("unmarshal nodeInfoValue", zap.Error(err))
							continue
						}
						key := nodeInfoValue.BuildProtocolKey()
						get, err := coordinator.client.Get(coordinator.ctx, key, clientv3.WithPrefix())
						if err != nil {
							coordinator.logger.Error("get nodeInfo", zap.Error(err))
							continue
						}
						if get.Count > 0 {
							nodeInfoValue.Timestamp = time.Now().UnixMilli()
							marshal, _ := json.Marshal(nodeInfoValue)
							_, err8 := coordinator.client.Put(coordinator.ctx, nodeInfoValue.BuildRecoverKey(), string(marshal))
							if err8 != nil {
								coordinator.logger.Error("put nodeInfo", zap.Error(err))
								return
							}
						}
					}
				}
			case protocolInfo := <-protocol:
				coordinator.allNodeInfoLock.RLock()
				for _, v := range protocolInfo.Events {
					switch v.Type {
					case mvccpb.DELETE:
						protocolInfoValue := &core.ProtocolInfo{}
						err := json.Unmarshal(v.PrevKv.Value, protocolInfoValue)
						if err != nil {
							coordinator.logger.Error("unmarshal ProtocolInfo", zap.Error(err))
							continue
						}
						key := protocolInfoValue.BuildNodeKey()
						get, err := coordinator.client.Get(coordinator.ctx, key, clientv3.WithCountOnly())
						if err != nil {
							coordinator.logger.Error("get", zap.Error(err))
							continue
						}
						if get.Count > 0 {
							allNodeCount := len(coordinator.allNodeInfos)
							hashCode := stringToHashCode(string(protocolInfoValue.Id))
							currentSlot := coordinator.getCurrentSlot()
							if allNodeCount == 0 {
								continue
							}
							u := hashCode % uint64(allNodeCount)
							coordinator.logger.Info("need dispatch", zap.String("protocolInfo",
								string(protocolInfoValue.Id)), zap.Int("current slot", currentSlot),
								zap.Int("allNodeCount", allNodeCount), zap.Any("计算后的值", u))
							if u != uint64(currentSlot) {
								coordinator.logger.Info("not need dispatch not current slot", zap.Int("currentSlot", currentSlot))
								continue
							} else {
								coordinator.dispatchToAlive(protocolInfoValue)
							}
						}
					}
				}
				coordinator.allNodeInfoLock.RUnlock()
			case recoverNodeInfo := <-recoverNode:
				coordinator.logger.Info("start fault recoverNodeInfo")
				coordinator.allNodeInfoLock.RLock()
				for _, v := range recoverNodeInfo.Events {
					switch v.Type {
					case mvccpb.PUT:
						//如果节点被删除 或停掉则 将 里面的正在运行的协议调度到其他节点
						nodeInfoValue := &core.NodeInfo{}
						err := json.Unmarshal(v.Kv.Value, nodeInfoValue)
						if err != nil {
							coordinator.logger.Error("unmarshal nodeInfoValue", zap.Error(err))
							continue
						}
						coordinator.logger.Info("start deploy recover", zap.String("nodeInfo", nodeInfoValue.BuildAddress()))
						oneProtocol := coordinator.getScheduleOneProtocol(nodeInfoValue)
						if nil != oneProtocol {
							if _, ok := coordinator.dispatchToAlive(oneProtocol); !ok {
								coordinator.logger.Warn("protocol dispatch failed")
							}
						} else {
							_, _ = coordinator.client.Delete(coordinator.ctx, string(v.Kv.Key))
						}
					}
				}
				coordinator.allNodeInfoLock.RUnlock()
				coordinator.logger.Info("fault update done", zap.Any("nodeInfo-len", len(coordinator.allNodeInfos)))
			}
		}
	}()
}

func (coordinator *Coordinator) getScheduleOneProtocol(nodeInfoValue *core.NodeInfo) *core.ProtocolInfo {
	allNodeCount := len(coordinator.allNodeInfos)
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
				coordinator.logger.Error("unmarshal nodeInfo", zap.Error(err7))
				continue
			}
			//如果不是当前节点调度的数据就跳过
			hashCode := stringToHashCode(string(protocolInfo.Id))
			currentSlot := coordinator.getCurrentSlot()
			u := hashCode % uint64(allNodeCount)
			coordinator.logger.Info("need dispatch", zap.String("protocolInfo",
				string(protocolInfo.Id)), zap.Int("current slot", currentSlot),
				zap.Int("allNodeCount", allNodeCount), zap.Any("计算后的值", u))
			if u != uint64(currentSlot) {
				coordinator.logger.Info("not need dispatch not current slot", zap.Int("currentSlot", currentSlot))
				continue
			}
			return protocolInfo
		}
	}
	return nil
}

func (coordinator *Coordinator) buildNeedDispatchProtocol(nodeInfoValue *core.NodeInfo) []*mvccpb.KeyValue {
	needDispatch := make([]*mvccpb.KeyValue, 0)
	key := nodeInfoValue.BuildProtocolKey()
	allNodeCount := len(coordinator.allNodeInfos)
	response, _ := coordinator.client.Get(coordinator.ctx, key, clientv3.WithPrefix())
	if response.Count > 0 {
		for _, dv := range response.Kvs {
			if len(needDispatch) == allNodeCount {
				break
			}
			protocolInfo := &core.ProtocolInfo{}
			err7 := json.Unmarshal(dv.Value, protocolInfo)
			if err7 != nil {
				coordinator.logger.Error("unmarshal nodeInfo", zap.Error(err7))
				continue
			}
			responseTemp, err7 := coordinator.client.Get(coordinator.ctx, protocolInfo.BuildKey(), clientv3.WithCountOnly())
			if err7 != nil {
				coordinator.logger.Error("get nodeInfo", zap.Error(err7))
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
	for i, v := range coordinator.allNodeInfos {
		if v.Host == coordinator.currentNodeInfo.Host && v.Port == coordinator.currentNodeInfo.Port {
			return i
		}
	}
	return 0
}

func (coordinator *Coordinator) checkProtocolAlreadyAlive(protocolInfoValue *core.ProtocolInfo) (*core.NodeInfo, bool) {
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
		get, err := coordinator.client.Get(coordinator.ctx, buildKey, clientv3.WithCountOnly())
		if err != nil {
			return nil, false
		}
		if get.Count > 0 {
			return nil, true
		}
	}
	return nil, false
}

func (coordinator *Coordinator) dispatchToAlive(protocolInfoValue *core.ProtocolInfo) (*core.NodeInfo, bool) {
	alive := coordinator.getAlive()
	if nil == alive {
		coordinator.logger.Error("not alive node to dispatch", zap.Any("protocolInfo", protocolInfoValue))
		return nil, false
	}
	makeClient := client.MakeClient(coordinator.logger, fmt.Sprintf("http://%s:%d", alive.Host, alive.Port))
	if protocolAlreadyAlive, alreadyAlive := coordinator.checkProtocolAlreadyAlive(protocolInfoValue); alreadyAlive {
		coordinator.doRemoveGarbageData(protocolInfoValue, alive)
		return protocolAlreadyAlive, alreadyAlive
	}
	count := 0
	for {
		getResponse, err := coordinator.client.Get(coordinator.ctx, protocolInfoValue.BuildInfoKey())
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
			coordinator.logger.Info("dispatchToAlive response", zap.Any("response", deploy))
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
			coordinator.logger.Info("dispatchToAlive success", zap.Any("protocolInfo", protocolInfoValue))
			return alive, true
		} else {
			count++
			continue
		}
	}
}

func (coordinator *Coordinator) doRemoveGarbageData(protocolInfoValue *core.ProtocolInfo, alive *core.NodeInfo) (*core.NodeInfo, bool, bool) {
	if alive.BuildAddress() != protocolInfoValue.Address {
		_, err7 := coordinator.client.Delete(coordinator.ctx, protocolInfoValue.BuildNodeKey())
		if err7 != nil {
			return alive, false, true
		}
		//如果原来的那个节点存活那就 卸载原来的protocol
		get, err6 := coordinator.client.Get(coordinator.ctx, protocolInfoValue.BuildAddressKey(), clientv3.WithCountOnly())
		if err6 != nil {
			return alive, false, true
		}
		if get.Count > 0 {
			makeClient := client.MakeClient(coordinator.logger, fmt.Sprintf("http://%s", protocolInfoValue.Address))
			response, err8 := makeClient.UnDeploy(&core.UnDeployRequest{
				ProtocolId:   protocolInfoValue.Id,
				ProtocolType: protocolInfoValue.ProtocolType,
			})
			coordinator.logger.Info("dispatchToAlive undeploy old node response", zap.Any("response", response))
			if err8 != nil {
				return alive, false, true
			}
		}
	}
	return nil, false, false
}

func (coordinator *Coordinator) getAlive() *core.NodeInfo {
	return coordinator.selectNode(coordinator.getAllAlive())
}

func (coordinator *Coordinator) next(nodes []*core.NodeInfo) *core.NodeInfo {
	coordinator.lock.Lock()
	defer coordinator.lock.Unlock()

	if len(nodes) == 0 {
		return nil // 或者你可以选择返回一个错误
	}
	if coordinator.loadBalanceIndex >= len(nodes) {
		coordinator.loadBalanceIndex = 0
	}
	current := nodes[coordinator.loadBalanceIndex]
	coordinator.loadBalanceIndex = (coordinator.loadBalanceIndex + 1) % len(nodes)
	return current
}

func (coordinator *Coordinator) selectNode(nodeInfos []*core.NodeInfo) *core.NodeInfo {
	if coordinator.LoadBalance == nil {
		return coordinator.next(nodeInfos)
	} else {
		return coordinator.LoadBalance.selectNode(nodeInfos)
	}
}

func (coordinator *Coordinator) getAllAlive() []*core.NodeInfo {
	get, err := coordinator.client.Get(coordinator.ctx, common.CompletePath, clientv3.WithPrefix())
	if err != nil {
		coordinator.logger.Error("get alive failed", zap.Error(err))
		return nil
	}
	result := make([]*core.NodeInfo, 0)
	for _, v := range get.Kvs {
		nodeInfo := new(core.NodeInfo)
		errTemp := json.Unmarshal(v.Value, nodeInfo)
		if errTemp != nil {
			coordinator.logger.Error("unmarshal failed", zap.Error(errTemp))
			continue
		}
		result = append(result, nodeInfo)
	}
	return result
}
