package register

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"time"
	"zingthings/pkg/common"
	"zingthings/pkg/protocol/core"
	"zingthings/pkg/util/etcd"
	"zingthings/pkg/util/network"
)

type (
	EtcdRegister struct {
		etcdClient *clientv3.Client
		ctx        context.Context
		logger     *zap.Logger
		address    string
		nodeInfo   *core.NodeInfo
	}
)

func NewEtcdRegister(ctx context.Context, logger *zap.Logger, port int) *EtcdRegister {
	client := etcd.NewClient(ctx, logger)
	nodeInfo := getCurrentNodeInfo(port)
	address := nodeInfo.BuildAddress()
	return &EtcdRegister{
		etcdClient: client,
		ctx:        ctx,
		logger:     logger.Named("etcd-register"),
		address:    address,
		nodeInfo:   nodeInfo,
	}
}

func getCurrentNodeInfo(port int) *core.NodeInfo {
	ip, err2 := network.GetLocalIP()
	if err2 != nil {
		panic(err2)
	}
	nodeInfo := &core.NodeInfo{Host: ip, Port: port, Timestamp: time.Now().UnixMilli()}
	return nodeInfo
}

func (etcdReg *EtcdRegister) Stop() {
	etcdReg.etcdClient.Close()
}

func (etcdReg *EtcdRegister) Start() {
	ctx, cancelFunc := context.WithCancel(etcdReg.ctx)
	grant, err := etcdReg.etcdClient.Grant(etcdReg.ctx, 10)
	if err != nil {
		etcdReg.logger.Warn("put error", zap.Error(err))
		cancelFunc()
		return
	}
	go func() {
		defer cancelFunc()
		ticker := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				queue, err2 := etcdReg.etcdClient.KeepAlive(etcdReg.ctx, grant.ID)
				if err2 != nil {
					etcdReg.logger.Warn("keep alive error", zap.Error(err2))
					return
				}
				go func() {
					cancel, c := context.WithCancel(etcdReg.ctx)
					defer c()
					for {
						select {
						case <-cancel.Done():
							return
						case responseA := <-queue:
							if responseA != nil && responseA.TTL == 0 {
								return
							}
						}
					}
				}()
			}
		}
	}()
	marshal, _ := json.Marshal(etcdReg.nodeInfo)
	_, err5 := etcdReg.etcdClient.Put(etcdReg.ctx, etcdReg.nodeInfo.BuildKey(), string(marshal), clientv3.WithLease(grant.ID))
	if err5 != nil {
		etcdReg.logger.Warn("put error", zap.Error(err5))
	}
	core.DefaultEventBus.Subscribe(common.ProtocolDeploySuccess, func(event core.Event) {
		if protocol, ok := event.(core.Protocol); ok {
			id := string(protocol.GetProtocolId())
			protocolType := string(protocol.GetProtocolType())
			info := &core.ProtocolInfo{Id: core.ProtocolId(id), Address: etcdReg.address,
				ProtocolType: core.ProtocolType(protocolType),
				Timestamp:    time.Now().UnixMilli()}
			infoString, _ := json.Marshal(info)

			protocolNodeKey := info.BuildNodeKey()
			_, err4 := etcdReg.etcdClient.Put(etcdReg.ctx, protocolNodeKey, string(infoString))
			if err4 != nil {
				etcdReg.logger.Warn("put error", zap.String("protocolNodeKey", protocolNodeKey), zap.Error(err4))
				return
			}
			if protocolWrapper, okD := event.(*core.ProtocolWrapper); okD {
				infoDetail := &core.ProtocolInfoDetail{DeviceGroup: protocolWrapper.DeviceGroup,
					DeviceInfos: protocolWrapper.DeviceInfos}
				infoStringData, _ := json.Marshal(infoDetail)
				_, err6 := etcdReg.etcdClient.Put(etcdReg.ctx, info.BuildInfoKey(), string(infoStringData))
				if err6 != nil {
					etcdReg.logger.Warn("put error", zap.Error(err6))
					return
				}
			}

			protocolKey := info.BuildKey()
			_, err3 := etcdReg.etcdClient.Put(etcdReg.ctx, protocolKey, string(infoString), clientv3.WithLease(grant.ID))
			if err3 != nil {
				etcdReg.logger.Warn("put error", zap.Error(err3))
				return
			}

		}
	})
	core.DefaultEventBus.Subscribe(common.ProtocolUnDeploySuccess, func(event core.Event) {
		if protocol, ok := event.(core.Protocol); ok {
			id := string(protocol.GetProtocolId())
			protocolType := string(protocol.GetProtocolType())
			info := &core.ProtocolInfo{Id: core.ProtocolId(id), ProtocolType: core.ProtocolType(protocolType), Address: etcdReg.address}
			protocolNodeKey := info.BuildNodeKey()
			_, err4 := etcdReg.etcdClient.Delete(etcdReg.ctx, protocolNodeKey)
			if err4 != nil {
				etcdReg.logger.Warn("put error", zap.String("protocolNodeKey", protocolNodeKey), zap.Error(err4))
				return
			}
			protocolKey := info.BuildKey()
			_, err3 := etcdReg.etcdClient.Delete(etcdReg.ctx, protocolKey)
			if err3 != nil {
				etcdReg.logger.Warn("put error", zap.Error(err3))
				return
			}

		}
	})
}

func (etcdReg *EtcdRegister) Register() {
	get, err := etcdReg.etcdClient.Get(etcdReg.ctx, "a")
	if err != nil {
		etcdReg.logger.Warn("failed to get a", zap.Error(err))
		return
	}
	if get.Count > 0 {
		for _, v := range get.Kvs {
			key := v.Key
			value := v.Value
			fmt.Println("key:", string(key), " value:", string(value))
		}
	}
}

func (etcdReg *EtcdRegister) GetNodeInfo() *core.NodeInfo {
	return etcdReg.nodeInfo
}

func (etcdReg *EtcdRegister) KeyExists(key string) bool {
	get, err := etcdReg.etcdClient.Get(etcdReg.ctx, key, clientv3.WithCountOnly())
	if err != nil {
		etcdReg.logger.Warn("failed to get key", zap.Error(err))
		return false
	}
	return get.Count > 0
}
