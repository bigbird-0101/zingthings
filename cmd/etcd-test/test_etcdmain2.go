package main

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"zingthings/pkg/util/etcd"
	"zingthings/pkg/util/loggerfactory"
	"zingthings/pkg/util/signals"
)

func main() {
	logger := loggerfactory.GetLogger()
	ctx := signals.SetupSignalHandlerWithContext(logger)
	client := etcd.NewClient(ctx, logger)
	cancel, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	watchChan := client.Watch(cancel, "/test/key", clientv3.WithPrefix(), clientv3.WithPrevKV())
	for {
		select {
		case <-cancel.Done():
			return
		case resp := <-watchChan:
			for _, event := range resp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					logger.Info("put", zap.String("key", string(event.Kv.Key)), zap.String("value", string(event.Kv.Value)))
				case clientv3.EventTypeDelete:
					logger.Info("delete", zap.String("key", string(event.Kv.Key)))
				}
			}
		}
	}
}
