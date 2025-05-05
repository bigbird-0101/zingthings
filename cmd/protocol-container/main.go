package main

import (
	"go.uber.org/zap"
	"zingthings/pkg/protocol/config"
	"zingthings/pkg/protocol/container"
	"zingthings/pkg/util/loggerfactory"
	"zingthings/pkg/util/signals"
)

func main() {
	logger := loggerfactory.GetLogger()
	ctx := signals.SetupSignalHandlerWithContext(logger)
	yamlConfig, err := config.LoadYAMLConfig("config/app.yaml")
	if err != nil {
		logger.Error("load config error", zap.Error(err))
		return
	}
	container.Server(ctx, logger, yamlConfig)
	<-ctx.Done()
	logger.Error("exiting")
}
