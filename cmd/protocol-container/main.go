package main

import (
	"zingthings/pkg/protocol/container"
	"zingthings/pkg/util/loggerfactory"
	"zingthings/pkg/util/signals"
)

func main() {
	logger := loggerfactory.GetLogger()
	ctx := signals.SetupSignalHandlerWithContext(logger)
	container.Server(ctx, logger)
	<-ctx.Done()
	logger.Error("exiting")
}
