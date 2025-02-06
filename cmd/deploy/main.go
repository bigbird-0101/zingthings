package main

import (
	"zingthings/pkg/deploy"
	"zingthings/pkg/util/loggerfactory"
	"zingthings/pkg/util/signals"
)

func main() {
	logger := loggerfactory.GetLogger()
	ctx := signals.SetupSignalHandlerWithContext(logger)
	deploy.Server(ctx, logger)
	<-ctx.Done()
	logger.Error("exiting")
}
