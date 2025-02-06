package signals

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

var onlyOneSignalHandler = make(chan struct{})

func SetupSignalHandlerWithContext(logger *zap.Logger) context.Context {
	var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	close(onlyOneSignalHandler) // panics when called twice
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		signal := <-c
		logger.Info("Received signal", zap.String("signal", signal.String()))
		cancel()
		<-c
		panic("multiple signals received")
	}()
	return ctx
}
