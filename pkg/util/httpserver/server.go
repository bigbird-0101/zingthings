package httpserver

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"net/http"
	"strconv"
)

func StartServer(ctx context.Context, log *zap.Logger, svc string, port int, handler http.Handler) {
	server := &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: handler,
	}
	l := log.With(zap.String("service", svc), zap.String("addr", server.Addr))
	l.Info("starting server")
	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server start failed", zap.Error(err))
		}
	}()
	<-ctx.Done()
	l.Info("shutting down server")
	if err := server.Shutdown(ctx); err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			l.Error("server shutdown error", zap.Error(err))
		}
	}
}
