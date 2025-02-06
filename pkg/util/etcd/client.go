package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"zingthings/pkg/util/env"
)

func NewClient(ctx context.Context, logger *zap.Logger) *clientv3.Client {
	etcdUrl := env.GetEnvOrDefault[string]("ETCD_ENDPOINTS", "http://localhost:2379")
	endpoints := strings.Split(etcdUrl, ",")
	enableTls := env.GetEnvOrDefault[bool]("ETCD_ENABLE_TLS", false)
	var tlsConfig *tls.Config
	if enableTls {
		sslPath := os.Getenv("ETCD_SSL_PATH")
		if sslPath == "" {
			sslPath = "/etc/etcd/ssl/"
		}
		// 指定证书文件路径
		caCertPath := sslPath + "etcd-ca.pem"     // CA 证书路径
		clientCertPath := sslPath + "etcd.pem"    // 客户端证书路径
		clientKeyPath := sslPath + "etcd-key.pem" // 客户端私钥路径
		// 构建 TLS 配置
		config, err2 := loadTLSConfig(caCertPath, clientCertPath, clientKeyPath)
		tlsConfig = config
		if err2 != nil {
			panic(err2)
		}
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, TLS: tlsConfig, DialTimeout: 5 * time.Second})
	if err != nil {
		panic(err)
	}
	var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	c := make(chan os.Signal, 1)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		client.Close()
	}()
	return client
}

func loadTLSConfig(caCertPath, clientCertPath, clientKeyPath string) (*tls.Config, error) {
	// 加载 CA 证书
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %v", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("failed to append CA certificates")
	}

	// 如果需要客户端认证，则加载客户端证书和私钥
	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load client key pair: %v", err)
	}

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		Certificates:       []tls.Certificate{clientCert},
		InsecureSkipVerify: true, // 生产环境中不要设置为 true
	}
	return tlsConfig, nil
}
