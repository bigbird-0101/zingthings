package tcpserver

import (
	"context"
	"encoding/hex"
	"go.uber.org/zap"
	"io"
	"net"
	"strconv"
	"zingthings/pkg/protocol/core"
)

const (
	DefaultPort = 8887
)

type (
	Protocol struct {
		core.GenericProtocol
		listener *net.TCPListener
		logger   *zap.Logger
		ctx      context.Context
	}
)

func init() {
	core.RegisterProtocol(core.ProtocolInitRegister{
		ProtocolType: core.TcpServer,
		Setup: func(context *core.ProtocolSetupContext) core.Protocol {
			return NewTcpServerProtocol(context.Logger, context.Protocol)
		},
	})
}

func NewTcpServerProtocol(logger *zap.Logger, protocol core.GenericProtocol) *Protocol {
	return &Protocol{
		GenericProtocol: protocol,
		logger:          logger.Named("TcpServerProtocol"),
	}
}

func (t *Protocol) Start(context *core.NodeContext) {
	port := context.GetPort(DefaultPort)
	addr, err := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(port))
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.logger.Error("tcp listen fail", zap.Error(err))
		return
	}
	t.listener = listener
	t.ctx = context.GetContext()
	go t.acceptConnections()
	channel := t.ChannelFactory.Create(t.ChannelHandlerPipeline, t)
	channel.Attach(core.ProtocolTypeKey, core.TcpServer)
	for _, info := range t.DeviceInfos {
		t.ChannelManager.Register(string(info.DeviceId), channel)
	}
	t.logger.Info("start tcp Server", zap.Int("port", port))
}

func (t *Protocol) acceptConnections() {
	ctx, cancelFunc := context.WithCancel(t.ctx)
	defer cancelFunc()
	for {
		select {
		case <-ctx.Done():
			t.Stop()
			return
		default:
			tcp, err := t.listener.AcceptTCP()
			if err != nil {
				t.logger.Error("tcp accept fail", zap.Error(err))
				continue
			}
			go t.handlerCon(tcp)
		}
	}
}

func (t *Protocol) Stop() {
	t.listener.Close()
}

func (t *Protocol) GetProtocolType() core.ProtocolType {
	return core.TcpServer
}

func (t *Protocol) GetProtocolId() core.ProtocolId {
	return t.ProtocolId
}

func (t *Protocol) handlerCon(tcp *net.TCPConn) {
	defer tcp.Close()
	for {
		a := make([]byte, 1024)
		n, err := tcp.Read(a)
		if err != nil {
			if err == io.EOF {
				t.logger.Error("tcp read fail client exit", zap.Error(err))
				return
			}
			t.logger.Error("tcp read fail", zap.Error(err))
			continue
		}
		hexData := hex.EncodeToString(a[:n])
		t.logger.Debug("tcp read data", zap.String("data", hexData))
		message := core.NewUpMessage(core.Header{
			ProtocolType:  t.GetProtocolType(),
			DeviceGroupId: t.DeviceGroup.DeviceGroupId,
		}, &core.ResponseData{Response: make(chan *core.Message, 1), Data: hexData})
		err = t.UpLink.UpLink(message)
		if err != nil {
			t.logger.Error("tcp uplink fail", zap.Error(err))
		}
		response, ok := message.Data.(*core.ResponseData)
		if ok {
			messageResponse := <-response.Response
			deviceId := messageResponse.Header.DeviceId
			if "" != deviceId {
				t.logger.Info("reply device", zap.String("deviceId", string(deviceId)))
				channel := t.ChannelManager.Get(string(deviceId))
				if channel.GetAttach(core.NetworkConnection) != tcp {
					channel.Attach(core.NetworkConnection, tcp)
				}
			}
		}
	}
}
