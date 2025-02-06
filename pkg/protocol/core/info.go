package core

import (
	"fmt"
	"zingthings/pkg/common"
)

func (p *ProtocolInfo) BuildKey() string {
	return fmt.Sprintf("%s/%s/%s/%s", common.CompleteProtocol, p.ProtocolType, p.Address, p.Id)
}

func (p *ProtocolInfo) BuildNodeKey() string {
	return fmt.Sprintf("%s/%s/%s/%s", common.CompleteProtocolNode, p.Address, p.ProtocolType, p.Id)
}

func (p *ProtocolInfo) BuildInfoKey() string {
	return fmt.Sprintf("%s/%s", common.CompleteProtocolInfo, p.Id)
}

func (p *ProtocolInfo) BuildAddressKey() string {
	return fmt.Sprintf("%s/%s", common.CompletePath, p.Address)
}

func (n *NodeInfo) BuildKey() string {
	return fmt.Sprintf("%s/%s:%d", common.CompletePath, n.Host, n.Port)
}

func (n *NodeInfo) BuildProtocolKey() string {
	return fmt.Sprintf("%s/%s:%d", common.CompleteProtocolNode, n.Host, n.Port)
}

func (n *NodeInfo) BuildDeployKey() string {
	return fmt.Sprintf("%s/%s:%d", common.CompleteDeploy, n.Host, n.Port)
}

func (n *NodeInfo) BuildRecoverKey() string {
	return fmt.Sprintf("%s/%s:%d", common.CompleteRecover, n.Host, n.Port)
}

func (n *NodeInfo) BuildAddress() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}
