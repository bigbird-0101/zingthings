package core

import "errors"

type (
	DownLink struct {
		ChannelManager ChannelManager
	}
)

func NewDownLink() *DownLink {
	return &DownLink{
		ChannelManager: DefaultChannelManagerCommon,
	}
}

func (d *DownLink) DownLink(message *Message) error {
	channel := d.ChannelManager.Get(string(message.Header.DeviceId))
	if channel == nil {
		return errors.New("channel not exist")
	}
	if message.Header.ProtocolType == "" && nil != channel.GetAttach(ProtocolTypeKey) {
		attach := channel.GetAttach(ProtocolTypeKey)
		message.Header.ProtocolType = ProtocolType(attach.(string))
	}
	channel.SendDownStream(message)
	return nil
}
