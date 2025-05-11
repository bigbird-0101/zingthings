package db

import "gorm.io/gorm"

type (
	ProtocolModel struct {
		gorm.Model
		Id           string `gorm:"column:id;primary_key"`
		ProtocolId   string `gorm:"column:protocol_id"`
		ProtocolType string `gorm:"column:protocol_type"`
		Address      string `gorm:"column:address"`
		Timestamp    int64  `gorm:"column:timestamp"`
	}
)
