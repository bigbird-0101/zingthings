package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

type (
	KafkaConfig struct {
		Broker struct {
			List []string `yaml:"list"`
		} `yaml:"broker"`
		Consumer struct {
			Group        string `yaml:"group"`
			TopicPattern string `yaml:"topic-pattern"`
		} `yaml:"consumer"`
	}
	Config struct {
		Kafka *KafkaConfig `yaml:"kafka"`
	}
)

func NewConfig() *Config {
	return &Config{}
}

// LoadYAMLConfig 从指定路径加载 YAML 配置文件
func LoadYAMLConfig(filePath string) (*Config, error) {
	appConfig := NewConfig()
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	err = yaml.Unmarshal(data, &appConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	return appConfig, nil
}
