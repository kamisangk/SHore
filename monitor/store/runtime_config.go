package store

import (
	"fmt"

	"shore-master/monitor/models"

	"gorm.io/gorm"
)

// MonitoringRuntimeConfig 描述监控与 SSH 运行时实际使用的系统配置。
type MonitoringRuntimeConfig struct {
	MetricCollectInterval   int
	RealtimeCollectInterval int
	SSHKeepaliveInterval    int
	SSHTimeout              int
	SSHReconnectInterval    int
}

// LoadMonitoringRuntimeConfig 从 sys_configs 读取当前监控运行时配置。
func LoadMonitoringRuntimeConfig(db *gorm.DB) (MonitoringRuntimeConfig, error) {
	config := MonitoringRuntimeConfig{}
	if db == nil {
		return config, nil
	}

	keys := []string{
		"metric_collect_interval",
		"realtime_collect_interval",
		"ssh_keepalive_interval",
		"ssh_timeout",
		"ssh_reconnect_interval",
	}

	var records []models.SystemConfig
	if err := db.Where("config_key IN ?", keys).Find(&records).Error; err != nil {
		return config, err
	}

	recordMap := make(map[string]string, len(records))
	for _, record := range records {
		recordMap[record.ConfigKey] = record.ConfigValue
	}

	var err error
	config.MetricCollectInterval, err = requiredIntSystemConfig(recordMap, "metric_collect_interval", 1)
	if err != nil {
		return config, err
	}
	config.RealtimeCollectInterval, err = requiredIntSystemConfig(recordMap, "realtime_collect_interval", 1)
	if err != nil {
		return config, err
	}
	config.SSHKeepaliveInterval, err = requiredIntSystemConfig(recordMap, "ssh_keepalive_interval", 1)
	if err != nil {
		return config, err
	}
	config.SSHTimeout, err = requiredIntSystemConfig(recordMap, "ssh_timeout", 1)
	if err != nil {
		return config, err
	}
	config.SSHReconnectInterval, err = requiredIntSystemConfig(recordMap, "ssh_reconnect_interval", 0)
	if err != nil {
		return config, err
	}

	return config, nil
}

// ApplyToServer 将全局运行时配置补到未设置节点覆盖值的服务器记录上。
func (config MonitoringRuntimeConfig) ApplyToServer(server models.Server) models.Server {
	if server.MetricCollectInterval == nil && config.MetricCollectInterval > 0 {
		server.MetricCollectInterval = intPtr(config.MetricCollectInterval)
	}

	if server.RealtimeCollectInterval == nil && config.RealtimeCollectInterval > 0 {
		server.RealtimeCollectInterval = intPtr(config.RealtimeCollectInterval)
	}

	if server.SSHKeepaliveInterval == nil && config.SSHKeepaliveInterval > 0 {
		server.SSHKeepaliveInterval = intPtr(config.SSHKeepaliveInterval)
	}

	if server.SSHTimeout == nil && config.SSHTimeout > 0 {
		server.SSHTimeout = intPtr(config.SSHTimeout)
	}

	if server.SSHReconnectInterval == nil && config.SSHReconnectInterval > 0 {
		server.SSHReconnectInterval = intPtr(config.SSHReconnectInterval)
	}

	return server
}

func intPtr(value int) *int {
	copy := value
	return &copy
}

func requiredIntSystemConfig(recordMap map[string]string, key string, minValue int) (int, error) {
	value, ok := recordMap[key]
	if !ok {
		return 0, fmt.Errorf("missing system config: %s", key)
	}

	parsed := ParseSystemConfigInt(key, value)
	if parsed < minValue {
		return 0, fmt.Errorf("invalid system config %s: %q", key, value)
	}

	return parsed, nil
}
