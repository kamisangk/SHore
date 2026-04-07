package store

import (
	"fmt"
	"strconv"
	"strings"
)

// SystemConfigDefinition 描述单个系统配置项的元数据。
type SystemConfigDefinition struct {
	Key          string
	DefaultValue string
	Description  string
	Group        string
	InputType    string
	Options      []SystemConfigOption
}

// SystemConfigOption 描述下拉类配置可选项。
type SystemConfigOption struct {
	Label string
	Value string
}

var phase1SystemConfigOrder = []string{
	"public_view_enabled",
	"site_name",
	"tsdb_type",
	"metric_collect_interval",
	"realtime_collect_interval",
	"ssh_keepalive_interval",
	"ssh_timeout",
	"ssh_reconnect_interval",
}

// DefaultSystemConfigDefinitions 返回 Phase 1 系统配置定义。
func DefaultSystemConfigDefinitions() map[string]SystemConfigDefinition {
	return map[string]SystemConfigDefinition{
		"public_view_enabled": {
			Key:          "public_view_enabled",
			DefaultValue: "true",
			Description:  "是否开放未登录时的公共服务器监控面板",
			Group:        "站点与公共访问",
			InputType:    "switch",
		},
		"site_name": {
			Key:          "site_name",
			DefaultValue: "SHore Monitor",
			Description:  "网站名称，用于后台和公共面板标题显示",
			Group:        "站点与公共访问",
			InputType:    "text",
		},
		"tsdb_type": {
			Key:          "tsdb_type",
			DefaultValue: "0",
			Description:  "时序数据库类型：0-不启用，1-TStorage，2-预留",
			Group:        "监控采集",
			InputType:    "select",
			Options: []SystemConfigOption{
				{Label: "不启用", Value: "0"},
				{Label: "TStorage", Value: "1"},
				{Label: "预留", Value: "2"},
			},
		},
		"metric_collect_interval": {
			Key:          "metric_collect_interval",
			DefaultValue: "120",
			Description:  "监控数据采集颗粒度（单位：秒）",
			Group:        "监控采集",
			InputType:    "number",
		},
		"realtime_collect_interval": {
			Key:          "realtime_collect_interval",
			DefaultValue: "5",
			Description:  "实时面板与实时图表的采集和刷新间隔（单位：秒）",
			Group:        "监控采集",
			InputType:    "number",
		},
		"ssh_keepalive_interval": {
			Key:          "ssh_keepalive_interval",
			DefaultValue: "30",
			Description:  "Master 对 SSH 节点进行探活的间隔时间（单位：秒）",
			Group:        "SSH 连接",
			InputType:    "number",
		},
		"ssh_timeout": {
			Key:          "ssh_timeout",
			DefaultValue: "10",
			Description:  "SSH 连接超时时间（单位：秒）",
			Group:        "SSH 连接",
			InputType:    "number",
		},
		"ssh_reconnect_interval": {
			Key:          "ssh_reconnect_interval",
			DefaultValue: "300",
			Description:  "SSH 连接失败后的重连等待时间（单位：秒）",
			Group:        "SSH 连接",
			InputType:    "number",
		},
	}
}

// OrderedSystemConfigDefinitions 返回稳定顺序的配置定义，便于播种和界面展示。
func OrderedSystemConfigDefinitions() []SystemConfigDefinition {
	definitions := DefaultSystemConfigDefinitions()
	result := make([]SystemConfigDefinition, 0, len(phase1SystemConfigOrder))

	for _, key := range phase1SystemConfigOrder {
		if definition, ok := definitions[key]; ok {
			result = append(result, definition)
		}
	}

	return result
}

// ValidateSystemConfigValue 对配置值做基础校验。
func ValidateSystemConfigValue(key string, value string) error {
	definition, ok := DefaultSystemConfigDefinitions()[key]
	if !ok {
		return fmt.Errorf("unknown config key: %s", key)
	}

	switch definition.InputType {
	case "switch":
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized != "true" && normalized != "false" {
			return fmt.Errorf("config %s expects true or false", key)
		}
	case "number":
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || parsed < 0 {
			return fmt.Errorf("config %s expects non-negative integer", key)
		}
	case "select":
		for _, option := range definition.Options {
			if option.Value == value {
				return nil
			}
		}
		return fmt.Errorf("config %s expects one of predefined values", key)
	}

	return nil
}

// ParseSystemConfigBool 读取布尔配置，非法时回退为 false。
func ParseSystemConfigBool(key string, value string) bool {
	_, ok := DefaultSystemConfigDefinitions()[key]
	if !ok {
		return false
	}

	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false
	}

	return parsed
}

// ParseSystemConfigInt 读取整数配置，非法时回退为 0。
func ParseSystemConfigInt(key string, value string) int {
	_, ok := DefaultSystemConfigDefinitions()[key]
	if !ok {
		return 0
	}

	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err == nil {
		return parsed
	}

	return 0
}
