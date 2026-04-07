package models

import (
	"strings"
	"time"

	"shore-master/monitor/identity"

	"gorm.io/gorm"
)

const (
	// AuthTypePassword 表示 SSH 密码认证。
	AuthTypePassword = 1
	// AuthTypePrivateKey 表示 SSH 私钥认证。
	AuthTypePrivateKey = 2
)

const (
	// ConnectionTypeSSH 表示纯 SSH 连接模式。
	ConnectionTypeSSH = 0
	// ConnectionTypeLiteAgent 为 LiteAgent 预留。
	ConnectionTypeLiteAgent = 1
	// ConnectionTypeFullAgent 为 FullAgent 预留。
	ConnectionTypeFullAgent = 2
)

type ServerStatus int

const (
	// ServerStatusOffline 表示服务器离线。
	ServerStatusOffline ServerStatus = 0
	// ServerStatusOnline 表示服务器在线。
	ServerStatusOnline ServerStatus = 1
)

// TimestampModel 抽取公共时间字段，便于字符串主键模型复用。
type TimestampModel struct {
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// BaseModel 保留给仍使用自增主键的系统表。
type BaseModel struct {
	ID uint `gorm:"primaryKey" json:"id"`
	TimestampModel
}

// SystemUser 表示后台管理员。
type SystemUser struct {
	BaseModel
	Username     string `gorm:"type:varchar(50);uniqueIndex;not null" json:"username"`
	PasswordHash string `gorm:"type:varchar(255);not null" json:"-"`
}

func (SystemUser) TableName() string {
	return "sys_users"
}

// SystemConfig 表示系统配置项。
type SystemConfig struct {
	BaseModel
	ConfigKey   string `gorm:"column:config_key;type:varchar(100);uniqueIndex;not null" json:"configKey"`
	ConfigValue string `gorm:"column:config_value;type:text;not null" json:"configValue"`
	Description string `gorm:"column:description;type:varchar(255)" json:"description"`
}

func (SystemConfig) TableName() string {
	return "sys_configs"
}

// Server 表示监控节点配置。
type Server struct {
	ID string `gorm:"primaryKey;type:char(26)" json:"id"`
	TimestampModel
	Name                    string       `gorm:"type:varchar(100);not null" json:"name"`
	GroupName               string       `gorm:"column:group_name;type:varchar(50)" json:"groupName"`
	IsPublic                int          `gorm:"column:is_public;type:int;not null" json:"isPublic"`
	OSPlatform              string       `gorm:"column:os_platform;type:varchar(50)" json:"osPlatform"`
	OSVersion               string       `gorm:"column:os_version;type:varchar(100)" json:"osVersion"`
	IPAddress               string       `gorm:"column:ip_address;type:varchar(45);not null" json:"ipAddress"`
	SSHPort                 int          `gorm:"column:ssh_port;not null;default:22" json:"sshPort"`
	SSHTimeout              *int         `gorm:"column:ssh_timeout" json:"sshTimeout"`
	SSHKeepaliveInterval    *int         `gorm:"column:ssh_keepalive_interval" json:"sshKeepaliveInterval"`
	MetricCollectInterval   *int         `gorm:"column:metric_collect_interval" json:"metricCollectInterval"`
	RealtimeCollectInterval *int         `gorm:"column:realtime_collect_interval" json:"realtimeCollectInterval"`
	SSHReconnectInterval    *int         `gorm:"column:ssh_reconnect_interval" json:"sshReconnectInterval"`
	AuthType                int          `gorm:"column:auth_type;not null" json:"authType"`
	AuthUser                string       `gorm:"column:auth_user;type:varchar(50);not null" json:"authUser"`
	AuthCredential          string       `gorm:"column:auth_credential;type:text;not null" json:"authCredential"`
	ConnType                int          `gorm:"column:conn_type;not null;default:0" json:"connType"`
	Status                  ServerStatus `gorm:"type:int;not null;default:0" json:"status"`
	DisplayIndex            int          `gorm:"column:display_index;not null;default:0" json:"displayIndex"`
	LastError               string       `gorm:"column:last_error;type:text" json:"lastError"`
}

func (Server) TableName() string {
	return "servers"
}

// BeforeCreate 统一生成或校验服务器 ULID，避免落库空主键。
func (server *Server) BeforeCreate(_ *gorm.DB) error {
	if strings.TrimSpace(server.ID) == "" {
		server.ID = identity.NewServerID()
		return nil
	}

	normalizedID, err := identity.ParseServerID(server.ID)
	if err != nil {
		return err
	}
	server.ID = normalizedID
	return nil
}
