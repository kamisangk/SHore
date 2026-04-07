package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	toml "github.com/pelletier/go-toml/v2"
)

const (
	UIModeEmbedded = "embedded"
	UIModeDisabled = "disabled"

	DatabaseDriverSQLite = "sqlite"

	defaultHTTPPort          = 8080
	defaultTStorageRetention = 7 * 24 * time.Hour
)

// Config 描述 Master 的启动期配置。
type Config struct {
	Server   ServerConfig   `toml:"server"`
	Database DatabaseConfig `toml:"database"`
	Security SecurityConfig `toml:"security"`
	History  HistoryConfig  `toml:"history"`

	collectorResolution time.Duration
	keepAliveResolution time.Duration
	historyRetention    time.Duration
}

type ServerConfig struct {
	HTTPPort int    `toml:"http_port"`
	UIMode   string `toml:"ui_mode"`
}

type DatabaseConfig struct {
	Driver     string `toml:"driver"`
	SQLitePath string `toml:"sqlite_path"`
}

type SecurityConfig struct {
	JWTSecret               string `toml:"jwt_secret"`
	CredentialEncryptionKey string `toml:"credential_encryption_key"`
}

type HistoryConfig struct {
	TStoragePath string `toml:"tstorage_path"`
}

// Load 从默认位置加载 shore.toml。
func Load() (Config, error) {
	for _, path := range candidateConfigPaths() {
		if _, err := os.Stat(path); err == nil {
			return LoadFromPath(path)
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return Config{}, fmt.Errorf("检查配置文件失败: %w", err)
		}
	}

	return Config{}, fmt.Errorf("未找到 shore.toml，请在当前目录、SHore 目录或可执行文件目录中提供该文件")
}

// LoadFromPath 从指定路径加载 TOML 启动配置。
func LoadFromPath(path string) (Config, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("读取配置文件失败: %w", err)
	}

	// 兼容 Windows 下常见的 UTF-8 BOM 配置文件。
	content = bytes.TrimPrefix(content, []byte{0xEF, 0xBB, 0xBF})

	cfg := Config{}
	if err := toml.Unmarshal(content, &cfg); err != nil {
		return Config{}, fmt.Errorf("解析 TOML 失败: %w", err)
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// NewTestConfig 提供测试专用配置。
func NewTestConfig() Config {
	databaseName := "shore-test-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	cfg := Config{
		Server: ServerConfig{
			HTTPPort: defaultHTTPPort,
			UIMode:   UIModeDisabled,
		},
		Database: DatabaseConfig{
			Driver:     DatabaseDriverSQLite,
			SQLitePath: "file:" + databaseName + "?mode=memory&cache=shared",
		},
		Security: SecurityConfig{
			JWTSecret:               "shore-test-jwt",
			CredentialEncryptionKey: "shore-test-encryption",
		},
		History: HistoryConfig{
			TStoragePath: filepath.Join(os.TempDir(), "shore-history-test-"+strconv.FormatInt(time.Now().UnixNano(), 10)),
		},
		collectorResolution: 10 * time.Millisecond,
		keepAliveResolution: 10 * time.Millisecond,
		historyRetention:    defaultTStorageRetention,
	}
	cfg.applyDefaults()
	return cfg
}

// ListenAddr 返回 Gin 监听地址。
func (c Config) ListenAddr() string {
	return ":" + strconv.Itoa(c.Server.HTTPPort)
}

// CollectorResolution 返回采集 ticker 的内部步进间隔。
func (c Config) CollectorResolution() time.Duration {
	if c.collectorResolution > 0 {
		return c.collectorResolution
	}

	return time.Second
}

// KeepAliveResolution 返回探活 ticker 的内部步进间隔。
func (c Config) KeepAliveResolution() time.Duration {
	if c.keepAliveResolution > 0 {
		return c.keepAliveResolution
	}

	return time.Second
}

// HistoryRetention 返回 TStorage 的保留时长。
func (c Config) HistoryRetention() time.Duration {
	if c.historyRetention > 0 {
		return c.historyRetention
	}

	return defaultTStorageRetention
}

func (c *Config) applyDefaults() {
	if c.Server.HTTPPort <= 0 {
		c.Server.HTTPPort = defaultHTTPPort
	}

	c.Server.UIMode = normalizeConfigValue(c.Server.UIMode)
	if c.Server.UIMode == "" {
		c.Server.UIMode = UIModeEmbedded
	}

	c.Database.Driver = normalizeConfigValue(c.Database.Driver)
	if c.Database.Driver == "" {
		c.Database.Driver = DatabaseDriverSQLite
	}

	c.Database.SQLitePath = strings.TrimSpace(c.Database.SQLitePath)
	c.Security.JWTSecret = strings.TrimSpace(c.Security.JWTSecret)
	c.Security.CredentialEncryptionKey = strings.TrimSpace(c.Security.CredentialEncryptionKey)
	c.History.TStoragePath = strings.TrimSpace(c.History.TStoragePath)

	if c.historyRetention <= 0 {
		c.historyRetention = defaultTStorageRetention
	}
}

func (c Config) validate() error {
	if c.Server.UIMode != UIModeEmbedded && c.Server.UIMode != UIModeDisabled {
		return fmt.Errorf("invalid server.ui_mode %q: 仅支持 embedded 或 disabled", c.Server.UIMode)
	}

	if c.Database.Driver != DatabaseDriverSQLite {
		return fmt.Errorf("invalid database.driver %q: 当前仅支持 sqlite", c.Database.Driver)
	}

	if c.Database.SQLitePath == "" {
		return fmt.Errorf("missing database.sqlite_path")
	}

	if c.Security.JWTSecret == "" {
		return fmt.Errorf("missing security.jwt_secret")
	}

	if c.Security.CredentialEncryptionKey == "" {
		return fmt.Errorf("missing security.credential_encryption_key")
	}

	if c.History.TStoragePath == "" {
		return fmt.Errorf("missing history.tstorage_path")
	}

	return nil
}

func candidateConfigPaths() []string {
	paths := []string{
		"shore.toml",
		filepath.Join("SHore", "shore.toml"),
	}

	if executable, err := os.Executable(); err == nil {
		executableDir := filepath.Dir(executable)
		paths = append(paths, filepath.Join(executableDir, "shore.toml"))
	}

	return uniqueNonEmptyPaths(paths)
}

func uniqueNonEmptyPaths(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		trimmed := strings.TrimSpace(path)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func normalizeConfigValue(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
