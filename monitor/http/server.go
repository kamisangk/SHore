package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"shore-master/monitor/config"
	"shore-master/monitor/geoip"
	"shore-master/monitor/identity"
	"shore-master/monitor/models"
	"shore-master/monitor/monitor"
	"shore-master/monitor/security"
	"shore-master/monitor/sshpool"
	"shore-master/monitor/store"
	"shore-master/monitor/ws"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"golang.org/x/crypto/ssh"
	"gorm.io/gorm"
)

// Server 封装 SHore 的 HTTP/WS 入口。
type Server struct {
	cfg       config.Config
	db        *gorm.DB
	engine    *gin.Engine
	encryptor *security.Encryptor
	pool      *sshpool.Pool
	hub       *ws.Hub
	monitor   *monitor.Service
}

type loginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type serverUpsertRequest struct {
	Name                    string `json:"name" binding:"required"`
	GroupName               string `json:"groupName"`
	IPAddress               string `json:"ipAddress"`
	SSHPort                 int    `json:"sshPort"`
	AuthType                int    `json:"authType"`
	AuthUser                string `json:"authUser"`
	AuthCredential          string `json:"authCredential"`
	ConnType                int    `json:"connType"`
	DisplayIndex            int    `json:"displayIndex"`
	IsPublic                *int   `json:"isPublic"`
	SSHTimeout              *int   `json:"sshTimeout"`
	SSHKeepaliveInterval    *int   `json:"sshKeepaliveInterval"`
	MetricCollectInterval   *int   `json:"metricCollectInterval"`
	RealtimeCollectInterval *int   `json:"realtimeCollectInterval"`
	SSHReconnectInterval    *int   `json:"sshReconnectInterval"`
}

type systemConfigUpdateRequest struct {
	Items []systemConfigUpdateItem `json:"items"`
}

type systemConfigUpdateItem struct {
	ConfigKey   string `json:"configKey"`
	ConfigValue string `json:"configValue"`
}

type systemConfigOptionView struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type adminSystemConfigView struct {
	ConfigKey    string                   `json:"configKey"`
	ConfigValue  string                   `json:"configValue"`
	DefaultValue string                   `json:"defaultValue"`
	Description  string                   `json:"description"`
	Group        string                   `json:"group"`
	InputType    string                   `json:"inputType"`
	Options      []systemConfigOptionView `json:"options,omitempty"`
}

type adminServerView struct {
	ID                      string              `json:"id"`
	Name                    string              `json:"name"`
	GroupName               string              `json:"groupName"`
	IPAddress               string              `json:"ipAddress"`
	SSHPort                 int                 `json:"sshPort"`
	AuthType                int                 `json:"authType"`
	AuthUser                string              `json:"authUser"`
	AuthCredential          string              `json:"authCredential"`
	ConnType                int                 `json:"connType"`
	Status                  models.ServerStatus `json:"status"`
	DisplayIndex            int                 `json:"displayIndex"`
	IsPublic                int                 `json:"isPublic"`
	OSPlatform              string              `json:"osPlatform"`
	OSVersion               string              `json:"osVersion"`
	SSHTimeout              *int                `json:"sshTimeout"`
	SSHKeepaliveInterval    *int                `json:"sshKeepaliveInterval"`
	MetricCollectInterval   *int                `json:"metricCollectInterval"`
	RealtimeCollectInterval *int                `json:"realtimeCollectInterval"`
	SSHReconnectInterval    *int                `json:"sshReconnectInterval"`
	LastError               string              `json:"lastError"`
	CreatedAt               time.Time           `json:"createdAt"`
	UpdatedAt               time.Time           `json:"updatedAt"`
}

// NewServer 创建并初始化后端服务。
func NewServer(cfg config.Config) (*Server, error) {
	if strings.Contains(cfg.Database.SQLitePath, "mode=memory") {
		gin.SetMode(gin.TestMode)
	}

	db, err := store.Open(cfg)
	if err != nil {
		return nil, err
	}

	if err := store.MigrateAndSeed(db, cfg); err != nil {
		return nil, err
	}

	historyStore, err := store.OpenHistoryStore(cfg)
	if err != nil {
		return nil, err
	}

	encryptor := security.NewEncryptor(cfg.Security.CredentialEncryptionKey)
	pool := sshpool.New(encryptor)
	hub := ws.NewHub()
	locationResolver := geoip.NewService("", 0, nil)
	monitorService := monitor.NewService(db, historyStore, locationResolver, pool, hub, cfg)

	engine := gin.New()
	engine.Use(gin.Recovery(), gin.Logger(), corsMiddleware())

	server := &Server{
		cfg:       cfg,
		db:        db,
		engine:    engine,
		encryptor: encryptor,
		pool:      pool,
		hub:       hub,
		monitor:   monitorService,
	}

	if err := server.registerRoutes(); err != nil {
		return nil, err
	}
	return server, nil
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type")
		c.Header("Access-Control-Expose-Headers", "Authorization")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// Engine 暴露 gin 引擎，便于测试。
func (s *Server) Engine() *gin.Engine {
	return s.engine
}

// StartBackground 启动后台采集与广播任务。
func (s *Server) StartBackground(ctx context.Context) {
	s.monitor.Start(ctx)
}

// Run 启动 Web 服务。
func (s *Server) Run() error {
	return s.engine.Run(s.cfg.ListenAddr())
}

// Close 释放服务器持有的资源。
func (s *Server) Close() error {
	s.pool.Close()
	return s.monitor.Close()
}

func (s *Server) registerRoutes() error {
	api := s.engine.Group("/api/v1")
	{
		api.POST("/auth/login", s.handleLogin)
		api.GET("/config/public", s.handlePublicConfig)
	}

	public := api.Group("/public")
	public.Use(s.publicViewMiddleware())
	{
		public.GET("/servers/metrics", s.handlePublicMetrics)
		public.GET("/servers/:id/history", s.handleServerHistory)
	}

	admin := api.Group("/admin")
	admin.Use(s.authMiddleware())
	{
		admin.GET("/system-configs", s.handleListSystemConfigs)
		admin.PUT("/system-configs", s.handleUpdateSystemConfigs)
		admin.GET("/servers", s.handleListServers)
		admin.GET("/servers/metrics", s.handleAdminMetrics)
		admin.POST("/servers", s.handleCreateServer)
		admin.PUT("/servers/:id", s.handleUpdateServer)
		admin.DELETE("/servers/:id", s.handleDeleteServer)
		admin.GET("/servers/:id/history", s.handleServerHistory)
	}

	wsGroup := s.engine.Group("/ws/v1")
	{
		wsGroup.GET("/dashboard", s.handleDashboardWS)
		wsGroup.GET("/terminal/:id", s.handleTerminalWS)
	}

	if s.cfg.Server.UIMode == config.UIModeEmbedded {
		return s.registerUIRoutes()
	}

	return nil
}

func (s *Server) handleLogin(c *gin.Context) {
	var request loginRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "请求参数错误"})
		return
	}

	var user models.SystemUser
	if err := s.db.Where("username = ?", request.Username).First(&user).Error; err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "message": "用户名或密码错误"})
		return
	}

	if err := security.ComparePassword(user.PasswordHash, request.Password); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "message": "用户名或密码错误"})
		return
	}

	token, err := security.IssueToken(s.cfg.Security.JWTSecret, user.ID, user.Username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "生成 token 失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":  200,
		"token": token,
		"user": gin.H{
			"id":       user.ID,
			"username": user.Username,
		},
	})
}

func (s *Server) handlePublicConfig(c *gin.Context) {
	publicView, err := s.isPublicViewEnabled()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "读取配置失败"})
		return
	}

	siteName, err := s.systemConfigValue("site_name")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "读取公共配置失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": gin.H{
			"public_view_enabled": publicView,
			"site_name":           siteName,
		},
	})
}

func (s *Server) handleListSystemConfigs(c *gin.Context) {
	items, err := s.listSystemConfigs()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "读取系统设置失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": items,
	})
}

func (s *Server) handleUpdateSystemConfigs(c *gin.Context) {
	var request systemConfigUpdateRequest
	if err := c.ShouldBindJSON(&request); err != nil || len(request.Items) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "请求参数错误"})
		return
	}

	// 先标准化并校验整批配置，再在一个事务里执行 upsert，避免部分写入造成运行时配置不一致。
	// Normalize and validate the full batch first, then upsert in one transaction to avoid partial runtime configuration writes.
	definitions := store.DefaultSystemConfigDefinitions()
	normalizedItems := make([]systemConfigUpdateItem, 0, len(request.Items))
	for _, item := range request.Items {
		item.ConfigKey = strings.TrimSpace(item.ConfigKey)
		item.ConfigValue = strings.TrimSpace(item.ConfigValue)
		if item.ConfigKey == "" {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "配置键不能为空"})
			return
		}
		if err := store.ValidateSystemConfigValue(item.ConfigKey, item.ConfigValue); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": err.Error()})
			return
		}
		normalizedItems = append(normalizedItems, item)
	}

	if err := s.db.Transaction(func(tx *gorm.DB) error {
		for _, item := range normalizedItems {
			definition := definitions[item.ConfigKey]
			var record models.SystemConfig
			err := tx.Where("config_key = ?", item.ConfigKey).First(&record).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}

			record.ConfigKey = item.ConfigKey
			record.ConfigValue = item.ConfigValue
			record.Description = definition.Description

			if record.ID == 0 {
				if err := tx.Create(&record).Error; err != nil {
					return err
				}
				continue
			}

			if err := tx.Save(&record).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "保存系统设置失败"})
		return
	}

	items, err := s.listSystemConfigs()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "读取系统设置失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": items,
	})
}

func (s *Server) handlePublicMetrics(c *gin.Context) {
	metrics, err := s.monitor.PublicMetrics(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "读取监控数据失败"})
		return
	}

	c.JSON(http.StatusOK, metrics)
}

func (s *Server) handleListServers(c *gin.Context) {
	var servers []models.Server
	if err := s.db.Order("display_index asc, id asc").Find(&servers).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "读取服务器列表失败"})
		return
	}

	result := make([]adminServerView, 0, len(servers))
	for _, server := range servers {
		result = append(result, s.toAdminServerView(server))
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": result,
	})
}

func (s *Server) handleAdminMetrics(c *gin.Context) {
	metrics, err := s.monitor.PublicMetrics(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "读取监控数据失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": metrics,
	})
}

func (s *Server) handleCreateServer(c *gin.Context) {
	var request serverUpsertRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "请求参数错误"})
		return
	}

	if err := validateServerUpsertRequest(request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": err.Error()})
		return
	}

	encryptedCredential, err := s.encryptor.Encrypt(request.AuthCredential)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "凭证加密失败"})
		return
	}

	var server models.Server
	applyServerUpsert(&server, request, encryptedCredential)

	if err := s.db.Create(&server).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "创建服务器失败"})
		return
	}

	s.monitor.TriggerCollectByID(c.Request.Context(), server.ID)
	c.JSON(http.StatusCreated, gin.H{
		"code": 201,
		"data": s.toAdminServerView(server),
	})
}

func (s *Server) handleUpdateServer(c *gin.Context) {
	serverID, err := parseID(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "服务器 ID 非法"})
		return
	}

	var request serverUpsertRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "请求参数错误"})
		return
	}

	if err := validateServerUpsertRequest(request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": err.Error()})
		return
	}

	var server models.Server
	if err := s.db.Where("id = ?", serverID).First(&server).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"code": 404, "message": "服务器不存在"})
		return
	}

	encryptedCredential, err := s.encryptor.Encrypt(request.AuthCredential)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "凭证加密失败"})
		return
	}

	applyServerUpsert(&server, request, encryptedCredential)

	if err := s.db.Save(&server).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "更新服务器失败"})
		return
	}

	s.pool.Remove(server.ID)
	s.monitor.TriggerCollectByID(c.Request.Context(), server.ID)
	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": s.toAdminServerView(server),
	})
}

func (s *Server) handleDeleteServer(c *gin.Context) {
	serverID, err := parseID(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "服务器 ID 非法"})
		return
	}

	if err := s.db.Where("id = ?", serverID).Delete(&models.Server{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "删除服务器失败"})
		return
	}

	s.pool.Remove(serverID)
	_ = s.monitor.RemoveServer(c.Request.Context(), serverID)
	c.JSON(http.StatusOK, gin.H{"code": 200})
}

func (s *Server) handleServerHistory(c *gin.Context) {
	serverID, err := parseID(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "服务器 ID 非法"})
		return
	}

	from, to, err := parseHistoryQueryRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "message": "历史时间范围参数错误"})
		return
	}

	var history []monitor.HistoryPoint
	mode := strings.TrimSpace(strings.ToLower(c.Query("mode")))
	if mode == "realtime" {
		history, err = s.monitor.RealtimeHistoryBetween(c.Request.Context(), serverID, from, to)
	} else {
		history, err = s.monitor.HistoryBetween(c.Request.Context(), serverID, from, to)
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "读取历史数据失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": history,
	})
}

func (s *Server) handleDashboardWS(c *gin.Context) {
	publicView, err := s.isPublicViewEnabled()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "读取配置失败"})
		return
	}

	if !publicView {
		if _, err := s.authClaimsFromRequest(c.Request); err != nil {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"code": 403, "message": "公共面板未开放"})
			return
		}
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}

	_ = s.monitor.CollectOnce(c.Request.Context())
	if payload, err := s.monitor.PublicMetrics(c.Request.Context()); err == nil {
		_ = conn.WriteJSON(map[string]any{
			"type": "metrics_update",
			"data": payload,
		})
	}

	s.hub.Register(conn)
	defer s.hub.Unregister(conn)
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (s *Server) handleTerminalWS(c *gin.Context) {
	if _, err := s.authClaimsFromRequest(c.Request); err != nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"code": 401, "message": "请先登录"})
		return
	}

	serverID, err := parseID(c.Param("id"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"code": 400, "message": "服务器 ID 非法"})
		return
	}

	var server models.Server
	if err := s.db.Where("id = ?", serverID).First(&server).Error; err != nil {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{"code": 404, "message": "服务器不存在"})
		return
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	runtimeServer, err := s.serverWithRuntimeConfig(server)
	if err != nil {
		_ = conn.WriteJSON(gin.H{"type": "error", "message": "读取 SSH 运行配置失败: " + err.Error()})
		return
	}

	client, err := s.pool.GetClient(runtimeServer)
	if err != nil {
		_ = conn.WriteJSON(gin.H{"type": "error", "message": "SSH 连接失败: " + err.Error()})
		return
	}

	// 这里把一个 SSH PTY 会话桥接到单个 WebSocket 连接，前端输入和远端输出都经由同一条会话转发。
	// This bridges one SSH PTY session to a single WebSocket connection so browser input and remote output share the same session.
	session, err := client.NewSession()
	if err != nil {
		_ = conn.WriteJSON(gin.H{"type": "error", "message": "创建 SSH Session 失败: " + err.Error()})
		return
	}
	defer session.Close()

	terminalWriter := &safeWSWriter{conn: conn}
	stdin, err := session.StdinPipe()
	if err != nil {
		_ = terminalWriter.WriteJSON(gin.H{"type": "error", "message": "初始化终端输入失败"})
		return
	}

	stdout, err := session.StdoutPipe()
	if err != nil {
		_ = terminalWriter.WriteJSON(gin.H{"type": "error", "message": "初始化终端输出失败"})
		return
	}

	stderr, err := session.StderrPipe()
	if err != nil {
		_ = terminalWriter.WriteJSON(gin.H{"type": "error", "message": "初始化终端错误输出失败"})
		return
	}

	modes := ssh.TerminalModes{
		ssh.ECHO:          1,
		ssh.TTY_OP_ISPEED: 14400,
		ssh.TTY_OP_OSPEED: 14400,
	}
	if err := session.RequestPty("xterm-256color", 32, 120, modes); err != nil {
		_ = terminalWriter.WriteJSON(gin.H{"type": "error", "message": "请求 PTY 失败: " + err.Error()})
		return
	}

	if err := session.Shell(); err != nil {
		_ = terminalWriter.WriteJSON(gin.H{"type": "error", "message": "启动远程 Shell 失败: " + err.Error()})
		return
	}

	go pipeSSHOutput(terminalWriter, stdout)
	go pipeSSHOutput(terminalWriter, stderr)

	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			break
		}

		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}

		if handled, err := handleTerminalControlFrame(session, stdin, payload); err != nil {
			_ = terminalWriter.WriteJSON(gin.H{"type": "error", "message": err.Error()})
			break
		} else if handled {
			continue
		}

		if _, err := stdin.Write(payload); err != nil {
			break
		}
	}
}

func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims, err := s.authClaimsFromRequest(c.Request)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"code": 401, "message": "未授权访问"})
			return
		}

		c.Set("user", claims)
		c.Next()
	}
}

func (s *Server) publicViewMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		enabled, err := s.isPublicViewEnabled()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"code": 500, "message": "读取配置失败"})
			return
		}

		if !enabled {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"code": 403, "message": "公共面板未开放"})
			return
		}

		c.Next()
	}
}

func (s *Server) isPublicViewEnabled() (bool, error) {
	value, err := s.systemConfigValue("public_view_enabled")
	if err != nil {
		return false, err
	}

	return store.ParseSystemConfigBool("public_view_enabled", value), nil
}

func (s *Server) authClaimsFromRequest(r *http.Request) (*security.Claims, error) {
	token := parseBearerToken(r.Header.Get("Authorization"))
	if token == "" {
		token = r.URL.Query().Get("token")
	}

	if token == "" {
		return nil, errors.New("missing token")
	}

	return security.ParseToken(s.cfg.Security.JWTSecret, token)
}

func (s *Server) toAdminServerView(server models.Server) adminServerView {
	credential, err := s.encryptor.Decrypt(server.AuthCredential)
	if err != nil {
		credential = ""
	}

	return adminServerView{
		ID:                      server.ID,
		Name:                    server.Name,
		GroupName:               normalizeGroupName(server.GroupName),
		IPAddress:               server.IPAddress,
		SSHPort:                 server.SSHPort,
		AuthType:                server.AuthType,
		AuthUser:                server.AuthUser,
		AuthCredential:          credential,
		ConnType:                server.ConnType,
		Status:                  server.Status,
		DisplayIndex:            server.DisplayIndex,
		IsPublic:                server.IsPublic,
		OSPlatform:              server.OSPlatform,
		OSVersion:               server.OSVersion,
		SSHTimeout:              server.SSHTimeout,
		SSHKeepaliveInterval:    server.SSHKeepaliveInterval,
		MetricCollectInterval:   server.MetricCollectInterval,
		RealtimeCollectInterval: server.RealtimeCollectInterval,
		SSHReconnectInterval:    server.SSHReconnectInterval,
		LastError:               server.LastError,
		CreatedAt:               server.CreatedAt,
		UpdatedAt:               server.UpdatedAt,
	}
}

func (s *Server) listSystemConfigs() ([]adminSystemConfigView, error) {
	var records []models.SystemConfig
	if err := s.db.Find(&records).Error; err != nil {
		return nil, err
	}

	recordMap := make(map[string]models.SystemConfig, len(records))
	for _, record := range records {
		recordMap[record.ConfigKey] = record
	}

	items := make([]adminSystemConfigView, 0, len(store.OrderedSystemConfigDefinitions()))
	for _, definition := range store.OrderedSystemConfigDefinitions() {
		record, ok := recordMap[definition.Key]
		if !ok {
			return nil, fmt.Errorf("missing system config record: %s", definition.Key)
		}

		item := adminSystemConfigView{
			ConfigKey:    definition.Key,
			ConfigValue:  record.ConfigValue,
			DefaultValue: definition.DefaultValue,
			Description:  definition.Description,
			Group:        definition.Group,
			InputType:    definition.InputType,
		}

		if len(definition.Options) > 0 {
			item.Options = make([]systemConfigOptionView, 0, len(definition.Options))
			for _, option := range definition.Options {
				item.Options = append(item.Options, systemConfigOptionView{
					Label: option.Label,
					Value: option.Value,
				})
			}
		}

		items = append(items, item)
	}

	return items, nil
}

func (s *Server) systemConfigValue(key string) (string, error) {
	if _, ok := store.DefaultSystemConfigDefinitions()[key]; !ok {
		return "", fmt.Errorf("unknown config key: %s", key)
	}

	var record models.SystemConfig
	if err := s.db.Where("config_key = ?", key).First(&record).Error; err != nil {
		return "", err
	}

	return record.ConfigValue, nil
}

func (s *Server) serverWithRuntimeConfig(server models.Server) (models.Server, error) {
	runtimeConfig, err := store.LoadMonitoringRuntimeConfig(s.db)
	if err != nil {
		return server, err
	}

	return runtimeConfig.ApplyToServer(server), nil
}

func applyServerUpsert(server *models.Server, request serverUpsertRequest, encryptedCredential string) {
	server.Name = strings.TrimSpace(request.Name)
	server.GroupName = normalizeGroupName(request.GroupName)
	server.IPAddress = strings.TrimSpace(request.IPAddress)
	server.SSHPort = normalizePort(request.SSHPort)
	server.AuthType = normalizeAuthType(request.AuthType)
	server.AuthUser = strings.TrimSpace(request.AuthUser)
	server.AuthCredential = encryptedCredential
	server.ConnType = request.ConnType
	server.Status = models.ServerStatusOffline
	server.DisplayIndex = request.DisplayIndex
	server.IsPublic = normalizePublicValue(request.IsPublic)
	server.SSHTimeout = request.SSHTimeout
	server.SSHKeepaliveInterval = request.SSHKeepaliveInterval
	server.MetricCollectInterval = request.MetricCollectInterval
	server.RealtimeCollectInterval = request.RealtimeCollectInterval
	server.SSHReconnectInterval = request.SSHReconnectInterval
}

func parseID(value string) (string, error) {
	return identity.ParseServerID(value)
}

func normalizePort(port int) int {
	if port <= 0 {
		return 22
	}

	return port
}

func normalizeAuthType(authType int) int {
	if authType == models.AuthTypePrivateKey {
		return authType
	}

	return models.AuthTypePassword
}

func normalizePublicValue(value *int) int {
	if value == nil || *value != 0 {
		return 1
	}

	return 0
}

func normalizeGroupName(value string) string {
	return strings.TrimSpace(value)
}

func validateServerUpsertRequest(request serverUpsertRequest) error {
	if strings.TrimSpace(request.Name) == "" {
		return errors.New("服务器名称不能为空")
	}

	switch request.ConnType {
	case models.ConnectionTypeSSH, models.ConnectionTypeLiteAgent, models.ConnectionTypeFullAgent:
	default:
		return errors.New("连接模式非法")
	}

	if request.IsPublic != nil && *request.IsPublic != 0 && *request.IsPublic != 1 {
		return errors.New("公开展示状态非法")
	}

	for _, item := range []struct {
		label string
		value *int
	}{
		{label: "SSH 超时", value: request.SSHTimeout},
		{label: "SSH 探活间隔", value: request.SSHKeepaliveInterval},
		{label: "采集间隔", value: request.MetricCollectInterval},
		{label: "SSH 重连间隔", value: request.SSHReconnectInterval},
	} {
		if item.value != nil && *item.value <= 0 {
			return fmt.Errorf("%s必须大于 0", item.label)
		}
	}

	if request.RealtimeCollectInterval != nil && *request.RealtimeCollectInterval <= 0 {
		return fmt.Errorf("%s must be greater than 0", "realtime collect interval")
	}

	if request.ConnType != models.ConnectionTypeSSH {
		return nil
	}

	if strings.TrimSpace(request.IPAddress) == "" {
		return errors.New("IP / 域名不能为空")
	}

	switch request.AuthType {
	case models.AuthTypePassword, models.AuthTypePrivateKey:
	default:
		return errors.New("认证方式非法")
	}

	if strings.TrimSpace(request.AuthUser) == "" {
		return errors.New("SSH 用户不能为空")
	}

	if strings.TrimSpace(request.AuthCredential) == "" {
		return errors.New("SSH 凭证不能为空")
	}

	return nil
}

func parseBearerToken(headerValue string) string {
	if headerValue == "" {
		return ""
	}

	parts := strings.SplitN(headerValue, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}

	return strings.TrimSpace(parts[1])
}

func parseHistoryQueryRange(fromValue string, toValue string, now time.Time) (time.Time, time.Time, error) {
	fromValue = strings.TrimSpace(fromValue)
	toValue = strings.TrimSpace(toValue)

	if fromValue == "" && toValue == "" {
		return now.Add(-time.Hour), now, nil
	}

	if fromValue == "" || toValue == "" {
		return time.Time{}, time.Time{}, errors.New("from and to must be provided together")
	}

	from, err := time.Parse(time.RFC3339, fromValue)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	to, err := time.Parse(time.RFC3339, toValue)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	if from.After(to) {
		return time.Time{}, time.Time{}, errors.New("from must be before to")
	}

	return from, to, nil
}

type safeWSWriter struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (w *safeWSWriter) WriteMessage(messageType int, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_ = w.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return w.conn.WriteMessage(messageType, payload)
}

func (w *safeWSWriter) WriteJSON(payload any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_ = w.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return w.conn.WriteJSON(payload)
}

func pipeSSHOutput(writer *safeWSWriter, reader io.Reader) {
	buffer := make([]byte, 1024)
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			_ = writer.WriteMessage(websocket.TextMessage, buffer[:n])
		}
		if err != nil {
			return
		}
	}
}

func handleTerminalControlFrame(session *ssh.Session, stdin io.Writer, payload []byte) (bool, error) {
	var control struct {
		Type string `json:"type"`
		Data string `json:"data"`
		Cols int    `json:"cols"`
		Rows int    `json:"rows"`
	}

	if err := json.Unmarshal(payload, &control); err != nil {
		return false, nil
	}

	switch control.Type {
	case "resize":
		if control.Cols <= 0 || control.Rows <= 0 {
			return true, fmt.Errorf("无效的终端尺寸")
		}
		return true, session.WindowChange(control.Rows, control.Cols)
	case "input":
		_, err := io.WriteString(stdin, control.Data)
		return true, err
	default:
		return false, nil
	}
}
