package monitor

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"shore-master/monitor/config"
	"shore-master/monitor/geoip"
	"shore-master/monitor/models"
	"shore-master/monitor/sshpool"
	"shore-master/monitor/store"
	"shore-master/monitor/ws"

	"gorm.io/gorm"
)

// Snapshot 表示当前缓存中的单节点实时快照。
type Snapshot struct {
	ServerID            string    `json:"serverId"`
	CPUUsage            float64   `json:"cpuUsage"`
	MemoryUsage         float64   `json:"memoryUsage"`
	MemoryUsed          uint64    `json:"memoryUsed"`
	MemoryTotal         uint64    `json:"memoryTotal"`
	SwapUsage           float64   `json:"swapUsage"`
	SwapUsed            uint64    `json:"swapUsed"`
	SwapTotal           uint64    `json:"swapTotal"`
	DiskUsage           float64   `json:"diskUsage"`
	DiskUsed            uint64    `json:"diskUsed"`
	DiskTotal           uint64    `json:"diskTotal"`
	NetworkIn           float64   `json:"networkIn"`
	NetworkOut          float64   `json:"networkOut"`
	TotalNetworkIn      uint64    `json:"totalNetworkIn"`
	TotalNetworkOut     uint64    `json:"totalNetworkOut"`
	CPUModel            string    `json:"cpuModel"`
	CPUSockets          int       `json:"cpuSockets"`
	CPUCores            int       `json:"cpuCores"`
	CPUThreads          int       `json:"cpuThreads"`
	CPUPerformanceCores int       `json:"cpuPerformanceCores"`
	CPUEfficiencyCores  int       `json:"cpuEfficiencyCores"`
	GPUModel            string    `json:"gpuModel"`
	TCPConnections      int       `json:"tcpConnections"`
	UDPConnections      int       `json:"udpConnections"`
	ProcessCount        int       `json:"processCount"`
	OS                  string    `json:"os"`
	Hostname            string    `json:"hostname"`
	Kernel              string    `json:"kernel"`
	Architecture        string    `json:"architecture"`
	Virtualization      string    `json:"virtualization"`
	UptimeSec           int64     `json:"uptimeSec"`
	CollectedAt         time.Time `json:"collectedAt"`
}

// PublicMetric 是推送给前端的脱敏结构。
type PublicMetric struct {
	ID                  string    `json:"id"`
	Name                string    `json:"name"`
	GroupName           string    `json:"groupName"`
	Status              int       `json:"status"`
	ConnType            int       `json:"connType"`
	CountryCode         string    `json:"countryCode"`
	FlagEmoji           string    `json:"flagEmoji"`
	CPUUsage            float64   `json:"cpuUsage"`
	MemoryUsage         float64   `json:"memoryUsage"`
	MemoryUsed          uint64    `json:"memoryUsed"`
	MemoryTotal         uint64    `json:"memoryTotal"`
	SwapUsage           float64   `json:"swapUsage"`
	SwapUsed            uint64    `json:"swapUsed"`
	SwapTotal           uint64    `json:"swapTotal"`
	DiskUsage           float64   `json:"diskUsage"`
	DiskUsed            uint64    `json:"diskUsed"`
	DiskTotal           uint64    `json:"diskTotal"`
	NetworkIn           float64   `json:"networkIn"`
	NetworkOut          float64   `json:"networkOut"`
	TotalNetworkIn      uint64    `json:"totalNetworkIn"`
	TotalNetworkOut     uint64    `json:"totalNetworkOut"`
	CPUModel            string    `json:"cpuModel"`
	CPUSockets          int       `json:"cpuSockets"`
	CPUCores            int       `json:"cpuCores"`
	CPUThreads          int       `json:"cpuThreads"`
	CPUPerformanceCores int       `json:"cpuPerformanceCores"`
	CPUEfficiencyCores  int       `json:"cpuEfficiencyCores"`
	GPUModel            string    `json:"gpuModel"`
	TCPConnections      int       `json:"tcpConnections"`
	UDPConnections      int       `json:"udpConnections"`
	ProcessCount        int       `json:"processCount"`
	OS                  string    `json:"os"`
	Hostname            string    `json:"hostname"`
	Kernel              string    `json:"kernel"`
	Architecture        string    `json:"architecture"`
	Virtualization      string    `json:"virtualization"`
	UptimeSec           int64     `json:"uptimeSec"`
	CollectedAt         time.Time `json:"collectedAt"`
}

// HistoryPoint 供卡片折线图使用。
type HistoryPoint struct {
	CollectedAt    time.Time `json:"collectedAt"`
	CPUUsage       float64   `json:"cpuUsage"`
	MemoryUsage    float64   `json:"memoryUsage"`
	SwapUsage      float64   `json:"swapUsage"`
	DiskUsage      float64   `json:"diskUsage"`
	NetworkIn      float64   `json:"networkIn"`
	NetworkOut     float64   `json:"networkOut"`
	TCPConnections float64   `json:"tcpConnections"`
	UDPConnections float64   `json:"udpConnections"`
	ProcessCount   float64   `json:"processCount"`
	Offline        bool      `json:"offline,omitempty"`
}

const realtimeHistoryRetention = 10 * time.Minute

type collectionPlan struct {
	writeHistory   bool
	appendRealtime bool
	markMetric     bool
	markRealtime   bool
}

// RawMetric 是 SSH 采集器直接返回的原始数据。
type RawMetric struct {
	CPUUsage            float64
	MemoryUsed          uint64
	MemoryTotal         uint64
	SwapUsed            uint64
	SwapTotal           uint64
	DiskUsed            uint64
	DiskTotal           uint64
	TotalNetworkIn      uint64
	TotalNetworkOut     uint64
	CPUModel            string
	CPUSockets          int
	CPUCores            int
	CPUThreads          int
	CPUPerformanceCores int
	CPUEfficiencyCores  int
	GPUModel            string
	TCPConnections      int
	UDPConnections      int
	ProcessCount        int
	OS                  string
	Hostname            string
	Kernel              string
	Architecture        string
	Virtualization      string
	UptimeSec           int64
}

// Provider 约束不同监控源的统一行为。
type Provider interface {
	CollectMetrics(ctx context.Context, server models.Server) (RawMetric, error)
}

// LocationResolver 负责将服务器地址解析为地区信息。
type LocationResolver interface {
	Resolve(ctx context.Context, address string) (geoip.Location, error)
}

// Service 负责聚合采集、缓存、历史持久化与广播。
type Service struct {
	db           *gorm.DB
	historyStore store.HistoryStore
	locator      LocationResolver
	pool         *sshpool.Pool
	hub          *ws.Hub
	provider     Provider
	cfg          config.Config

	mu                  sync.RWMutex
	current             map[string]Snapshot
	realtimeHistory     map[string][]HistoryPoint
	lastMetricAttempt   map[string]time.Time
	lastRealtimeAttempt map[string]time.Time
	lastKeepAlive       map[string]time.Time
}

// NewService 创建监控服务。
func NewService(db *gorm.DB, historyStore store.HistoryStore, locator LocationResolver, pool *sshpool.Pool, hub *ws.Hub, cfg config.Config) *Service {
	return &Service{
		db:                  db,
		historyStore:        historyStore,
		locator:             locator,
		pool:                pool,
		hub:                 hub,
		provider:            NewSSHMonitorProvider(pool),
		cfg:                 cfg,
		current:             make(map[string]Snapshot),
		realtimeHistory:     make(map[string][]HistoryPoint),
		lastMetricAttempt:   make(map[string]time.Time),
		lastRealtimeAttempt: make(map[string]time.Time),
		lastKeepAlive:       make(map[string]time.Time),
	}
}

// Start 启动采集、广播与探活任务。
func (s *Service) Start(ctx context.Context) {
	go s.runCollector(ctx)
	go s.runKeepAlive(ctx)
}

// TriggerCollectByID 在新增或更新服务器后触发一次即时采集。
func (s *Service) TriggerCollectByID(ctx context.Context, serverID string) {
	go func() {
		if err := s.CollectServerByID(ctx, serverID); err == nil && s.hasRealtimeSubscribers() {
			_ = s.BroadcastDashboard(ctx)
		}
	}()
}

// CollectServerByID 根据服务器 ID 进行单次采集。
func (s *Service) CollectServerByID(ctx context.Context, serverID string) error {
	var server models.Server
	if err := s.db.Where("id = ?", serverID).First(&server).Error; err != nil {
		return err
	}

	return s.collectServer(ctx, server)
}

// PublicMetrics 获取适合前端展示的当前缓存快照。
func (s *Service) PublicMetrics(ctx context.Context) ([]PublicMetric, error) {
	var servers []models.Server
	if err := s.db.Order("display_index asc, id asc").Find(&servers).Error; err != nil {
		return nil, err
	}

	s.mu.RLock()
	snapshots := make(map[string]Snapshot, len(s.current))
	for serverID, snapshot := range s.current {
		snapshots[serverID] = snapshot
	}
	s.mu.RUnlock()

	result := make([]PublicMetric, 0, len(servers))
	for _, server := range servers {
		snapshot, ok := snapshots[server.ID]
		if !ok {
			snapshot = Snapshot{
				ServerID:    server.ID,
				CollectedAt: time.Time{},
			}
		}

		location := geoip.Location{}
		if s.locator != nil {
			resolved, err := s.locator.Resolve(ctx, server.IPAddress)
			if err == nil {
				location = resolved
			}
		}

		result = append(result, BuildPublicMetric(server, snapshot, location))
	}

	return result, nil
}

func (s *Service) HistoryBetween(ctx context.Context, serverID string, from time.Time, to time.Time) ([]HistoryPoint, error) {
	if !s.historyEnabled() {
		return []HistoryPoint{}, nil
	}

	if from.After(to) {
		return nil, fmt.Errorf("invalid history range")
	}

	historyPoints, err := s.historyStore.QueryHistory(ctx, serverID, from, to)
	if err != nil {
		return nil, err
	}

	points := make([]HistoryPoint, 0, len(historyPoints))
	for _, point := range historyPoints {
		points = append(points, HistoryPoint{
			CollectedAt:    point.CollectedAt,
			CPUUsage:       point.CPUUsage,
			MemoryUsage:    point.MemoryUsage,
			SwapUsage:      point.SwapUsage,
			DiskUsage:      point.DiskUsage,
			NetworkIn:      point.NetworkIn,
			NetworkOut:     point.NetworkOut,
			TCPConnections: point.TCPConnections,
			UDPConnections: point.UDPConnections,
			ProcessCount:   point.ProcessCount,
		})
	}

	return points, nil
}

// RealtimeHistoryBetween 读取指定服务器在时间窗口内的实时内存序列，不走 TSDB。
func (s *Service) RealtimeHistoryBetween(_ context.Context, serverID string, from time.Time, to time.Time) ([]HistoryPoint, error) {
	if from.After(to) {
		return nil, fmt.Errorf("invalid realtime range")
	}

	s.mu.RLock()
	points := append([]HistoryPoint(nil), s.realtimeHistory[serverID]...)
	snapshot, hasSnapshot := s.current[serverID]
	s.mu.RUnlock()

	result := make([]HistoryPoint, 0, len(points)+1)
	for _, point := range points {
		if point.CollectedAt.Before(from) || point.CollectedAt.After(to) {
			continue
		}
		result = append(result, point)
	}

	if len(result) == 0 && hasSnapshot && !snapshot.CollectedAt.IsZero() &&
		!snapshot.CollectedAt.Before(from) && !snapshot.CollectedAt.After(to) {
		result = append(result, historyPointFromSnapshot(snapshot, false))
	}

	return result, nil
}

// RemoveServer 清理指定服务器的内存快照与历史数据。
func (s *Service) RemoveServer(ctx context.Context, serverID string) error {
	s.mu.Lock()
	delete(s.current, serverID)
	delete(s.realtimeHistory, serverID)
	delete(s.lastMetricAttempt, serverID)
	delete(s.lastRealtimeAttempt, serverID)
	delete(s.lastKeepAlive, serverID)
	s.mu.Unlock()

	if s.historyStore == nil {
		return nil
	}

	return s.historyStore.DeleteServer(ctx, serverID)
}

// Close 释放监控服务持有的外部资源。
func (s *Service) Close() error {
	if s.historyStore == nil {
		return nil
	}

	return s.historyStore.Close()
}

// BroadcastDashboard 将当前快照广播给所有已连接的 dashboard 客户端。
func (s *Service) BroadcastDashboard(ctx context.Context) error {
	payload, err := s.PublicMetrics(ctx)
	if err != nil {
		return err
	}

	s.hub.BroadcastJSON(map[string]any{
		"type": "metrics_update",
		"data": payload,
	})

	return nil
}

// BuildPublicMetric 将内部快照转换为脱敏的公共响应对象。
func BuildPublicMetric(server models.Server, snapshot Snapshot, location geoip.Location) PublicMetric {
	metric := snapshotMetricFields(snapshot)
	metric.ID = server.ID
	metric.Name = server.Name
	metric.GroupName = strings.TrimSpace(server.GroupName)
	metric.Status = int(server.Status)
	metric.ConnType = server.ConnType
	metric.CountryCode = location.CountryCode
	metric.FlagEmoji = location.FlagEmoji
	return metric
}

func snapshotMetricFields(snapshot Snapshot) PublicMetric {
	return PublicMetric{
		CPUUsage:            snapshot.CPUUsage,
		MemoryUsage:         snapshot.MemoryUsage,
		MemoryUsed:          snapshot.MemoryUsed,
		MemoryTotal:         snapshot.MemoryTotal,
		SwapUsage:           snapshot.SwapUsage,
		SwapUsed:            snapshot.SwapUsed,
		SwapTotal:           snapshot.SwapTotal,
		DiskUsage:           snapshot.DiskUsage,
		DiskUsed:            snapshot.DiskUsed,
		DiskTotal:           snapshot.DiskTotal,
		NetworkIn:           snapshot.NetworkIn,
		NetworkOut:          snapshot.NetworkOut,
		TotalNetworkIn:      snapshot.TotalNetworkIn,
		TotalNetworkOut:     snapshot.TotalNetworkOut,
		CPUModel:            snapshot.CPUModel,
		CPUSockets:          snapshot.CPUSockets,
		CPUCores:            snapshot.CPUCores,
		CPUThreads:          snapshot.CPUThreads,
		CPUPerformanceCores: snapshot.CPUPerformanceCores,
		CPUEfficiencyCores:  snapshot.CPUEfficiencyCores,
		GPUModel:            snapshot.GPUModel,
		TCPConnections:      snapshot.TCPConnections,
		UDPConnections:      snapshot.UDPConnections,
		ProcessCount:        snapshot.ProcessCount,
		OS:                  snapshot.OS,
		Hostname:            snapshot.Hostname,
		Kernel:              snapshot.Kernel,
		Architecture:        snapshot.Architecture,
		Virtualization:      snapshot.Virtualization,
		UptimeSec:           snapshot.UptimeSec,
		CollectedAt:         snapshot.CollectedAt,
	}
}

func snapshotFromRawMetric(serverID string, raw RawMetric, collectedAt time.Time) Snapshot {
	return Snapshot{
		ServerID:            serverID,
		CPUUsage:            raw.CPUUsage,
		MemoryUsed:          raw.MemoryUsed,
		MemoryTotal:         raw.MemoryTotal,
		MemoryUsage:         usagePercent(raw.MemoryUsed, raw.MemoryTotal),
		SwapUsed:            raw.SwapUsed,
		SwapTotal:           raw.SwapTotal,
		SwapUsage:           usagePercent(raw.SwapUsed, raw.SwapTotal),
		DiskUsed:            raw.DiskUsed,
		DiskTotal:           raw.DiskTotal,
		DiskUsage:           usagePercent(raw.DiskUsed, raw.DiskTotal),
		TotalNetworkIn:      raw.TotalNetworkIn,
		TotalNetworkOut:     raw.TotalNetworkOut,
		CPUModel:            raw.CPUModel,
		CPUSockets:          raw.CPUSockets,
		CPUCores:            raw.CPUCores,
		CPUThreads:          raw.CPUThreads,
		CPUPerformanceCores: raw.CPUPerformanceCores,
		CPUEfficiencyCores:  raw.CPUEfficiencyCores,
		GPUModel:            raw.GPUModel,
		TCPConnections:      raw.TCPConnections,
		UDPConnections:      raw.UDPConnections,
		ProcessCount:        raw.ProcessCount,
		OS:                  raw.OS,
		Hostname:            raw.Hostname,
		Kernel:              raw.Kernel,
		Architecture:        raw.Architecture,
		Virtualization:      raw.Virtualization,
		UptimeSec:           raw.UptimeSec,
		CollectedAt:         collectedAt,
	}
}

func (s *Service) runCollector(ctx context.Context) {
	// 采集循环使用固定分辨率驱动，具体是否到期由 CollectOnce 内部按节点与配置判断。
	// The collector ticks at a fixed resolution, while per-server due checks are decided inside CollectOnce.
	ticker := time.NewTicker(s.collectorResolution())
	defer ticker.Stop()

	_ = s.CollectOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.CollectOnce(ctx)
		}
	}
}

func (s *Service) runKeepAlive(ctx context.Context) {
	ticker := time.NewTicker(s.keepAliveResolution())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.keepAliveOnce(time.Now())
		}
	}
}

func (s *Service) keepAliveOnce(now time.Time) error {
	var servers []models.Server
	if err := s.db.Find(&servers).Error; err != nil {
		return err
	}

	runtimeConfig, err := store.LoadMonitoringRuntimeConfig(s.db)
	if err != nil {
		return err
	}

	// 探活先加载一次运行时配置，再按节点筛选是否需要发送 keepalive，避免循环内重复读取配置。
	// Keepalive loads runtime settings once, then filters eligible servers to avoid repeated config reads inside the loop.
	for _, server := range servers {
		if !s.shouldKeepAliveServer(server, runtimeConfig.SSHKeepaliveInterval, now) {
			continue
		}

		s.recordKeepAliveAttempt(server.ID, now)
		runtimeServer := runtimeConfig.ApplyToServer(server)
		if err := s.pool.KeepAlive(runtimeServer); err != nil {
			if s.pool.Failures(server.ID) >= 3 {
				server.Status = models.ServerStatusOffline
				server.LastError = err.Error()
				_ = s.db.Save(&server).Error
			}
			continue
		}

		s.pool.ResetFailures(server.ID)
	}

	return nil
}

// CollectOnce 对所有服务器执行一轮采集。
func (s *Service) CollectOnce(ctx context.Context) error {
	var servers []models.Server
	if err := s.db.Order("display_index asc, id asc").Find(&servers).Error; err != nil {
		return err
	}

	runtimeConfig, err := store.LoadMonitoringRuntimeConfig(s.db)
	if err != nil {
		return err
	}

	now := time.Now()
	realtimeActive := s.hasRealtimeSubscribers()
	broadcastAfterRealtime := false

	for _, server := range servers {
		metricInterval := resolvedMetricCollectInterval(server, runtimeConfig.MetricCollectInterval)
		realtimeInterval := resolvedRealtimeCollectInterval(server, runtimeConfig.RealtimeCollectInterval)

		metricDue := s.shouldCollectMetricServer(server, metricInterval, now)
		realtimeDue := realtimeActive && s.shouldCollectRealtimeServer(server, realtimeInterval, metricInterval, now)
		mergeRealtime := realtimeActive && realtimeInterval > 0 && realtimeInterval == metricInterval && metricDue
		if !metricDue && !realtimeDue {
			continue
		}

		plan := collectionPlan{
			writeHistory:   metricDue,
			appendRealtime: realtimeDue || mergeRealtime,
			markMetric:     metricDue,
			markRealtime:   realtimeDue || mergeRealtime,
		}
		if plan.appendRealtime {
			broadcastAfterRealtime = true
		}

		if err := s.collectServerWithRuntimeConfig(ctx, server, runtimeConfig, plan); err != nil {
			continue
		}
	}

	if realtimeActive && broadcastAfterRealtime {
		return s.BroadcastDashboard(ctx)
	}

	return nil
}

func (s *Service) collectServer(ctx context.Context, server models.Server) error {
	runtimeConfig, err := store.LoadMonitoringRuntimeConfig(s.db)
	if err != nil {
		return err
	}

	appendRealtime := s.hasRealtimeSubscribers()
	return s.collectServerWithRuntimeConfig(ctx, server, runtimeConfig, collectionPlan{
		writeHistory:   true,
		appendRealtime: appendRealtime,
		markMetric:     true,
		markRealtime:   appendRealtime,
	})
}

func (s *Service) collectServerWithRuntimeConfig(ctx context.Context, server models.Server, runtimeConfig store.MonitoringRuntimeConfig, plan collectionPlan) error {
	if server.ConnType != models.ConnectionTypeSSH {
		return errors.New("unsupported connection type")
	}

	// 这条链路统一处理单节点采集：记录尝试、执行采集、更新缓存与状态，并按计划写入实时序列或 TSDB。
	// This path handles a full single-node collection cycle: record attempts, collect metrics, refresh state, and persist realtime or TSDB data as planned.
	attemptedAt := time.Now()
	s.recordCollectAttempt(server.ID, attemptedAt, plan.markMetric, plan.markRealtime)

	raw, err := s.provider.CollectMetrics(ctx, runtimeConfig.ApplyToServer(server))
	if err != nil {
		failures := s.pool.MarkFailure(server.ID)
		if failures >= 3 {
			server.Status = models.ServerStatusOffline
			server.LastError = err.Error()
			_ = s.db.Save(&server).Error
		}

		if plan.appendRealtime {
			s.appendRealtimePoint(server.ID, HistoryPoint{
				CollectedAt: attemptedAt,
				Offline:     true,
			})
		}
		return err
	}

	s.mu.Lock()
	previous, hasPrevious := s.current[server.ID]
	snapshot := snapshotFromRawMetric(server.ID, raw, attemptedAt)
	if hasPrevious {
		snapshot.NetworkIn = perSecond(raw.TotalNetworkIn, previous.TotalNetworkIn, attemptedAt, previous.CollectedAt)
		snapshot.NetworkOut = perSecond(raw.TotalNetworkOut, previous.TotalNetworkOut, attemptedAt, previous.CollectedAt)
	}
	s.current[server.ID] = snapshot
	s.mu.Unlock()

	server.Status = models.ServerStatusOnline
	server.LastError = ""
	server.OSPlatform = normalizeCollectedOSPlatform(raw.OS)
	server.OSVersion = normalizeCollectedOSVersion(raw.OS)
	s.pool.ResetFailures(server.ID)
	if err := s.db.Save(&server).Error; err != nil {
		return err
	}

	if plan.appendRealtime {
		s.appendRealtimePoint(server.ID, historyPointFromSnapshot(snapshot, false))
	}

	if !plan.writeHistory || !s.historyEnabled() {
		return nil
	}

	return s.historyStore.WriteSnapshot(ctx, store.HistorySnapshot{
		ServerID:       server.ID,
		CollectedAt:    snapshot.CollectedAt,
		CPUUsage:       snapshot.CPUUsage,
		MemoryUsage:    snapshot.MemoryUsage,
		SwapUsage:      snapshot.SwapUsage,
		DiskUsage:      snapshot.DiskUsage,
		NetworkIn:      snapshot.NetworkIn,
		NetworkOut:     snapshot.NetworkOut,
		TCPConnections: float64(snapshot.TCPConnections),
		UDPConnections: float64(snapshot.UDPConnections),
		ProcessCount:   float64(snapshot.ProcessCount),
	})
}

func (s *Service) historyEnabled() bool {
	if s.historyStore == nil {
		return false
	}

	if s.db == nil {
		return false
	}

	var record models.SystemConfig
	if err := s.db.Where("config_key = ?", "tsdb_type").First(&record).Error; err != nil {
		return false
	}

	return strings.TrimSpace(record.ConfigValue) == "1"
}

func (s *Service) collectorResolution() time.Duration {
	return s.cfg.CollectorResolution()
}

func (s *Service) keepAliveResolution() time.Duration {
	return s.cfg.KeepAliveResolution()
}

func (s *Service) shouldCollectMetricServer(server models.Server, intervalSeconds int, now time.Time) bool {
	if server.ConnType != models.ConnectionTypeSSH || intervalSeconds <= 0 {
		return false
	}

	lastCollectedAt := s.lastMetricCollectedAt(server.ID)
	if lastCollectedAt.IsZero() {
		return true
	}

	return now.Sub(lastCollectedAt) >= time.Duration(intervalSeconds)*time.Second
}

func (s *Service) shouldCollectRealtimeServer(server models.Server, realtimeInterval int, metricInterval int, now time.Time) bool {
	if server.ConnType != models.ConnectionTypeSSH || realtimeInterval <= 0 {
		return false
	}

	lastCollectedAt := s.lastRealtimeCollectedAt(server.ID, realtimeInterval == metricInterval)
	if lastCollectedAt.IsZero() {
		return true
	}

	return now.Sub(lastCollectedAt) >= time.Duration(realtimeInterval)*time.Second
}

func (s *Service) shouldKeepAliveServer(server models.Server, globalInterval int, now time.Time) bool {
	if server.ConnType != models.ConnectionTypeSSH {
		return false
	}

	intervalSeconds := globalInterval
	if server.SSHKeepaliveInterval != nil {
		intervalSeconds = *server.SSHKeepaliveInterval
	}

	if intervalSeconds <= 0 {
		return false
	}

	lastKeepAliveAt := s.lastKeepAliveAt(server.ID)
	if lastKeepAliveAt.IsZero() {
		return true
	}

	return now.Sub(lastKeepAliveAt) >= time.Duration(intervalSeconds)*time.Second
}

func (s *Service) lastMetricCollectedAt(serverID string) time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if attemptedAt, ok := s.lastMetricAttempt[serverID]; ok && !attemptedAt.IsZero() {
		return attemptedAt
	}

	if snapshot, ok := s.current[serverID]; ok {
		return snapshot.CollectedAt
	}

	return time.Time{}
}

func (s *Service) lastRealtimeCollectedAt(serverID string, mergeWithMetric bool) time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lastCollectedAt := time.Time{}
	if attemptedAt, ok := s.lastRealtimeAttempt[serverID]; ok && !attemptedAt.IsZero() {
		lastCollectedAt = attemptedAt
	}

	if !mergeWithMetric {
		return lastCollectedAt
	}

	if attemptedAt, ok := s.lastMetricAttempt[serverID]; ok && attemptedAt.After(lastCollectedAt) {
		lastCollectedAt = attemptedAt
	}

	if snapshot, ok := s.current[serverID]; ok && snapshot.CollectedAt.After(lastCollectedAt) {
		lastCollectedAt = snapshot.CollectedAt
	}

	return lastCollectedAt
}

func (s *Service) recordCollectAttempt(serverID string, attemptedAt time.Time, markMetric bool, markRealtime bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if markMetric {
		if s.lastMetricAttempt == nil {
			s.lastMetricAttempt = make(map[string]time.Time)
		}
		s.lastMetricAttempt[serverID] = attemptedAt
	}

	if markRealtime {
		if s.lastRealtimeAttempt == nil {
			s.lastRealtimeAttempt = make(map[string]time.Time)
		}
		s.lastRealtimeAttempt[serverID] = attemptedAt
	}
}

func (s *Service) lastKeepAliveAt(serverID string) time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.lastKeepAlive == nil {
		return time.Time{}
	}

	return s.lastKeepAlive[serverID]
}

func (s *Service) recordKeepAliveAttempt(serverID string, attemptedAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastKeepAlive == nil {
		s.lastKeepAlive = make(map[string]time.Time)
	}

	s.lastKeepAlive[serverID] = attemptedAt
}

func (s *Service) hasRealtimeSubscribers() bool {
	return s.hub != nil && s.hub.Count() > 0
}

func (s *Service) appendRealtimePoint(serverID string, point HistoryPoint) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.realtimeHistory == nil {
		s.realtimeHistory = make(map[string][]HistoryPoint)
	}

	points := append(s.realtimeHistory[serverID], point)
	cutoff := time.Now().Add(-realtimeHistoryRetention)
	trimmed := points[:0]
	for _, item := range points {
		if item.CollectedAt.Before(cutoff) {
			continue
		}
		trimmed = append(trimmed, item)
	}

	s.realtimeHistory[serverID] = append([]HistoryPoint(nil), trimmed...)
}

func historyPointFromSnapshot(snapshot Snapshot, offline bool) HistoryPoint {
	return HistoryPoint{
		CollectedAt:    snapshot.CollectedAt,
		CPUUsage:       snapshot.CPUUsage,
		MemoryUsage:    snapshot.MemoryUsage,
		SwapUsage:      snapshot.SwapUsage,
		DiskUsage:      snapshot.DiskUsage,
		NetworkIn:      snapshot.NetworkIn,
		NetworkOut:     snapshot.NetworkOut,
		TCPConnections: float64(snapshot.TCPConnections),
		UDPConnections: float64(snapshot.UDPConnections),
		ProcessCount:   float64(snapshot.ProcessCount),
		Offline:        offline,
	}
}

func resolvedMetricCollectInterval(server models.Server, globalInterval int) int {
	if server.MetricCollectInterval != nil {
		return *server.MetricCollectInterval
	}

	return globalInterval
}

func resolvedRealtimeCollectInterval(server models.Server, globalInterval int) int {
	if server.RealtimeCollectInterval != nil {
		return *server.RealtimeCollectInterval
	}

	return globalInterval
}

func usagePercent(used uint64, total uint64) float64 {
	if total == 0 {
		return 0
	}

	return float64(used) * 100 / float64(total)
}

func perSecond(current uint64, previous uint64, now time.Time, previousAt time.Time) float64 {
	if previousAt.IsZero() || now.Before(previousAt) || current < previous {
		return 0
	}

	seconds := now.Sub(previousAt).Seconds()
	if seconds <= 0 {
		return 0
	}

	return float64(current-previous) / seconds
}

func normalizeCollectedOSVersion(value string) string {
	return strings.TrimSpace(value)
}

func normalizeCollectedOSPlatform(osVersion string) string {
	normalized := strings.ToLower(strings.TrimSpace(osVersion))
	switch {
	case normalized == "":
		return ""
	case strings.Contains(normalized, "windows"):
		return "windows"
	case strings.Contains(normalized, "macos"),
		strings.Contains(normalized, "os x"),
		strings.Contains(normalized, "darwin"):
		return "darwin"
	default:
		// 当前 SSH 采集器仅支持类 Linux 系统，未知发行版按 linux 归类。
		return "linux"
	}
}

// SSHMonitorProvider 基于 SSH 直接采集 Linux 系统指标。
type SSHMonitorProvider struct {
	pool *sshpool.Pool
}

// NewSSHMonitorProvider 创建 SSH 采集器。
func NewSSHMonitorProvider(pool *sshpool.Pool) *SSHMonitorProvider {
	return &SSHMonitorProvider{pool: pool}
}

// CollectMetrics 通过执行 shell 脚本获取系统指标。
func (p *SSHMonitorProvider) CollectMetrics(ctx context.Context, server models.Server) (RawMetric, error) {
	client, err := p.pool.GetClient(server)
	if err != nil {
		return RawMetric{}, err
	}

	session, err := client.NewSession()
	if err != nil {
		return RawMetric{}, err
	}
	defer session.Close()

	output, err := session.CombinedOutput(collectorScript)
	if err != nil {
		return RawMetric{}, fmt.Errorf("collector script failed: %w: %s", err, strings.TrimSpace(string(output)))
	}

	return parseCollectorOutput(output)
}

var collectorScript = `
set -eu
read _ user nice system idle iowait irq softirq steal _ _ < /proc/stat
prev_idle=$((idle + iowait))
prev_busy=$((user + nice + system + irq + softirq + steal))
prev_total=$((prev_idle + prev_busy))
sleep 1
read _ user nice system idle iowait irq softirq steal _ _ < /proc/stat
curr_idle=$((idle + iowait))
curr_busy=$((user + nice + system + irq + softirq + steal))
curr_total=$((curr_idle + curr_busy))
total_delta=$((curr_total - prev_total))
idle_delta=$((curr_idle - prev_idle))
cpu_usage=$(awk -v total="$total_delta" -v idle="$idle_delta" 'BEGIN { if (total <= 0) { printf "0.00" } else { printf "%.2f", (total-idle) * 100 / total } }')
mem_total=$(awk '/MemTotal/ {print $2*1024}' /proc/meminfo)
mem_available=$(awk '/MemAvailable/ {print $2*1024}' /proc/meminfo)
swap_total=$(awk '/SwapTotal/ {print $2*1024}' /proc/meminfo)
swap_free=$(awk '/SwapFree/ {print $2*1024}' /proc/meminfo)
disk_total=$(df -kP / | awk 'NR==2 {print $2*1024}')
disk_used=$(df -kP / | awk 'NR==2 {print $3*1024}')
net_line=$(awk -F'[: ]+' '$1 !~ /lo/ && NF > 10 {rx += $3; tx += $11} END {print rx+0 " " tx+0}' /proc/net/dev)
net_in_total=$(printf "%s" "$net_line" | awk '{print $1}')
net_out_total=$(printf "%s" "$net_line" | awk '{print $2}')
cpu_model=$(awk -F': +' '/model name/ {print $2; exit}' /proc/cpuinfo 2>/dev/null || true)
if [ -z "${cpu_model:-}" ] && command -v lscpu >/dev/null 2>&1; then
  cpu_model=$(lscpu 2>/dev/null | awk -F: '/Model name/ {sub(/^[[:space:]]+/, "", $2); print $2; exit}')
fi
cpu_sockets=$(awk -F': +' '/physical id/ {ids[$2]=1} END {count=0; for (id in ids) count++; print count+0}' /proc/cpuinfo 2>/dev/null || true)
if [ -z "${cpu_sockets:-}" ] || [ "${cpu_sockets:-0}" -le 0 ]; then
  cpu_sockets=$(lscpu 2>/dev/null | awk -F: '/Socket\(s\)/ {gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); print $2; exit}' || true)
fi
if [ -z "${cpu_sockets:-}" ] || [ "${cpu_sockets:-0}" -le 0 ]; then
  cpu_sockets=1
fi
cpu_threads=$(nproc 2>/dev/null || true)
if [ -z "${cpu_threads:-}" ] || [ "${cpu_threads:-0}" -le 0 ]; then
  cpu_threads=$(lscpu 2>/dev/null | awk -F: '/^CPU\(s\):/ {gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); print $2; exit}' || true)
fi
if [ -z "${cpu_threads:-}" ] || [ "${cpu_threads:-0}" -le 0 ]; then
  cpu_threads=$(grep -c '^processor' /proc/cpuinfo 2>/dev/null || true)
fi
cpu_cores=$(lscpu 2>/dev/null | awk -F: '
  /Socket\(s\)/ {gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); sockets=$2}
  /Core\(s\) per socket/ {gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); cores=$2}
  END {if (sockets > 0 && cores > 0) print sockets * cores}
' || true)
if [ -z "${cpu_cores:-}" ] || [ "${cpu_cores:-0}" -le 0 ]; then
  cpu_cores=$(awk -F': +' '
    /^physical id/ {pkg=$2}
    /^core id/ && pkg != "" {ids[pkg ":" $2]=1}
    END {count=0; for (id in ids) count++; print count+0}
  ' /proc/cpuinfo 2>/dev/null || true)
fi
if [ -z "${cpu_cores:-}" ] || [ "${cpu_cores:-0}" -le 0 ]; then
  cpu_cores=$(awk -F': +' '/^core id/ {ids[$2]=1} END {count=0; for (id in ids) count++; print count+0}' /proc/cpuinfo 2>/dev/null || true)
fi
if [ -z "${cpu_cores:-}" ] || [ "${cpu_cores:-0}" -le 0 ]; then
  cpu_cores=${cpu_threads:-0}
fi
if [ -z "${cpu_threads:-}" ] || [ "${cpu_threads:-0}" -le 0 ]; then
  cpu_threads=${cpu_cores:-0}
fi
cpu_performance_cores=0
cpu_efficiency_cores=0
if [ "${cpu_cores:-0}" -gt 0 ] && [ "${cpu_threads:-0}" -gt "${cpu_cores:-0}" ] && [ "${cpu_threads:-0}" -lt $((cpu_cores * 2)) ]; then
  cpu_performance_cores=$((cpu_threads - cpu_cores))
  cpu_efficiency_cores=$((cpu_cores - cpu_performance_cores))
fi
gpu_model=""
if command -v nvidia-smi >/dev/null 2>&1; then
  gpu_names=$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | sed '/^[[:space:]]*$/d' || true)
  if [ -n "${gpu_names:-}" ]; then
    gpu_model=$(printf "%s\n" "$gpu_names" | sort | uniq -c | awk 'BEGIN{first=1} {if (!first) printf ", "; $1=$1; count=$1; sub(/^[0-9]+ /, "", $0); printf "%s", $0; if (count > 1) printf " x%d", count; first=0}')
  fi
fi
if [ -z "${gpu_model:-}" ] && command -v lspci >/dev/null 2>&1; then
  gpu_model=$(lspci 2>/dev/null | awk 'BEGIN{first=1} /VGA compatible controller|3D controller|Display controller/ {sub(/^.*: /, "", $0); if (!first) printf ", "; printf "%s", $0; first=0} END {printf ""}')
fi
tcp_connections=$(awk '/^TCP:/{for(i=1;i<=NF;i++) if($i=="inuse") sum+=$(i+1)} /^TCP6:/{for(i=1;i<=NF;i++) if($i=="inuse") sum+=$(i+1)} END {print sum+0}' /proc/net/sockstat /proc/net/sockstat6 2>/dev/null)
udp_connections=$(awk '/^UDP:/{for(i=1;i<=NF;i++) if($i=="inuse") sum+=$(i+1)} /^UDP6:/{for(i=1;i<=NF;i++) if($i=="inuse") sum+=$(i+1)} END {print sum+0}' /proc/net/sockstat /proc/net/sockstat6 2>/dev/null)
process_count=$(ps -e --no-headers 2>/dev/null | wc -l | tr -d ' ' || true)
if [ -z "${process_count:-}" ] || [ "${process_count:-0}" -le 0 ]; then
  process_count=$(find /proc -maxdepth 1 -type d -name '[0-9]*' 2>/dev/null | wc -l | tr -d ' ')
fi
os_name=$(grep '^PRETTY_NAME=' /etc/os-release 2>/dev/null | head -n1 | cut -d= -f2- | tr -d '"')
if [ -z "${os_name:-}" ]; then os_name=$(uname -s); fi
kernel=$(uname -r)
host_name=$(hostname)
arch=$(uname -m)
virt_type=""
if command -v systemd-detect-virt >/dev/null 2>&1; then
  virt_type=$(systemd-detect-virt 2>/dev/null || true)
fi
if [ "${virt_type:-}" = "none" ]; then
  virt_type=""
fi
if [ -z "${virt_type:-}" ] && [ -f /proc/user_beancounters ]; then
  virt_type="openvz"
fi
if [ -z "${virt_type:-}" ] && grep -Eqi '(docker|containerd|kubepods|lxc|podman|libpod)' /proc/1/cgroup 2>/dev/null; then
  virt_type="container"
fi
virt_product_name=$(cat /sys/class/dmi/id/product_name 2>/dev/null | head -n1 || true)
virt_sys_vendor=$(cat /sys/class/dmi/id/sys_vendor 2>/dev/null | head -n1 || true)
virt_cpuinfo_flags=""
if grep -qi 'hypervisor' /proc/cpuinfo 2>/dev/null; then
  virt_cpuinfo_flags="hypervisor"
fi
uptime_sec=$(cut -d. -f1 /proc/uptime)
echo "cpu_usage=$cpu_usage"
echo "mem_total=$mem_total"
echo "mem_available=$mem_available"
echo "swap_total=$swap_total"
echo "swap_free=$swap_free"
echo "disk_total=$disk_total"
echo "disk_used=$disk_used"
echo "net_in_total=$net_in_total"
echo "net_out_total=$net_out_total"
echo "cpu_model=$cpu_model"
echo "cpu_sockets=$cpu_sockets"
echo "cpu_cores=$cpu_cores"
echo "cpu_threads=$cpu_threads"
echo "cpu_performance_cores=$cpu_performance_cores"
echo "cpu_efficiency_cores=$cpu_efficiency_cores"
echo "gpu_model=$gpu_model"
echo "tcp_connections=$tcp_connections"
echo "udp_connections=$udp_connections"
echo "process_count=$process_count"
echo "os=$os_name"
echo "kernel=$kernel"
echo "hostname=$host_name"
echo "arch=$arch"
echo "virtualization=$virt_type"
echo "virt_product_name=$virt_product_name"
echo "virt_sys_vendor=$virt_sys_vendor"
echo "virt_cpuinfo_flags=$virt_cpuinfo_flags"
echo "uptime_sec=$uptime_sec"
`

func parseCollectorOutput(output []byte) (RawMetric, error) {
	// SSH 采集脚本输出先按 key=value 归档，再在这里集中完成类型转换、缺省处理和派生指标计算。
	// Collector output is first normalized as key=value pairs, then converted here into typed fields, defaults, and derived metrics.
	values := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		values[parts[0]] = parts[1]
	}

	if err := scanner.Err(); err != nil {
		return RawMetric{}, err
	}

	memoryTotal, err := parseUint(values["mem_total"])
	if err != nil {
		return RawMetric{}, err
	}

	memoryAvailable, err := parseUint(values["mem_available"])
	if err != nil {
		return RawMetric{}, err
	}

	swapTotal := parseOptionalUint(values["swap_total"])
	swapFree := parseOptionalUint(values["swap_free"])

	diskTotal, err := parseUint(values["disk_total"])
	if err != nil {
		return RawMetric{}, err
	}

	diskUsed, err := parseUint(values["disk_used"])
	if err != nil {
		return RawMetric{}, err
	}

	networkIn, err := parseUint(values["net_in_total"])
	if err != nil {
		return RawMetric{}, err
	}

	networkOut, err := parseUint(values["net_out_total"])
	if err != nil {
		return RawMetric{}, err
	}

	cpuSockets := parseOptionalInt(values["cpu_sockets"])
	cpuCores := parseOptionalInt(values["cpu_cores"])
	cpuThreads := parseOptionalInt(values["cpu_threads"])
	cpuPerformanceCores := parseOptionalInt(values["cpu_performance_cores"])
	cpuEfficiencyCores := parseOptionalInt(values["cpu_efficiency_cores"])
	tcpConnections := parseOptionalInt(values["tcp_connections"])
	udpConnections := parseOptionalInt(values["udp_connections"])
	processCount := parseOptionalInt(values["process_count"])

	uptimeSec, err := parseInt64(values["uptime_sec"])
	if err != nil {
		return RawMetric{}, err
	}

	cpuUsage, err := strconv.ParseFloat(values["cpu_usage"], 64)
	if err != nil {
		return RawMetric{}, err
	}

	memoryUsed := uint64(0)
	if memoryTotal >= memoryAvailable {
		memoryUsed = memoryTotal - memoryAvailable
	}
	swapUsed := uint64(0)
	if swapTotal >= swapFree {
		swapUsed = swapTotal - swapFree
	}

	cpuModel := strings.TrimSpace(values["cpu_model"])
	if cpuSockets <= 0 && cpuModel != "" {
		cpuSockets = 1
	}
	cpuCores, cpuThreads, cpuPerformanceCores, cpuEfficiencyCores = normalizeCPUCoreTopology(
		cpuCores,
		cpuThreads,
		cpuPerformanceCores,
		cpuEfficiencyCores,
	)

	return RawMetric{
		CPUUsage:            cpuUsage,
		MemoryUsed:          memoryUsed,
		MemoryTotal:         memoryTotal,
		SwapUsed:            swapUsed,
		SwapTotal:           swapTotal,
		DiskUsed:            diskUsed,
		DiskTotal:           diskTotal,
		TotalNetworkIn:      networkIn,
		TotalNetworkOut:     networkOut,
		CPUModel:            cpuModel,
		CPUSockets:          cpuSockets,
		CPUCores:            cpuCores,
		CPUThreads:          cpuThreads,
		CPUPerformanceCores: cpuPerformanceCores,
		CPUEfficiencyCores:  cpuEfficiencyCores,
		GPUModel:            strings.TrimSpace(values["gpu_model"]),
		TCPConnections:      tcpConnections,
		UDPConnections:      udpConnections,
		ProcessCount:        processCount,
		OS:                  values["os"],
		Hostname:            values["hostname"],
		Kernel:              values["kernel"],
		Architecture:        strings.TrimSpace(values["arch"]),
		Virtualization:      detectVirtualization(values),
		UptimeSec:           uptimeSec,
	}, nil
}

func detectVirtualization(values map[string]string) string {
	if resolved := normalizeVirtualization(values["virtualization"]); resolved != "" {
		return resolved
	}

	for _, hint := range []string{
		values["virt_product_name"],
		values["virt_sys_vendor"],
	} {
		if resolved := normalizeVirtualizationHint(hint); resolved != "" {
			return resolved
		}
	}

	if strings.Contains(strings.ToLower(values["virt_cpuinfo_flags"]), "hypervisor") {
		return "Virtual Machine"
	}

	return ""
}

func normalizeVirtualization(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))

	switch normalized {
	case "", "none", "physical", "bare-metal", "baremetal":
		return ""
	case "kvm", "qemu":
		return "KVM"
	case "openvz":
		return "OpenVZ"
	case "vmware":
		return "VMware"
	case "microsoft", "hyperv", "hyper-v":
		return "Hyper-V"
	case "virtualbox", "oracle":
		return "VirtualBox"
	case "docker", "container", "containerd", "podman", "lxc", "lxd", "wsl", "systemd-nspawn":
		return "Container"
	default:
		return strings.TrimSpace(value)
	}
}

func normalizeVirtualizationHint(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch {
	case normalized == "":
		return ""
	case strings.Contains(normalized, "openvz"):
		return "OpenVZ"
	case strings.Contains(normalized, "vmware"):
		return "VMware"
	case strings.Contains(normalized, "hyper-v"), strings.Contains(normalized, "hyperv"), strings.Contains(normalized, "virtual machine"):
		return "Hyper-V"
	case strings.Contains(normalized, "virtualbox"), strings.Contains(normalized, "oracle"):
		return "VirtualBox"
	case strings.Contains(normalized, "lxc"), strings.Contains(normalized, "lxd"), strings.Contains(normalized, "docker"), strings.Contains(normalized, "container"):
		return "Container"
	case strings.Contains(normalized, "kvm"), strings.Contains(normalized, "qemu"), strings.Contains(normalized, "bochs"), strings.Contains(normalized, "red hat"):
		return "KVM"
	default:
		return ""
	}
}

func parseUint(value string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(value), 10, 64)
}

func parseInt64(value string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(value), 10, 64)
}

func parseOptionalUint(value string) uint64 {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}

	parsed, err := strconv.ParseUint(trimmed, 10, 64)
	if err != nil {
		return 0
	}

	return parsed
}

func normalizeCPUCoreTopology(cpuCores int, cpuThreads int, cpuPerformanceCores int, cpuEfficiencyCores int) (int, int, int, int) {
	if cpuCores <= 0 && cpuThreads > 0 {
		cpuCores = cpuThreads
	}

	if cpuThreads <= 0 && cpuCores > 0 {
		cpuThreads = cpuCores
	}

	if cpuPerformanceCores > 0 && cpuEfficiencyCores > 0 && cpuPerformanceCores+cpuEfficiencyCores == cpuCores {
		return cpuCores, cpuThreads, cpuPerformanceCores, cpuEfficiencyCores
	}

	// 仅在线程数介于 C 和 2C 之间时，按常见大小核模型推导 P/E，避免把普通 SMT 机器误判为混合架构。
	if cpuCores > 0 && cpuThreads > cpuCores && cpuThreads < cpuCores*2 {
		cpuPerformanceCores = cpuThreads - cpuCores
		cpuEfficiencyCores = cpuCores - cpuPerformanceCores
		if cpuPerformanceCores > 0 && cpuEfficiencyCores > 0 {
			return cpuCores, cpuThreads, cpuPerformanceCores, cpuEfficiencyCores
		}
	}

	return cpuCores, cpuThreads, 0, 0
}

func parseOptionalInt(value string) int {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}

	parsed, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0
	}

	return parsed
}
