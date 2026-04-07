package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"shore-master/monitor/config"

	"github.com/nakabonne/tstorage"
)

const (
	historyMetricCPUUsage    = "cpu_usage"
	historyMetricMemoryUsage = "memory_usage"
	historyMetricSwapUsage   = "swap_usage"
	historyMetricDiskUsage   = "disk_usage"
	historyMetricNetworkIn   = "network_in"
	historyMetricNetworkOut  = "network_out"
	historyMetricTCPConn     = "tcp_connections"
	historyMetricUDPConn     = "udp_connections"
	historyMetricProcessCnt  = "process_count"
)

// HistorySnapshot 表示一次需要落盘的历史快照。
type HistorySnapshot struct {
	ServerID       string
	CollectedAt    time.Time
	CPUUsage       float64
	MemoryUsage    float64
	SwapUsage      float64
	DiskUsage      float64
	NetworkIn      float64
	NetworkOut     float64
	TCPConnections float64
	UDPConnections float64
	ProcessCount   float64
}

// HistoryPoint 表示返回给上层的趋势点。
type HistoryPoint struct {
	CollectedAt    time.Time
	CPUUsage       float64
	MemoryUsage    float64
	SwapUsage      float64
	DiskUsage      float64
	NetworkIn      float64
	NetworkOut     float64
	TCPConnections float64
	UDPConnections float64
	ProcessCount   float64
}

// HistoryStore 抽象历史时序存储能力。
type HistoryStore interface {
	WriteSnapshot(ctx context.Context, snapshot HistorySnapshot) error
	QueryHistory(ctx context.Context, serverID string, from time.Time, to time.Time) ([]HistoryPoint, error)
	DeleteServer(ctx context.Context, serverID string) error
	Close() error
}

// TStorageHistoryStore 使用 Tstorage 为每台服务器维护独立的时序库。
type TStorageHistoryStore struct {
	basePath  string
	retention time.Duration

	mu       sync.Mutex
	storages map[string]tstorage.Storage
	deleted  map[string]bool
}

// OpenHistoryStore 根据配置打开历史时序存储。
func OpenHistoryStore(cfg config.Config) (HistoryStore, error) {
	return NewTStorageHistoryStore(cfg.History.TStoragePath, cfg.HistoryRetention())
}

// NewTStorageHistoryStore 创建基于 Tstorage 的历史存储。
func NewTStorageHistoryStore(basePath string, retention time.Duration) (*TStorageHistoryStore, error) {
	if basePath == "" {
		return nil, fmt.Errorf("history storage path is required")
	}

	if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("创建历史存储目录失败: %w", err)
	}

	store := &TStorageHistoryStore{
		basePath:  basePath,
		retention: retention,
		storages:  make(map[string]tstorage.Storage),
		deleted:   make(map[string]bool),
	}
	if err := os.MkdirAll(store.pendingDeleteDir(), os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to prepare pending delete directory: %w", err)
	}
	store.cleanupPendingDeletes()
	return store, nil
}

// WriteSnapshot 写入一条服务器历史快照。
func (s *TStorageHistoryStore) WriteSnapshot(_ context.Context, snapshot HistorySnapshot) error {
	s.mu.Lock()
	delete(s.deleted, snapshot.ServerID)
	s.mu.Unlock()

	storage, err := s.openServerStorage(snapshot.ServerID, true)
	if err != nil {
		return err
	}

	collectedAt := snapshot.CollectedAt.UTC()
	if collectedAt.IsZero() {
		collectedAt = time.Now().UTC()
	}

	timestamp := collectedAt.Unix()
	rows := []tstorage.Row{
		{Metric: historyMetricCPUUsage, DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: snapshot.CPUUsage}},
		{Metric: historyMetricMemoryUsage, DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: snapshot.MemoryUsage}},
		{Metric: historyMetricSwapUsage, DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: snapshot.SwapUsage}},
		{Metric: historyMetricDiskUsage, DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: snapshot.DiskUsage}},
		{Metric: historyMetricNetworkIn, DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: snapshot.NetworkIn}},
		{Metric: historyMetricNetworkOut, DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: snapshot.NetworkOut}},
		{Metric: historyMetricTCPConn, DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: snapshot.TCPConnections}},
		{Metric: historyMetricUDPConn, DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: snapshot.UDPConnections}},
		{Metric: historyMetricProcessCnt, DataPoint: tstorage.DataPoint{Timestamp: timestamp, Value: snapshot.ProcessCount}},
	}

	if err := storage.InsertRows(rows); err != nil {
		return fmt.Errorf("写入时序快照失败: %w", err)
	}

	return nil
}

// QueryHistory 查询指定服务器在时间窗口内的趋势数据。
func (s *TStorageHistoryStore) QueryHistory(_ context.Context, serverID string, from time.Time, to time.Time) ([]HistoryPoint, error) {
	if !to.After(from) {
		return []HistoryPoint{}, nil
	}

	s.mu.Lock()
	deleted := s.deleted[serverID]
	s.mu.Unlock()
	if deleted {
		return []HistoryPoint{}, nil
	}

	storage, err := s.openServerStorage(serverID, false)
	if err != nil {
		return nil, err
	}
	if storage == nil {
		return []HistoryPoint{}, nil
	}

	start := from.UTC().Unix()
	endExclusive := to.UTC().Unix() + 1
	pointMap := make(map[int64]*HistoryPoint)

	loadMetric := func(metric string, assign func(point *HistoryPoint, value float64)) error {
		points, err := storage.Select(metric, nil, start, endExclusive)
		if errors.Is(err, tstorage.ErrNoDataPoints) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("查询指标 %s 失败: %w", metric, err)
		}

		for _, rawPoint := range points {
			point := pointMap[rawPoint.Timestamp]
			if point == nil {
				point = &HistoryPoint{
					CollectedAt: time.Unix(rawPoint.Timestamp, 0).UTC(),
				}
				pointMap[rawPoint.Timestamp] = point
			}

			assign(point, rawPoint.Value)
		}

		return nil
	}

	if err := loadMetric(historyMetricCPUUsage, func(point *HistoryPoint, value float64) {
		point.CPUUsage = value
	}); err != nil {
		return nil, err
	}
	if err := loadMetric(historyMetricMemoryUsage, func(point *HistoryPoint, value float64) {
		point.MemoryUsage = value
	}); err != nil {
		return nil, err
	}
	if err := loadMetric(historyMetricSwapUsage, func(point *HistoryPoint, value float64) {
		point.SwapUsage = value
	}); err != nil {
		return nil, err
	}
	if err := loadMetric(historyMetricDiskUsage, func(point *HistoryPoint, value float64) {
		point.DiskUsage = value
	}); err != nil {
		return nil, err
	}
	if err := loadMetric(historyMetricNetworkIn, func(point *HistoryPoint, value float64) {
		point.NetworkIn = value
	}); err != nil {
		return nil, err
	}
	if err := loadMetric(historyMetricNetworkOut, func(point *HistoryPoint, value float64) {
		point.NetworkOut = value
	}); err != nil {
		return nil, err
	}
	if err := loadMetric(historyMetricTCPConn, func(point *HistoryPoint, value float64) {
		point.TCPConnections = value
	}); err != nil {
		return nil, err
	}
	if err := loadMetric(historyMetricUDPConn, func(point *HistoryPoint, value float64) {
		point.UDPConnections = value
	}); err != nil {
		return nil, err
	}
	if err := loadMetric(historyMetricProcessCnt, func(point *HistoryPoint, value float64) {
		point.ProcessCount = value
	}); err != nil {
		return nil, err
	}

	if len(pointMap) == 0 {
		return []HistoryPoint{}, nil
	}

	timestamps := make([]int64, 0, len(pointMap))
	for timestamp := range pointMap {
		timestamps = append(timestamps, timestamp)
	}
	slices.Sort(timestamps)

	result := make([]HistoryPoint, 0, len(timestamps))
	for _, timestamp := range timestamps {
		result = append(result, *pointMap[timestamp])
	}

	return result, nil
}

// DeleteServer 逻辑删除指定服务器的时序数据。
func (s *TStorageHistoryStore) DeleteServer(_ context.Context, serverID string) error {
	serverPath := s.serverPath(serverID)
	markerPath := s.pendingDeleteMarkerPath(serverID)
	if err := os.WriteFile(markerPath, []byte(serverID), 0o644); err != nil {
		return fmt.Errorf("failed to mark server history delete: %w", err)
	}
	s.mu.Lock()
	s.deleted[serverID] = true
	storage, ok := s.storages[serverID]
	if ok {
		delete(s.storages, serverID)
	}
	s.mu.Unlock()

	if ok {
		if err := storage.Close(); err != nil {
			return fmt.Errorf("关闭服务器时序存储失败: %w", err)
		}
	}

	if err := removePathWithRetry(serverPath); err == nil {
		_ = os.Remove(markerPath)
	}

	return nil
}

// Close 关闭所有已打开的时序存储。
func (s *TStorageHistoryStore) Close() error {
	s.mu.Lock()
	storages := make([]tstorage.Storage, 0, len(s.storages))
	for serverID, storage := range s.storages {
		storages = append(storages, storage)
		delete(s.storages, serverID)
	}
	s.mu.Unlock()

	var firstErr error
	for _, storage := range storages {
		if err := storage.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (s *TStorageHistoryStore) openServerStorage(serverID string, createIfMissing bool) (tstorage.Storage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 时序存储按服务器维度惰性打开并缓存，只有在首次写入或查询命中时才创建或复用底层实例。
	// History storage is opened lazily per server and cached, so the underlying instance is created or reused only when needed.
	if storage, ok := s.storages[serverID]; ok {
		return storage, nil
	}

	serverPath := s.serverPath(serverID)
	if !createIfMissing {
		if _, err := os.Stat(serverPath); errors.Is(err, os.ErrNotExist) {
			return nil, nil
		} else if err != nil {
			return nil, fmt.Errorf("读取服务器时序目录失败: %w", err)
		}
	}

	storage, err := tstorage.NewStorage(
		tstorage.WithDataPath(serverPath),
		tstorage.WithRetention(s.retention),
		tstorage.WithTimestampPrecision(tstorage.Seconds),
	)
	if err != nil {
		return nil, fmt.Errorf("打开服务器时序存储失败: %w", err)
	}

	s.storages[serverID] = storage
	return storage, nil
}

func (s *TStorageHistoryStore) serverPath(serverID string) string {
	return filepath.Join(s.basePath, serverID)
}

func (s *TStorageHistoryStore) pendingDeleteDir() string {
	return filepath.Join(s.basePath, ".pending-delete")
}

func (s *TStorageHistoryStore) pendingDeleteMarkerPath(serverID string) string {
	return filepath.Join(s.pendingDeleteDir(), serverID)
}

func (s *TStorageHistoryStore) cleanupPendingDeletes() {
	entries, err := os.ReadDir(s.pendingDeleteDir())
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		serverID := entry.Name()
		if err := removePathWithRetry(s.serverPath(serverID)); err == nil {
			_ = os.Remove(s.pendingDeleteMarkerPath(serverID))
		}
	}
}

func removePathWithRetry(path string) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if err := os.RemoveAll(path); err == nil || errors.Is(err, os.ErrNotExist) {
			return nil
		} else {
			lastErr = err
		}

		time.Sleep(50 * time.Millisecond)
	}

	return lastErr
}
