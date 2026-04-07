package geoip

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultTimeout    = 3 * time.Second
	defaultFailureTTL = 10 * time.Minute
)

// Location 表示 IP 对应的地区结果。
type Location struct {
	CountryCode string
	FlagEmoji   string
}

type cacheEntry struct {
	location  Location
	expiresAt time.Time
}

// Service 负责解析 IP 对应的国家代码并做内存缓存。
type Service struct {
	baseURL string
	ttl     time.Duration
	client  *http.Client

	mu    sync.Mutex
	cache map[string]cacheEntry
}

type countryResponse struct {
	IP      string `json:"ip"`
	Country string `json:"country"`
}

// NewService 创建 GeoIP 服务。
func NewService(baseURL string, ttl time.Duration, client *http.Client) *Service {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = "https://api.country.is"
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}

	return &Service{
		baseURL: strings.TrimRight(baseURL, "/"),
		ttl:     ttl,
		client:  client,
		cache:   make(map[string]cacheEntry),
	}
}

// Resolve 根据 IP 或域名解析国家代码与旗帜。
func (s *Service) Resolve(ctx context.Context, address string) (Location, error) {
	targetIP, err := normalizeAddress(ctx, address)
	if err != nil {
		return Location{}, err
	}
	if targetIP == "" {
		return Location{}, nil
	}

	now := time.Now()

	s.mu.Lock()
	if entry, ok := s.cache[targetIP]; ok && now.Before(entry.expiresAt) {
		s.mu.Unlock()
		return entry.location, nil
	}
	s.mu.Unlock()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+"/"+targetIP, nil)
	if err != nil {
		return Location{}, fmt.Errorf("创建 GeoIP 请求失败: %w", err)
	}

	response, err := s.client.Do(request)
	if err != nil {
		s.cacheFailure(targetIP, now)
		return Location{}, fmt.Errorf("GeoIP 查询失败: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		s.cacheFailure(targetIP, now)
		return Location{}, fmt.Errorf("GeoIP 查询返回异常状态: %d", response.StatusCode)
	}

	var payload countryResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		s.cacheFailure(targetIP, now)
		return Location{}, fmt.Errorf("解析 GeoIP 响应失败: %w", err)
	}

	location := Location{
		CountryCode: strings.ToUpper(strings.TrimSpace(payload.Country)),
	}
	location.FlagEmoji = CountryCodeToFlagEmoji(location.CountryCode)

	s.mu.Lock()
	s.cache[targetIP] = cacheEntry{
		location:  location,
		expiresAt: now.Add(s.ttl),
	}
	s.mu.Unlock()

	return location, nil
}

// CountryCodeToFlagEmoji 将 ISO 国家代码转换为国旗 emoji。
func CountryCodeToFlagEmoji(countryCode string) string {
	code := strings.ToUpper(strings.TrimSpace(countryCode))
	if len(code) != 2 {
		return ""
	}

	runes := make([]rune, 0, 2)
	for _, char := range code {
		if char < 'A' || char > 'Z' {
			return ""
		}
		runes = append(runes, rune(0x1F1E6+(char-'A')))
	}

	return string(runes)
}

func normalizeAddress(ctx context.Context, address string) (string, error) {
	target := strings.TrimSpace(address)
	if target == "" {
		return "", nil
	}

	if host, _, err := net.SplitHostPort(target); err == nil && host != "" {
		target = host
	}

	if ip := net.ParseIP(target); ip != nil {
		if isPrivateOrLocal(ip) {
			return "", nil
		}
		return ip.String(), nil
	}

	lookupCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		lookupCtx, cancel = context.WithTimeout(ctx, defaultTimeout)
		defer cancel()
	}

	ips, err := net.DefaultResolver.LookupIP(lookupCtx, "ip", target)
	if err != nil {
		return "", nil
	}

	for _, ip := range ips {
		if isPrivateOrLocal(ip) {
			continue
		}
		return ip.String(), nil
	}

	return "", nil
}

func isPrivateOrLocal(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalMulticast() || ip.IsLinkLocalUnicast() || ip.IsUnspecified()
}

func (s *Service) cacheFailure(targetIP string, now time.Time) {
	s.mu.Lock()
	s.cache[targetIP] = cacheEntry{
		location:  Location{},
		expiresAt: now.Add(defaultFailureTTL),
	}
	s.mu.Unlock()
}
