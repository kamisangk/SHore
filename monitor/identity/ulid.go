package identity

import (
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

var (
	ulidEntropyMu sync.Mutex
	ulidEntropy   = ulid.Monotonic(rand.Reader, 0)
)

// NewServerID 生成用于 servers 表的 26 位 ULID。
func NewServerID() string {
	ulidEntropyMu.Lock()
	defer ulidEntropyMu.Unlock()

	return ulid.MustNew(ulid.Timestamp(time.Now().UTC()), ulidEntropy).String()
}

// ParseServerID 统一校验并规范化服务器 ID。
func ParseServerID(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("empty id")
	}

	if _, err := ulid.ParseStrict(trimmed); err != nil {
		return "", fmt.Errorf("invalid server id: %w", err)
	}

	return trimmed, nil
}
