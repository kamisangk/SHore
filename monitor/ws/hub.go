package ws

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Hub 负责维护 dashboard WebSocket 客户端连接。
type Hub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]struct{}
}

// NewHub 创建一个新的 Hub。
func NewHub() *Hub {
	return &Hub{
		clients: make(map[*websocket.Conn]struct{}),
	}
}

// Register 注册一个 dashboard 客户端。
func (h *Hub) Register(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.clients[conn] = struct{}{}
}

// Unregister 移除 dashboard 客户端。
func (h *Hub) Unregister(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.clients, conn)
	if conn != nil {
		_ = conn.Close()
	}
}

// Count 返回当前已注册的 dashboard 连接数。
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.clients)
}

// BroadcastJSON 将 JSON 消息推送给所有已注册客户端。
func (h *Hub) BroadcastJSON(payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	h.mu.RLock()
	conns := make([]*websocket.Conn, 0, len(h.clients))
	for conn := range h.clients {
		conns = append(conns, conn)
	}
	h.mu.RUnlock()

	for _, conn := range conns {
		if conn == nil {
			continue
		}
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			h.Unregister(conn)
		}
	}
}
