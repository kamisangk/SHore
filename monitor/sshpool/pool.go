package sshpool

import (
	"fmt"
	"net"
	"sync"
	"time"

	"shore-master/monitor/models"
	"shore-master/monitor/security"

	"golang.org/x/crypto/ssh"
)

type cachedClient struct {
	client *ssh.Client
}

// Pool 负责复用 SSH 长连接，并记录失败次数。
type Pool struct {
	mu              sync.Mutex
	clients         map[string]*cachedClient
	failures        map[string]int
	lastDialFailure map[string]time.Time
	encryptor       *security.Encryptor
	dialSSH         func(network string, address string, config *ssh.ClientConfig) (*ssh.Client, error)
	now             func() time.Time
}

// New 创建新的连接池。
func New(encryptor *security.Encryptor) *Pool {
	return &Pool{
		clients:         make(map[string]*cachedClient),
		failures:        make(map[string]int),
		lastDialFailure: make(map[string]time.Time),
		encryptor:       encryptor,
		dialSSH:         ssh.Dial,
		now:             time.Now,
	}
}

// GetClient 获取可用的 SSH Client，若不存在则新建连接。
func (p *Pool) GetClient(server models.Server) (*ssh.Client, error) {
	if client := p.cachedClient(server.ID); client != nil {
		if _, _, err := client.SendRequest("keepalive@openssh.com", true, nil); err == nil {
			p.markDialSuccess(server.ID)
			return client, nil
		}

		_ = client.Close()
		p.removeCachedClient(server.ID, client)
	}

	if retryAt, blocked := p.reconnectBlockedUntil(server); blocked {
		return nil, fmt.Errorf("ssh reconnect deferred until %s", retryAt.Format(time.RFC3339))
	}

	client, err := p.dial(server)
	if err != nil {
		p.recordDialFailure(server.ID)
		return nil, err
	}

	return p.storeClient(server.ID, client), nil
}

// KeepAlive 向目标服务器发送 keepalive 请求。
func (p *Pool) KeepAlive(server models.Server) error {
	client, err := p.GetClient(server)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, _, err := client.SendRequest("keepalive@openssh.com", true, nil); err != nil {
		p.failures[server.ID]++
		_ = client.Close()
		delete(p.clients, server.ID)
		return err
	}

	p.failures[server.ID] = 0
	return nil
}

// Remove 删除并关闭指定服务器连接。
func (p *Pool) Remove(serverID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cached, ok := p.clients[serverID]; ok && cached.client != nil {
		_ = cached.client.Close()
	}

	delete(p.clients, serverID)
	delete(p.failures, serverID)
	delete(p.lastDialFailure, serverID)
}

// Failures 返回某台服务器当前累计失败次数。
func (p *Pool) Failures(serverID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.failures[serverID]
}

// MarkFailure 手动累加失败次数。
func (p *Pool) MarkFailure(serverID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.failures[serverID]++
	return p.failures[serverID]
}

// ResetFailures 将失败次数归零。
func (p *Pool) ResetFailures(serverID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.failures[serverID] = 0
	delete(p.lastDialFailure, serverID)
}

// Close 关闭池中全部连接。
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for id, cached := range p.clients {
		if cached.client != nil {
			_ = cached.client.Close()
		}
		delete(p.clients, id)
		delete(p.lastDialFailure, id)
	}
}

func (p *Pool) cachedClient(serverID string) *ssh.Client {
	p.mu.Lock()
	defer p.mu.Unlock()

	cached, ok := p.clients[serverID]
	if !ok || cached == nil {
		return nil
	}

	return cached.client
}

func (p *Pool) removeCachedClient(serverID string, client *ssh.Client) {
	p.mu.Lock()
	defer p.mu.Unlock()

	cached, ok := p.clients[serverID]
	if !ok || cached == nil || cached.client != client {
		return
	}

	delete(p.clients, serverID)
}

func (p *Pool) recordDialFailure(serverID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.failures[serverID]++
	p.lastDialFailure[serverID] = p.now()
}

func (p *Pool) markDialSuccess(serverID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.failures[serverID] = 0
	delete(p.lastDialFailure, serverID)
}

func (p *Pool) storeClient(serverID string, client *ssh.Client) *ssh.Client {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cached, ok := p.clients[serverID]; ok && cached != nil && cached.client != nil {
		_ = client.Close()
		p.failures[serverID] = 0
		delete(p.lastDialFailure, serverID)
		return cached.client
	}

	p.failures[serverID] = 0
	delete(p.lastDialFailure, serverID)
	p.clients[serverID] = &cachedClient{client: client}
	return client
}

func (p *Pool) dial(server models.Server) (*ssh.Client, error) {
	credential, err := p.encryptor.Decrypt(server.AuthCredential)
	if err != nil {
		return nil, err
	}

	authMethods := make([]ssh.AuthMethod, 0, 1)
	switch server.AuthType {
	case models.AuthTypePrivateKey:
		signer, err := ssh.ParsePrivateKey([]byte(credential))
		if err != nil {
			return nil, err
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	default:
		authMethods = append(authMethods, ssh.Password(credential))
	}

	clientConfig := &ssh.ClientConfig{
		User:            server.AuthUser,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         serverDialTimeout(server),
	}

	address := net.JoinHostPort(server.IPAddress, fmt.Sprintf("%d", server.SSHPort))
	return p.dialSSH("tcp", address, clientConfig)
}

func (p *Pool) reconnectBlockedUntil(server models.Server) (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if server.SSHReconnectInterval == nil || *server.SSHReconnectInterval <= 0 {
		return time.Time{}, false
	}

	failedAt, ok := p.lastDialFailure[server.ID]
	if !ok || failedAt.IsZero() {
		return time.Time{}, false
	}

	retryAt := failedAt.Add(time.Duration(*server.SSHReconnectInterval) * time.Second)
	if !p.now().Before(retryAt) {
		return time.Time{}, false
	}

	return retryAt, true
}

func serverDialTimeout(server models.Server) time.Duration {
	if server.SSHTimeout == nil || *server.SSHTimeout <= 0 {
		return 0
	}

	return time.Duration(*server.SSHTimeout) * time.Second
}
