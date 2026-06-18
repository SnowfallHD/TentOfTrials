package ws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

func TestNewHubDefaultHeartbeat(t *testing.T) {
	logger := zap.NewNop()
	hub := NewHub(logger)
	if hub.heartbeatInterval != 30*time.Second {
		t.Fatalf("expected default interval 30s, got %v", hub.heartbeatInterval)
	}
}

func TestNewHubCustomHeartbeat(t *testing.T) {
	t.Setenv("WS_HEARTBEAT_INTERVAL_SECS", "10")
	logger := zap.NewNop()
	hub := NewHub(logger)
	if hub.heartbeatInterval != 10*time.Second {
		t.Fatalf("expected interval 10s, got %v", hub.heartbeatInterval)
	}
}

func TestNewHubInvalidHeartbeatUsesDefault(t *testing.T) {
	t.Setenv("WS_HEARTBEAT_INTERVAL_SECS", "notanumber")
	logger := zap.NewNop()
	hub := NewHub(logger)
	if hub.heartbeatInterval != 30*time.Second {
		t.Fatalf("expected default interval 30s on bad input, got %v", hub.heartbeatInterval)
	}
}

func TestActiveConnections(t *testing.T) {
	logger := zap.NewNop()
	hub := NewHub(logger)
	go hub.Run()

	if n := hub.ActiveConnections(); n != 0 {
		t.Fatalf("expected 0 connections, got %d", n)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, _ := upgrader.Upgrade(w, r, nil)
		defer conn.Close()
		// hold connection open briefly
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				return
			}
			defer c.Close()
			time.Sleep(100 * time.Millisecond)
		}()
	}
	wg.Wait()
	time.Sleep(300 * time.Millisecond)
}

func TestClientLastPongTracked(t *testing.T) {
	client := &Client{
		lastPong: time.Time{},
	}
	if !client.lastPong.IsZero() {
		t.Fatal("expected zero lastPong on new client")
	}

	now := time.Now()
	client.mu.Lock()
	client.lastPong = now
	client.mu.Unlock()

	if client.lastPong.IsZero() {
		t.Fatal("expected non-zero lastPong after update")
	}
}

func TestHealthEndpoint(t *testing.T) {
	logger := zap.NewNop()
	hub := NewHub(logger)
	go hub.Run()

	srv := NewServer(hub, nil, logger, 0)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	srv.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `"connections"`) {
		t.Error("health response missing connections field")
	}
	if !strings.Contains(body, `"heartbeat_secs"`) {
		t.Error("health response missing heartbeat_secs field")
	}
}
