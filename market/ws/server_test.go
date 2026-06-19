package ws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

func newTestWebSocketServer(t *testing.T) (*Hub, string) {
	t.Helper()

	hub := NewHub(zap.NewNop())
	go hub.Run()

	server := NewServer(hub, nil, zap.NewNop(), 0)
	testServer := httptest.NewServer(http.HandlerFunc(server.handleWebSocket))
	t.Cleanup(testServer.Close)

	return hub, "ws" + strings.TrimPrefix(testServer.URL, "http")
}

func dialTestWebSocket(t *testing.T, url string) *websocket.Conn {
	t.Helper()

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})
	return conn
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition was not met within %s", timeout)
}

func TestHeartbeatDisconnectsIdleClients(t *testing.T) {
	t.Setenv(heartbeatIntervalEnvVar, "1")
	hub, url := newTestWebSocketServer(t)
	_ = dialTestWebSocket(t, url)

	waitFor(t, time.Second, func() bool {
		return hub.ActiveConnectionCount() == 1
	})

	waitFor(t, 4*time.Second, func() bool {
		return hub.ActiveConnectionCount() == 0
	})
}

func TestHeartbeatKeepsPongingClientsConnected(t *testing.T) {
	t.Setenv(heartbeatIntervalEnvVar, "1")
	hub, url := newTestWebSocketServer(t)
	conn := dialTestWebSocket(t, url)

	waitFor(t, time.Second, func() bool {
		return hub.ActiveConnectionCount() == 1
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()

	time.Sleep(3 * time.Second)

	if count := hub.ActiveConnectionCount(); count != 1 {
		t.Fatalf("expected ponging client to remain connected, got %d active connections", count)
	}
	health := hub.ConnectionHealth()
	if len(health) != 1 {
		t.Fatalf("expected one connection health entry, got %d", len(health))
	}
	if health[0].LastPong.IsZero() {
		t.Fatal("expected last pong timestamp to be tracked")
	}

	_ = conn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("websocket reader did not stop after closing the connection")
	}
}
