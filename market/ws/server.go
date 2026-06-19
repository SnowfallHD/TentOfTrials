package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tent-of-trials/market/matching"
	"github.com/tent-of-trials/market/types"
	"go.uber.org/zap"
)

const (
	defaultHeartbeatInterval = 30 * time.Second
	heartbeatIntervalEnvVar = "WS_HEARTBEAT_INTERVAL_SECS"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan []byte
	subs     map[types.Symbol]struct{}
	remote   string
	lastPong time.Time
	mu       sync.Mutex
}

type Hub struct {
	clients           map[*Client]struct{}
	register          chan *Client
	unregister        chan *Client
	broadcast         chan []byte
	logger            *zap.Logger
	heartbeatInterval time.Duration
	mu                sync.RWMutex
}

type ConnectionHealth struct {
	Remote       string    `json:"remote"`
	LastPong     time.Time `json:"last_pong"`
	LastPongUnix int64     `json:"last_pong_unix"`
}

type Server struct {
	hub    *Hub
	engine *matching.MatchingEngine
	logger *zap.Logger
	port   int
	srv    *http.Server
}

func heartbeatIntervalFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv(heartbeatIntervalEnvVar))
	if raw == "" {
		return defaultHeartbeatInterval
	}

	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return defaultHeartbeatInterval
	}
	return time.Duration(seconds) * time.Second
}

func NewHub(logger *zap.Logger) *Hub {
	return &Hub{
		clients:           make(map[*Client]struct{}),
		register:          make(chan *Client),
		unregister:        make(chan *Client),
		broadcast:         make(chan []byte, 256),
		logger:            logger,
		heartbeatInterval: heartbeatIntervalFromEnv(),
	}
}

func (h *Hub) heartbeat() time.Duration {
	if h.heartbeatInterval <= 0 {
		return defaultHeartbeatInterval
	}
	return h.heartbeatInterval
}

func (h *Hub) ActiveConnectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) ConnectionHealth() []ConnectionHealth {
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()

	health := make([]ConnectionHealth, 0, len(clients))
	for _, client := range clients {
		lastPong := client.LastPong()
		lastPongUnix := int64(0)
		if !lastPong.IsZero() {
			lastPongUnix = lastPong.Unix()
		}
		health = append(health, ConnectionHealth{
			Remote:       client.remote,
			LastPong:     lastPong,
			LastPongUnix: lastPongUnix,
		})
	}

	sort.Slice(health, func(i, j int) bool {
		return health[i].Remote < health[j].Remote
	})
	return health
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = struct{}{}
			total := len(h.clients)
			h.mu.Unlock()
			h.logger.Info("client connected",
				zap.String("remote", client.remote),
				zap.Int("total", total),
			)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			total := len(h.clients)
			h.mu.Unlock()
			h.logger.Info("client disconnected",
				zap.String("remote", client.remote),
				zap.Int("total", total),
			)

		case message := <-h.broadcast:
			var stalled []*Client
			h.mu.Lock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
					stalled = append(stalled, client)
				}
			}
			h.mu.Unlock()
			for _, client := range stalled {
				h.logger.Warn("client dropped because send buffer is full", zap.String("remote", client.remote))
			}
		}
	}
}

func NewServer(hub *Hub, engine *matching.MatchingEngine, logger *zap.Logger, port int) *Server {
	return &Server{
		hub:    hub,
		engine: engine,
		logger: logger,
		port:   port,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/v1/trades", s.handleGetTrades)
	mux.HandleFunc("/api/v1/depth", s.handleGetDepth)

	s.srv = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s.srv.ListenAndServe()
}

func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.srv.Shutdown(ctx)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("websocket upgrade failed", zap.Error(err))
		return
	}

	client := &Client{
		hub:      s.hub,
		conn:     conn,
		send:     make(chan []byte, 256),
		subs:     make(map[types.Symbol]struct{}),
		remote:   r.RemoteAddr,
	}

	s.hub.register <- client

	go client.writePump()
	go client.readPump()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"service": "tent-market",
		"time":    time.Now().Unix(),
		"websocket": map[string]interface{}{
			"active_connections": s.hub.ActiveConnectionCount(),
			"connections":        s.hub.ConnectionHealth(),
		},
	})
}

func (s *Server) handleGetTrades(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	trades := s.engine.GetRecentTrades(100)
	json.NewEncoder(w).Encode(trades)
}

func (s *Server) handleGetDepth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": "depth endpoint"})
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	idleTimeout := 2 * c.hub.heartbeat()
	c.conn.SetReadLimit(65536)
	c.conn.SetReadDeadline(time.Now().Add(idleTimeout))
	c.conn.SetPongHandler(func(string) error {
		c.markPong(time.Now())
		return c.conn.SetReadDeadline(time.Now().Add(idleTimeout))
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		var event map[string]interface{}
		if err := json.Unmarshal(message, &event); err != nil {
			continue
		}

		c.mu.Lock()

		c.mu.Unlock()
	}
}

func (c *Client) markPong(t time.Time) {
	c.mu.Lock()
	c.lastPong = t
	c.mu.Unlock()
}

func (c *Client) LastPong() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastPong
}

func (c *Client) writePump() {
	ticker := time.NewTicker(c.hub.heartbeat())
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
