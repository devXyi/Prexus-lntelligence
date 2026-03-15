// backend/apps/api-gateway/risk.go
// Prexus Intelligence — Risk proxy handlers
// Forwards authenticated risk requests to the Python data engine.
// Handles caching, timeout, and graceful degradation.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	ProxyTimeoutSeconds    = 35
	CacheRiskTTLSeconds    = 300   // 5 min cache for asset risk scores
)

// In-memory risk cache (replace with Redis in production)
var riskCache = struct {
	mu    sync.RWMutex
	store map[string]cachedRisk
}{store: make(map[string]cachedRisk)}

type cachedRisk struct {
	data      []byte
	expiresAt time.Time
}

// ── Proxy factory ────────────────────────────────────────────────────────────

// proxyToDataEngine returns a handler that forwards POST bodies to the engine.
func proxyToDataEngine(path string) gin.HandlerFunc {
	return func(c *gin.Context) {
		engineURL := getDataEngineURL() + path

		// Read request body
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read request"})
			return
		}

		// Cache check for risk/asset (cache by asset_id + scenario + horizon)
		if path == "/risk/asset" {
			if cached, ok := getRiskCache(body); ok {
				c.Data(http.StatusOK, "application/json", cached)
				return
			}
		}

		// Forward to data engine
		client := &http.Client{Timeout: time.Duration(ProxyTimeoutSeconds) * time.Second}

		req, err := http.NewRequest(http.MethodPost, engineURL, bytes.NewReader(body))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Proxy request failed"})
			return
		}
		req.Header.Set("Content-Type", "application/json")

		// Pass engine secret if configured
		if secret := os.Getenv("ENGINE_SECRET"); secret != "" {
			req.Header.Set("Authorization", "Bearer "+secret)
		}

		start := time.Now()
		resp, err := client.Do(req)
		elapsed := time.Since(start)

		if err != nil {
			log.Printf("[proxy] %s engine error after %v: %v", path, elapsed, err)
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":    "Data engine unavailable",
				"detail":   err.Error(),
				"path":     path,
			})
			return
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("[proxy] %s → %d (%v)", path, resp.StatusCode, elapsed)

		// Cache successful risk responses
		if path == "/risk/asset" && resp.StatusCode == 200 {
			setRiskCache(body, respBody)
		}

		c.Data(resp.StatusCode, "application/json", respBody)
	}
}

// proxyToDataEngineGET forwards GET requests to the engine.
func proxyToDataEngineGET(path string) gin.HandlerFunc {
	return func(c *gin.Context) {
		engineURL := getDataEngineURL() + path
		client    := &http.Client{Timeout: 10 * time.Second}

		req, _ := http.NewRequest(http.MethodGet, engineURL, nil)
		if secret := os.Getenv("ENGINE_SECRET"); secret != "" {
			req.Header.Set("Authorization", "Bearer "+secret)
		}

		resp, err := client.Do(req)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":   "Data engine unavailable",
				"online":  false,
				"status":  "offline",
			})
			return
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		c.Data(resp.StatusCode, "application/json", body)
	}
}

// ── Risk cache helpers ────────────────────────────────────────────────────────

type cacheKey struct {
	AssetID  string `json:"asset_id"`
	Scenario string `json:"scenario"`
	Horizon  int    `json:"horizon_days"`
}

func getRiskCache(reqBody []byte) ([]byte, bool) {
	var k cacheKey
	if err := json.Unmarshal(reqBody, &k); err != nil || k.AssetID == "" {
		return nil, false
	}
	key := fmt.Sprintf("%s:%s:%d", k.AssetID, k.Scenario, k.Horizon)

	riskCache.mu.RLock()
	defer riskCache.mu.RUnlock()

	if c, ok := riskCache.store[key]; ok && time.Now().Before(c.expiresAt) {
		return c.data, true
	}
	return nil, false
}

func setRiskCache(reqBody, respBody []byte) {
	var k cacheKey
	if err := json.Unmarshal(reqBody, &k); err != nil || k.AssetID == "" {
		return
	}
	key := fmt.Sprintf("%s:%s:%d", k.AssetID, k.Scenario, k.Horizon)

	riskCache.mu.Lock()
	defer riskCache.mu.Unlock()

	riskCache.store[key] = cachedRisk{
		data:      respBody,
		expiresAt: time.Now().Add(CacheRiskTTLSeconds * time.Second),
	}

	// Evict expired entries (lazy GC)
	if len(riskCache.store) > 1000 {
		now := time.Now()
		for k, v := range riskCache.store {
			if now.After(v.expiresAt) {
				delete(riskCache.store, k)
			}
		}
	}
}
