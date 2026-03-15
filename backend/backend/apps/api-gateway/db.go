// backend/apps/api-gateway/db.go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

var DB *sql.DB

type Asset struct {
	ID        string    `json:"id"`
	UserID    int64     `json:"user_id,omitempty"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Country   string    `json:"country"`
	CC        string    `json:"cc"`
	Lat       float64   `json:"lat"`
	Lon       float64   `json:"lon"`
	ValueMM   float64   `json:"value_mm"`
	PR        float64   `json:"pr"`
	TR        float64   `json:"tr"`
	CR        float64   `json:"cr"`
	Alerts    int       `json:"alerts"`
	UpdatedAt time.Time `json:"updated_at"`
}

type AssetRequest struct {
	Name    string  `json:"name"     binding:"required"`
	Type    string  `json:"type"`
	Country string  `json:"country"`
	CC      string  `json:"cc"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`
	ValueMM float64 `json:"value_mm"`
	PR      float64 `json:"pr"`
	TR      float64 `json:"tr"`
	CR      float64 `json:"cr"`
	Alerts  int     `json:"alerts"`
}

func InitDB() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			getEnv("DB_HOST","localhost"), getEnv("DB_PORT","5432"),
			getEnv("DB_USER","postgres"),  getEnv("DB_PASS","postgres"),
			getEnv("DB_NAME","prexus"),
		)
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil { return fmt.Errorf("sql.Open: %w", err) }
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil { return fmt.Errorf("db.Ping: %w", err) }
	DB = db
	return migrate()
}

func CloseDB() {
	if DB != nil { DB.Close() }
}

func migrate() error {
	_, err := DB.Exec(`
	CREATE TABLE IF NOT EXISTS users (
		id            BIGSERIAL PRIMARY KEY,
		email         TEXT NOT NULL UNIQUE,
		password_hash TEXT NOT NULL,
		full_name     TEXT,
		org_name      TEXT,
		role          TEXT NOT NULL DEFAULT 'user',
		created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at    TIMESTAMPTZ
	);
	CREATE TABLE IF NOT EXISTS assets (
		id         TEXT PRIMARY KEY,
		user_id    BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
		name       TEXT NOT NULL,
		type       TEXT NOT NULL DEFAULT 'Infrastructure',
		country    TEXT, cc TEXT,
		lat        DOUBLE PRECISION NOT NULL DEFAULT 0,
		lon        DOUBLE PRECISION NOT NULL DEFAULT 0,
		value_mm   DOUBLE PRECISION NOT NULL DEFAULT 0,
		pr         DOUBLE PRECISION NOT NULL DEFAULT 0.5,
		tr         DOUBLE PRECISION NOT NULL DEFAULT 0.5,
		cr         DOUBLE PRECISION NOT NULL DEFAULT 0.5,
		alerts     INTEGER NOT NULL DEFAULT 0,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_assets_user_id ON assets(user_id);
	`)
	if err != nil { return fmt.Errorf("migration: %w", err) }
	log.Printf("✓ Database schema ready")
	return nil
}

func handleGetAssets(c *gin.Context) {
	userID := c.GetInt64("user_id")
	rows, err := DB.Query(`SELECT id,name,type,COALESCE(country,''),COALESCE(cc,''),lat,lon,value_mm,pr,tr,cr,alerts,updated_at FROM assets WHERE user_id=$1 ORDER BY cr DESC`, userID)
	if err != nil { c.JSON(500, gin.H{"error": "Database error"}); return }
	defer rows.Close()
	assets := []Asset{}
	for rows.Next() {
		var a Asset; a.UserID = userID
		if err := rows.Scan(&a.ID,&a.Name,&a.Type,&a.Country,&a.CC,&a.Lat,&a.Lon,&a.ValueMM,&a.PR,&a.TR,&a.CR,&a.Alerts,&a.UpdatedAt); err != nil { continue }
		assets = append(assets, a)
	}
	c.JSON(200, assets)
}

func handleCreateAsset(c *gin.Context) {
	userID := c.GetInt64("user_id")
	var req AssetRequest
	if err := c.ShouldBindJSON(&req); err != nil { c.JSON(400, gin.H{"error": err.Error()}); return }
	assetID := generateAssetID(req.CC, req.Type, userID)
	var a Asset
	err := DB.QueryRow(`INSERT INTO assets (id,user_id,name,type,country,cc,lat,lon,value_mm,pr,tr,cr,alerts,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING id,name,type,COALESCE(country,''),COALESCE(cc,''),lat,lon,value_mm,pr,tr,cr,alerts,updated_at`,
		assetID,userID,req.Name,req.Type,req.Country,req.CC,req.Lat,req.Lon,req.ValueMM,clamp01(req.PR),clamp01(req.TR),clamp01(req.CR),req.Alerts,time.Now().UTC(),
	).Scan(&a.ID,&a.Name,&a.Type,&a.Country,&a.CC,&a.Lat,&a.Lon,&a.ValueMM,&a.PR,&a.TR,&a.CR,&a.Alerts,&a.UpdatedAt)
	if err != nil { c.JSON(500, gin.H{"error": "Failed to create asset: "+err.Error()}); return }
	c.JSON(201, a)
}

func handleUpdateAsset(c *gin.Context) {
	userID  := c.GetInt64("user_id")
	assetID := c.Param("id")
	var req AssetRequest
	if err := c.ShouldBindJSON(&req); err != nil { c.JSON(400, gin.H{"error": err.Error()}); return }
	var a Asset
	err := DB.QueryRow(`UPDATE assets SET name=$1,type=$2,country=$3,cc=$4,lat=$5,lon=$6,value_mm=$7,pr=$8,tr=$9,cr=$10,alerts=$11,updated_at=$12 WHERE id=$13 AND user_id=$14 RETURNING id,name,type,COALESCE(country,''),COALESCE(cc,''),lat,lon,value_mm,pr,tr,cr,alerts,updated_at`,
		req.Name,req.Type,req.Country,req.CC,req.Lat,req.Lon,req.ValueMM,clamp01(req.PR),clamp01(req.TR),clamp01(req.CR),req.Alerts,time.Now().UTC(),assetID,userID,
	).Scan(&a.ID,&a.Name,&a.Type,&a.Country,&a.CC,&a.Lat,&a.Lon,&a.ValueMM,&a.PR,&a.TR,&a.CR,&a.Alerts,&a.UpdatedAt)
	if err == sql.ErrNoRows { c.JSON(404, gin.H{"error": "Asset not found"}); return }
	if err != nil { c.JSON(500, gin.H{"error": "Update failed"}); return }
	c.JSON(200, a)
}

func handleDeleteAsset(c *gin.Context) {
	userID  := c.GetInt64("user_id")
	assetID := c.Param("id")
	result, err := DB.Exec("DELETE FROM assets WHERE id=$1 AND user_id=$2", assetID, userID)
	if err != nil { c.JSON(500, gin.H{"error": "Delete failed"}); return }
	rows, _ := result.RowsAffected()
	if rows == 0 { c.JSON(404, gin.H{"error": "Asset not found"}); return }
	c.JSON(200, gin.H{"deleted": assetID})
}

func generateAssetID(cc, assetType string, userID int64) string {
	prefix := "AST"
	if len(cc) >= 2 { prefix = cc }
	if len(cc) > 3   { prefix = cc[:3] }
	abbrev := assetType
	if len(abbrev) > 3 { abbrev = abbrev[:3] }
	if abbrev == "" { abbrev = "AST" }
	return fmt.Sprintf("%s-%s-%04d", prefix, abbrev, userID%9999+1)
}

func clamp01(v float64) float64 {
	if v < 0 { return 0 }
	if v > 1 { return 1 }
	return v
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" { return v }
	return fallback
}
