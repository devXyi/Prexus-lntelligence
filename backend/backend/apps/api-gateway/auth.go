// backend/apps/api-gateway/auth.go
// Prexus Intelligence — Hardened Auth Layer (v3.0)

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

const authTimeout = 5 * time.Second

// ─────────────────────────────────────────────────────────────
// Models
// ─────────────────────────────────────────────────────────────

type RegisterRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required,min=6"`
	FullName string `json:"full_name"`
	OrgName  string `json:"org_name"`
}

type LoginRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

type GoogleLoginRequest struct {
	Credential string `json:"credential" binding:"required"`
}

type AuthResponse struct {
	Token string  `json:"token"`
	User  UserDTO `json:"user"`
}

type UserDTO struct {
	ID       int64  `json:"id"`
	Email    string `json:"email"`
	FullName string `json:"full_name"`
	OrgName  string `json:"org_name"`
	Role     string `json:"role"`
}

type Claims struct {
	UserID int64  `json:"user_id"`
	Email  string `json:"email"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

type GoogleTokenInfo struct {
	Audience      string `json:"aud"`
	Email         string `json:"email"`
	EmailVerified string `json:"email_verified"`
	Name          string `json:"name"`
	Subject       string `json:"sub"`
}

// ─────────────────────────────────────────────────────────────
// Register
// ─────────────────────────────────────────────────────────────

func handleRegister(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	email := normalizeEmail(req.Email)

	ctx, cancel := context.WithTimeout(c.Request.Context(), authTimeout)
	defer cancel()

	var existingID int64
	err := DB.QueryRowContext(ctx, "SELECT id FROM users WHERE email=$1", email).Scan(&existingID)
	if err == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "Email already registered"})
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		log.Printf("bcrypt error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process password"})
		return
	}

	var userID int64
	err = DB.QueryRowContext(ctx,
		`INSERT INTO users (email,password_hash,full_name,org_name,role,created_at)
		 VALUES ($1,$2,$3,$4,'user',$5) RETURNING id`,
		email, string(hash), req.FullName, req.OrgName, time.Now().UTC(),
	).Scan(&userID)

	if err != nil {
		log.Printf("register DB error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create account"})
		return
	}

	token, err := issueToken(userID, email, "user")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Token generation failed"})
		return
	}

	c.JSON(http.StatusCreated, AuthResponse{
		Token: token,
		User:  UserDTO{ID: userID, Email: email, FullName: req.FullName, OrgName: req.OrgName, Role: "user"},
	})
}

// ─────────────────────────────────────────────────────────────
// Login
// ─────────────────────────────────────────────────────────────

func handleLogin(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	email := normalizeEmail(req.Email)

	ctx, cancel := context.WithTimeout(c.Request.Context(), authTimeout)
	defer cancel()

	var id int64
	var passHash, fullName, orgName, role string

	err := DB.QueryRowContext(ctx,
		`SELECT id,password_hash,COALESCE(full_name,''),COALESCE(org_name,''),role
		 FROM users WHERE email=$1`,
		email,
	).Scan(&id, &passHash, &fullName, &orgName, &role)

	// 🔒 Timing attack protection
	dummyHash := "$2a$10$7EqJtq98hPqEX7fNZaFWoO5uX0ZQ5Y9z3rroWAt4EvsC0BpFkOukC"

	if err != nil {
		_ = bcrypt.CompareHashAndPassword([]byte(dummyHash), []byte(req.Password))
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}

	if bcrypt.CompareHashAndPassword([]byte(passHash), []byte(req.Password)) != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}

	token, err := issueToken(id, email, role)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Token generation failed"})
		return
	}

	c.JSON(http.StatusOK, AuthResponse{
		Token: token,
		User:  UserDTO{ID: id, Email: email, FullName: fullName, OrgName: orgName, Role: role},
	})
}

func handleGoogleConfig(c *gin.Context) {
	clientID := strings.TrimSpace(os.Getenv("GOOGLE_CLIENT_ID"))
	c.JSON(http.StatusOK, gin.H{
		"enabled":   clientID != "",
		"client_id": clientID,
	})
}

func verifyGoogleIDToken(ctx context.Context, idToken string) (*GoogleTokenInfo, error) {
	tokenInfoURL := "https://oauth2.googleapis.com/tokeninfo?id_token=" + url.QueryEscape(idToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenInfoURL, nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, errors.New("invalid Google credential")
	}

	var info GoogleTokenInfo
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		return nil, err
	}

	if info.Email == "" || info.Subject == "" || info.EmailVerified != "true" {
		return nil, errors.New("Google account is not eligible for sign-in")
	}

	if expectedAud := strings.TrimSpace(os.Getenv("GOOGLE_CLIENT_ID")); expectedAud != "" && info.Audience != expectedAud {
		return nil, errors.New("Google credential audience mismatch")
	}

	return &info, nil
}

func handleGoogleLogin(c *gin.Context) {
	var req GoogleLoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), authTimeout)
	defer cancel()

	tokenInfo, err := verifyGoogleIDToken(ctx, req.Credential)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}

	email := normalizeEmail(tokenInfo.Email)

	var id int64
	var fullName, orgName, role string
	err = DB.QueryRowContext(ctx,
		`SELECT id,COALESCE(full_name,''),COALESCE(org_name,''),role
		 FROM users WHERE email=$1`,
		email,
	).Scan(&id, &fullName, &orgName, &role)

	if errors.Is(err, sql.ErrNoRows) {
		role = "user"
		fullName = strings.TrimSpace(tokenInfo.Name)
		seed := "google-oauth-" + tokenInfo.Subject + "-" + time.Now().UTC().Format(time.RFC3339Nano)
		hash, hashErr := bcrypt.GenerateFromPassword([]byte(seed), bcrypt.DefaultCost)
		if hashErr != nil {
			log.Printf("google signup hash error: %v", hashErr)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create account"})
			return
		}

		insertErr := DB.QueryRowContext(ctx,
			`INSERT INTO users (email,password_hash,full_name,org_name,role,created_at)
			 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
			email, string(hash), fullName, "", role, time.Now().UTC(),
		).Scan(&id)
		if insertErr != nil {
			log.Printf("google signup DB error: %v", insertErr)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create account"})
			return
		}
	} else if err != nil {
		log.Printf("google login DB error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Login failed"})
		return
	}

	token, err := issueToken(id, email, role)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Token generation failed"})
		return
	}

	c.JSON(http.StatusOK, AuthResponse{
		Token: token,
		User:  UserDTO{ID: id, Email: email, FullName: fullName, OrgName: orgName, Role: role},
	})
}

// ─────────────────────────────────────────────────────────────
// Me
// ─────────────────────────────────────────────────────────────

func handleGetMe(c *gin.Context) {
	userID := c.GetInt64("user_id")

	ctx, cancel := context.WithTimeout(c.Request.Context(), authTimeout)
	defer cancel()

	var fullName, orgName string
	err := DB.QueryRowContext(ctx,
		"SELECT COALESCE(full_name,''),COALESCE(org_name,'') FROM users WHERE id=$1",
		userID,
	).Scan(&fullName, &orgName)

	if err != nil {
		log.Printf("getMe error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "User fetch failed"})
		return
	}

	c.JSON(http.StatusOK, UserDTO{
		ID:       userID,
		Email:    c.GetString("email"),
		FullName: fullName,
		OrgName:  orgName,
		Role:     c.GetString("role"),
	})
}

func handleUpdateMe(c *gin.Context) {
	userID := c.GetInt64("user_id")

	var req struct {
		FullName string `json:"full_name"`
		OrgName  string `json:"org_name"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), authTimeout)
	defer cancel()

	_, err := DB.ExecContext(ctx,
		"UPDATE users SET full_name=$1,org_name=$2,updated_at=$3 WHERE id=$4",
		req.FullName, req.OrgName, time.Now().UTC(), userID,
	)

	if err != nil {
		log.Printf("updateMe error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Update failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

// ─────────────────────────────────────────────────────────────
// JWT
// ─────────────────────────────────────────────────────────────

func jwtSecret() []byte {
	s := os.Getenv("JWT_SECRET")
	if s == "" {
		log.Fatal("JWT_SECRET not set")
	}
	return []byte(s)
}

func issueToken(userID int64, email, role string) (string, error) {
	claims := Claims{
		UserID: userID,
		Email:  email,
		Role:   role,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			Issuer:    "prexus-gateway",
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(jwtSecret())
}

// ─────────────────────────────────────────────────────────────
// Middleware
// ─────────────────────────────────────────────────────────────

func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		if h == "" || !strings.HasPrefix(h, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Authorization required"})
			return
		}

		tokenStr := strings.TrimPrefix(h, "Bearer ")

		claims := &Claims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return jwtSecret(), nil
		})

		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
			return
		}

		c.Set("user_id", claims.UserID)
		c.Set("email", claims.Email)
		c.Set("role", claims.Role)

		c.Next()
	}
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

func normalizeEmail(e string) string {
	return strings.ToLower(strings.TrimSpace(e))
}
