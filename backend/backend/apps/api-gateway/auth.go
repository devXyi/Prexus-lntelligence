// backend/apps/api-gateway/auth.go
package main

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

type RegisterRequest struct {
	Email    string `json:"email"    binding:"required,email"`
	Password string `json:"password" binding:"required,min=6"`
	FullName string `json:"full_name"`
	OrgName  string `json:"org_name"`
}

type LoginRequest struct {
	Email    string `json:"email"    binding:"required,email"`
	Password string `json:"password" binding:"required"`
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

func handleRegister(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var existingID int64
	if err := DB.QueryRow("SELECT id FROM users WHERE email = $1", req.Email).Scan(&existingID); err == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "Email already registered"})
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process password"})
		return
	}
	var userID int64
	if err := DB.QueryRow(
		`INSERT INTO users (email,password_hash,full_name,org_name,role,created_at) VALUES ($1,$2,$3,$4,'user',$5) RETURNING id`,
		req.Email, string(hash), req.FullName, req.OrgName, time.Now().UTC(),
	).Scan(&userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create account"})
		return
	}
	token, err := issueToken(userID, req.Email, "user")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Token generation failed"})
		return
	}
	c.JSON(http.StatusCreated, AuthResponse{
		Token: token,
		User:  UserDTO{ID: userID, Email: req.Email, FullName: req.FullName, OrgName: req.OrgName, Role: "user"},
	})
}

func handleLogin(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var id int64
	var passHash, fullName, orgName, role string
	if err := DB.QueryRow(
		`SELECT id,password_hash,COALESCE(full_name,''),COALESCE(org_name,''),role FROM users WHERE email=$1`,
		req.Email,
	).Scan(&id, &passHash, &fullName, &orgName, &role); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(passHash), []byte(req.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}
	token, err := issueToken(id, req.Email, role)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Token generation failed"})
		return
	}
	c.JSON(http.StatusOK, AuthResponse{
		Token: token,
		User:  UserDTO{ID: id, Email: req.Email, FullName: fullName, OrgName: orgName, Role: role},
	})
}

func handleGetMe(c *gin.Context) {
	userID := c.GetInt64("user_id")
	var fullName, orgName string
	DB.QueryRow("SELECT COALESCE(full_name,''),COALESCE(org_name,'') FROM users WHERE id=$1", userID).Scan(&fullName, &orgName)
	c.JSON(http.StatusOK, UserDTO{ID: userID, Email: c.GetString("email"), FullName: fullName, OrgName: orgName, Role: c.GetString("role")})
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
	if _, err := DB.Exec("UPDATE users SET full_name=$1,org_name=$2,updated_at=$3 WHERE id=$4",
		req.FullName, req.OrgName, time.Now().UTC(), userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Update failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

func jwtSecret() []byte {
	s := os.Getenv("JWT_SECRET")
	if s == "" {
		s = "prexus-dev-secret-change-in-production"
	}
	return []byte(s)
}

func issueToken(userID int64, email, role string) (string, error) {
	claims := Claims{
		UserID: userID, Email: email, Role: role,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			Issuer:    "prexus-gateway",
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(jwtSecret())
}

func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		if h == "" || !strings.HasPrefix(h, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Authorization required"})
			return
		}
		claims := &Claims{}
		token, err := jwt.ParseWithClaims(strings.TrimPrefix(h, "Bearer "), claims, func(t *jwt.Token) (interface{}, error) {
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
		c.Set("email",   claims.Email)
		c.Set("role",    claims.Role)
		c.Next()
	}
}
