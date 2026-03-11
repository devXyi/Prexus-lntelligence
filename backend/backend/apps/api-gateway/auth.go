package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

/* ═══════════════════════════════════════════════════
   JWT  (HS256 · stdlib only — no external deps)
═══════════════════════════════════════════════════ */

func jwtSecret() string {
	s := os.Getenv("JWT_SECRET")
	if s == "" {
		s = "prexus-default-secret-change-in-production"
	}
	return s
}

func jwtSign(claims map[string]interface{}) (string, error) {
	header := base64.RawURLEncoding.EncodeToString(
		[]byte(`{"alg":"HS256","typ":"JWT"}`),
	)
	claims["iat"] = time.Now().Unix()
	claims["exp"] = time.Now().Add(72 * time.Hour).Unix()

	pb, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(pb)

	mac := hmac.New(sha256.New, []byte(jwtSecret()))
	mac.Write([]byte(header + "." + payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return header + "." + payload + "." + sig, nil
}

func jwtVerify(token string) (map[string]interface{}, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("malformed token")
	}

	mac := hmac.New(sha256.New, []byte(jwtSecret()))
	mac.Write([]byte(parts[0] + "." + parts[1]))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(expected), []byte(parts[2])) {
		return nil, fmt.Errorf("invalid signature")
	}

	pb, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("bad payload")
	}

	var claims map[string]interface{}
	json.Unmarshal(pb, &claims)

	exp, _ := claims["exp"].(float64)
	if time.Now().Unix() > int64(exp) {
		return nil, fmt.Errorf("token expired")
	}
	return claims, nil
}

// extractToken pulls Bearer token from Authorization header
func extractToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return auth[7:]
	}
	return ""
}

// authMiddleware validates JWT and injects user_id into request header
func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		token := extractToken(r)
		if token == "" {
			w.WriteHeader(401)
			json.NewEncoder(w).Encode(map[string]string{"error": "authorization required"})
			return
		}
		claims, err := jwtVerify(token)
		if err != nil {
			w.WriteHeader(401)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		// Inject user_id so handlers can read it
		r.Header.Set("X-User-Id", fmt.Sprintf("%v", claims["user_id"]))
		next(w, r)
	}
}

/* ═══════════════════════════════════════════════════
   Password hashing (SHA-256 + secret salt)
═══════════════════════════════════════════════════ */

func hashPassword(password string) string {
	h := sha256.New()
	h.Write([]byte(jwtSecret() + ":" + password))
	return fmt.Sprintf("%x", h.Sum(nil))
}

/* ═══════════════════════════════════════════════════
   Supabase REST helper
═══════════════════════════════════════════════════ */

func supabaseURL() string { return os.Getenv("SUPABASE_URL") }
func supabaseKey() string { return os.Getenv("SUPABASE_KEY") }

func supaReq(method, table, query, body string) ([]byte, int, error) {
	url := supabaseURL() + "/rest/v1/" + table
	if query != "" {
		url += "?" + query
	}
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("apikey", supabaseKey())
	req.Header.Set("Authorization", "Bearer "+supabaseKey())
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "return=representation")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b, resp.StatusCode, nil
}

/* ═══════════════════════════════════════════════════
   /register
═══════════════════════════════════════════════════ */

func registerHandler(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(200)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		OrgName  string `json:"org_name"`
		FullName string `json:"full_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body"})
		return
	}
	if req.Email == "" || req.Password == "" {
		json.NewEncoder(w).Encode(map[string]string{"error": "email and password are required"})
		return
	}

	// Check duplicate email
	b, _, err := supaReq("GET", "users", "email=eq."+req.Email+"&select=id", "")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]string{"error": "database error: " + err.Error()})
		return
	}
	var existing []map[string]interface{}
	json.Unmarshal(b, &existing)
	if len(existing) > 0 {
		json.NewEncoder(w).Encode(map[string]string{"error": "email already registered"})
		return
	}

	// Insert user
	userData, _ := json.Marshal(map[string]string{
		"email":         req.Email,
		"password_hash": hashPassword(req.Password),
		"org_name":      req.OrgName,
		"full_name":     req.FullName,
		"role":          "ORG_ADMIN",
	})
	b, status, err := supaReq("POST", "users", "", string(userData))
	if err != nil || status >= 400 {
		json.NewEncoder(w).Encode(map[string]string{"error": "registration failed: " + string(b)})
		return
	}

	var created []map[string]interface{}
	json.Unmarshal(b, &created)
	if len(created) == 0 {
		json.NewEncoder(w).Encode(map[string]string{"error": "user creation failed"})
		return
	}

	userId := fmt.Sprintf("%v", created[0]["id"])
	token, _ := jwtSign(map[string]interface{}{
		"user_id":  userId,
		"email":    req.Email,
		"org_name": req.OrgName,
		"role":     "ORG_ADMIN",
	})
	json.NewEncoder(w).Encode(map[string]interface{}{
		"token": token,
		"user": map[string]string{
			"id":       userId,
			"email":    req.Email,
			"org_name": req.OrgName,
			"full_name": req.FullName,
			"role":     "ORG_ADMIN",
		},
	})
}

/* ═══════════════════════════════════════════════════
   /login
═══════════════════════════════════════════════════ */

func loginHandler(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == "OPTIONS" {
		w.WriteHeader(200)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body"})
		return
	}
	if req.Email == "" || req.Password == "" {
		json.NewEncoder(w).Encode(map[string]string{"error": "email and password are required"})
		return
	}

	hashed := hashPassword(req.Password)
	b, _, err := supaReq("GET", "users",
		"email=eq."+req.Email+"&password_hash=eq."+hashed+
			"&select=id,email,org_name,full_name,role", "")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]string{"error": "database error: " + err.Error()})
		return
	}

	var users []map[string]interface{}
	json.Unmarshal(b, &users)
	if len(users) == 0 {
		w.WriteHeader(401)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid email or password"})
		return
	}

	u := users[0]
	userId := fmt.Sprintf("%v", u["id"])
	orgName := fmt.Sprintf("%v", u["org_name"])
	role := fmt.Sprintf("%v", u["role"])

	token, _ := jwtSign(map[string]interface{}{
		"user_id":  userId,
		"email":    req.Email,
		"org_name": orgName,
		"role":     role,
	})
	json.NewEncoder(w).Encode(map[string]interface{}{
		"token": token,
		"user": map[string]string{
			"id":        userId,
			"email":     req.Email,
			"org_name":  orgName,
			"full_name": fmt.Sprintf("%v", u["full_name"]),
			"role":      role,
		},
	})
}
