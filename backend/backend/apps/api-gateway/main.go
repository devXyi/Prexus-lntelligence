package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "Prexus API is running"})
}

func analyzeHandler(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == "OPTIONS" { w.WriteHeader(200); return }
	w.Header().Set("Content-Type", "application/json")
	var req struct {
		Prompt string `json:"prompt"`
		Model  string `json:"model"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if req.Prompt == "" { json.NewEncoder(w).Encode(map[string]string{"error": "prompt required"}); return }
	if req.Model == "" { req.Model = "gemini" }
	result, err := AnalyzeProbability(req.Prompt, req.Model)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("KERNEL ERROR: %s\nVerify backend is running on https://prexus-intelligence.onrender.com", err.Error())})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"result": result})
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func chatHandler(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == "OPTIONS" { w.WriteHeader(200); return }
	w.Header().Set("Content-Type", "application/json")
	var req struct {
		Messages []ChatMessage `json:"messages"`
		Model    string        `json:"model"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if len(req.Messages) == 0 { json.NewEncoder(w).Encode(map[string]string{"error": "messages required"}); return }
	if req.Model == "" { req.Model = "gemini" }
	var sb strings.Builder
	for _, m := range req.Messages {
		if m.Role == "user" { sb.WriteString("User: " + m.Content + "\n") } else { sb.WriteString("Assistant: " + m.Content + "\n") }
	}
	result, err := AnalyzeProbability(sb.String(), req.Model)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("KERNEL ERROR: %s\nVerify backend is running on https://prexus-intelligence.onrender.com", err.Error())})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"result": result})
}

func router(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	path := r.URL.Path
	switch {
	case path == "/health":
		healthHandler(w, r)
	case path == "/register":
		registerHandler(w, r)
	case path == "/login":
		loginHandler(w, r)
	case path == "/analyze":
		analyzeHandler(w, r)
	case path == "/chat":
		chatHandler(w, r)
	case strings.HasPrefix(path, "/assets"):
		authMiddleware(assetsRouter)(w, r)
	default:
		http.NotFound(w, r)
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" { port = "8080" }
	http.HandleFunc("/", router)
	log.Printf("🔥 Prexus API running on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
