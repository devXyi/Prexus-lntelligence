package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	kernel "github.com/devXyi/Prexus-Intelligence-/backend/internal/kernel"
)

type AnalyzeRequest struct {
	Prompt string `json:"prompt"`
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequest struct {
	Messages []ChatMessage `json:"messages"`
}

type APIResponse struct {
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Content-Type", "application/json")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Prexus API is running")
}

func analyzeHandler(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == "OPTIONS" { w.WriteHeader(200); return }
	if r.Method != "POST" { http.Error(w, "Method not allowed", 405); return }

	var req AnalyzeRequest
	json.NewDecoder(r.Body).Decode(&req)
	if req.Prompt == "" {
		json.NewEncoder(w).Encode(APIResponse{Error: "prompt is required"})
		return
	}

	fullPrompt := fmt.Sprintf(`You are Prexus, a probability predictive intelligence engine.
Analyze the following scenario and provide:
1. A probability score (0-100%%)
2. Key factors influencing the probability
3. A brief recommendation

Scenario: %s`, req.Prompt)

	result, err := kernel.AnalyzeProbability(fullPrompt)
	if err != nil {
		json.NewEncoder(w).Encode(APIResponse{Error: err.Error()})
		return
	}
	json.NewEncoder(w).Encode(APIResponse{Result: result})
}

func chatHandler(w http.ResponseWriter, r *http.Request) {
	enableCORS(w)
	if r.Method == "OPTIONS" { w.WriteHeader(200); return }
	if r.Method != "POST" { http.Error(w, "Method not allowed", 405); return }

	var req ChatRequest
	json.NewDecoder(r.Body).Decode(&req)
	if len(req.Messages) == 0 {
		json.NewEncoder(w).Encode(APIResponse{Error: "messages are required"})
		return
	}

	lastMsg := req.Messages[len(req.Messages)-1].Content
	fullPrompt := fmt.Sprintf("You are Prexus, an intelligent assistant specialized in probability analysis. Be concise and data-driven.\n\nUser: %s", lastMsg)

	result, err := kernel.AnalyzeProbability(fullPrompt)
	if err != nil {
		json.NewEncoder(w).Encode(APIResponse{Error: err.Error()})
		return
	}
	json.NewEncoder(w).Encode(APIResponse{Result: result})
}

func main() {
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/analyze", analyzeHandler)
	http.HandleFunc("/chat", chatHandler)

	fmt.Println("🚀 Prexus API running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
