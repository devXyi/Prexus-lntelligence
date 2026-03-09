package kernel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// ── Claude API structs ──────────────────────────────────────────────────────

type claudeMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type claudeRequest struct {
	Model     string          `json:"model"`
	MaxTokens int             `json:"max_tokens"`
	Messages  []claudeMessage `json:"messages"`
}

type claudeResponse struct {
	Content []struct {
		Text string `json:"text"`
	} `json:"content"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// ── API Key Config ──────────────────────────────────────────────────────────
// 🔑 OPTION 1: Hardcode for local testing ONLY (never push to GitHub!)
// Replace @ with your actual key:
const hardcodedKey = "@"

// 🔑 OPTION 2 (RECOMMENDED): Set environment variable instead
// Run this in terminal before starting:
//   export ANTHROPIC_API_KEY=your-key-here
// Then hardcodedKey above won't be used

// ── Core Function ───────────────────────────────────────────────────────────

// AnalyzeProbability sends a prompt to Claude and returns the response
func AnalyzeProbability(prompt string) (string, error) {
	// Try environment variable first (safer), fallback to hardcoded
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		apiKey = hardcodedKey
	}
	if apiKey == "" || apiKey == "@" {
		return "", fmt.Errorf("❌ API key not set — replace @ in claude.go or set ANTHROPIC_API_KEY env variable")
	}

	// Build request
	reqBody := claudeRequest{
		Model:     "claude-sonnet-4-20250514",
		MaxTokens: 1024,
		Messages: []claudeMessage{
			{
				Role:    "user",
				Content: prompt,
			},
		},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequest(
		"POST",
		"https://api.anthropic.com/v1/messages",
		bytes.NewBuffer(jsonBody),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("Content-Type", "application/json")

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	// Parse response
	var result claudeResponse
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return "", fmt.Errorf("failed to parse response: %w", err)
	}

	// Check for API error
	if result.Error != nil {
		return "", fmt.Errorf("Claude API error: %s", result.Error.Message)
	}

	// Return text
	if len(result.Content) > 0 {
		return result.Content[0].Text, nil
	}

	return "", fmt.Errorf("empty response from Claude")
}
